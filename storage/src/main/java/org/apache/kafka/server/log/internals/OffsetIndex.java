/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.internals;

import org.apache.kafka.common.errors.InvalidOffsetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 *
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 *
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 *
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 *
 * The frequency of entries is up to the user of this class.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex extends AbstractIndex {
    private static final Logger log = LoggerFactory.getLogger(OffsetIndex.class);
    private static final int ENTRY_SIZE = 8;

    /* the last offset in the index */
    private long lastOffset;

    public OffsetIndex(File file, long baseOffset) throws IOException {
        this(file, baseOffset, -1);
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this(file, baseOffset, maxIndexSize, true);
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize, boolean writable) throws IOException {
        super(file, baseOffset, maxIndexSize, writable);

        lastOffset = lastEntry().offset;

        log.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}",
            file.getAbsolutePath(), maxEntries(), maxIndexSize, entries(), lastOffset, mmap().position());
    }

    @Override
    public void sanityCheck() {
        if (entries() != 0 && lastOffset < baseOffset())
            throw new CorruptIndexException("Corrupt index found, index file " + file().getAbsolutePath() + " has non-zero size " +
                "but the last offset is " + lastOffset + " which is less than the base offset " + baseOffset());
        if (length() % entrySize() != 0)
            throw new CorruptIndexException("Index file " + file().getAbsolutePath() + " is corrupt, found " + length() +
                " bytes which is neither positive nor a multiple of " + ENTRY_SIZE);
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and its corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset
     *         If the target offset is smaller than the least entry in the index (or the index is empty),
     *         the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(long targetOffset) {
        return maybeLock(lock, () -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY);
            if (slot == -1)
                return new OffsetPosition(baseOffset(), 0);
            else
                return parseEntry(idx, slot);
        });
    }

    /**
     * Get the nth offset mapping from the index
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(int n) {
        return maybeLock(lock, () -> {
            if (n >= entries())
                throw new IllegalArgumentException("Attempt to fetch the " + n + "th entry from index " +
                    file().getAbsolutePath() + ", which has size " + entries());
            return parseEntry(mmap(), n);
        });
    }

    /**
     * Find an upper bound offset for the given fetch starting position and size. This is an offset which
     * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
     * such offset.
     */
    public Optional<OffsetPosition> fetchUpperBoundOffset(OffsetPosition fetchOffset, int fetchSize) {
        return maybeLock(lock, () -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE);
            if (slot == -1)
                return Optional.empty();
            else
                return Optional.of(parseEntry(idx, slot));
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     * @throws IndexOffsetOverflowException if the offset causes index offset to overflow
     * @throws InvalidOffsetException if provided offset is not larger than the last offset
     */
    public void append(long offset, int position) {
        lock.lock();
        try {
            if (isFull())
                throw new IllegalArgumentException("Attempt to append to a full index (size = " + entries() + ").");

            if (entries() == 0 || offset > lastOffset) {
                log.trace("Adding index entry {} => {} to {}", offset, position, file().getAbsolutePath());
                mmap().putInt(relativeOffset(offset));
                mmap().putInt(position);
                incrementEntries();
                lastOffset = offset;
                if (entries() * ENTRY_SIZE != mmap().position())
                    throw new IllegalStateException(entries() + " entries but file position in index is " + mmap().position());
            } else
                throw new InvalidOffsetException("Attempt to append an offset " + offset + " to position " + entries() +
                    " no larger than the last offset appended (" + lastOffset + ") to " + file().getAbsolutePath());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void truncateTo(long offset) {
        lock.lock();
        try {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY);

            /* There are 3 cases for choosing the new size
             * 1) if there is no entry in the index <= the offset, delete everything
             * 2) if there is an entry for this exact offset, delete it and everything larger than it
             * 3) if there is no entry for this offset, delete everything larger than the next smallest
             */
            int newEntries;
            if (slot < 0)
                newEntries = 0;
            else if (relativeOffset(idx, slot) == offset - baseOffset())
                newEntries = slot;
            else
                newEntries = slot + 1;
            truncateToEntries(newEntries);
        } finally {
            lock.unlock();
        }
    }

    public long lastOffset() {
        return lastOffset;
    }

    @Override
    public void truncate() {
        truncateToEntries(0);
    }

    @Override
    protected int entrySize() {
        return ENTRY_SIZE;
    }

    @Override
    protected OffsetPosition parseEntry(ByteBuffer buffer, int n) {
        return new OffsetPosition(baseOffset() + relativeOffset(buffer, n), physical(buffer, n));
    }

    private int relativeOffset(ByteBuffer buffer, int n) {
        return buffer.getInt(n * ENTRY_SIZE);
    }

    private int physical(ByteBuffer buffer, int n) {
        return buffer.getInt(n * ENTRY_SIZE + 4);
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(int entries) {
        lock.lock();
        try {
            super.truncateToEntries0(entries);
            this.lastOffset = lastEntry().offset;
            log.debug("Truncated index {} to {} entries; position is now {} and last offset is now {}",
                    file().getAbsolutePath(), entries, mmap().position(), lastOffset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The last entry in the index
     */
    private OffsetPosition lastEntry() {
        lock.lock();
        try {
            int entries = entries();
            if (entries == 0)
                return new OffsetPosition(baseOffset(), 0);
            else
                return parseEntry(mmap(), entries - 1);
        } finally {
            lock.unlock();
        }
    }
}
