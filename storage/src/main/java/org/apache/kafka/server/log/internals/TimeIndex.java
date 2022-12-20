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
import org.apache.kafka.common.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * An index that maps from the timestamp to the logical offsets of the messages in a segment. This index might be
 * sparse, i.e. it may not hold an entry for all the messages in the segment.
 *
 * The index is stored in a file that is preallocated to hold a fixed maximum amount of 12-byte time index entries.
 * The file format is a series of time index entries. The physical format is a 8 bytes timestamp and a 4 bytes "relative"
 * offset used in the [[OffsetIndex]]. A time index entry (TIMESTAMP, OFFSET) means that the biggest timestamp seen
 * before OFFSET is TIMESTAMP. i.e. Any message whose timestamp is greater than TIMESTAMP must come after OFFSET.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 *
 * The timestamps in the same time index file are guaranteed to be monotonically increasing.
 *
 * The index supports timestamp lookup for a memory map of this file. The lookup is done using a binary search to find
 * the offset of the message whose indexed timestamp is closest but smaller or equals to the target timestamp.
 *
 * Time index files can be opened in two ways: either as an empty, mutable index that allows appending or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 */
public class TimeIndex extends AbstractIndex {
    private static final Logger log = LoggerFactory.getLogger(TimeIndex.class);
    private static final int ENTRY_SIZE = 12;

    private volatile TimestampOffset lastEntry;

    public TimeIndex(File file, long baseOffset, int maxIndexSize) throws IOException {
        this(file, baseOffset, maxIndexSize, true);
    }

    public TimeIndex(File file, long baseOffset, int maxIndexSize, boolean writable) throws IOException {
        super(file, baseOffset, maxIndexSize, writable);

        this.lastEntry = lastEntryFromIndexFile();

        log.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}",
            file.getAbsolutePath(), maxEntries(), maxIndexSize, entries(), lastEntry, mmap().position());
    }

    @Override
    public void sanityCheck() {
        TimestampOffset entry = lastEntry();
        long lastTimestamp = entry.timestamp;
        long lastOffset = entry.offset;
        if (entries() != 0 && lastTimestamp < timestamp(mmap(), 0))
            throw new CorruptIndexException("Corrupt time index found, time index file (" + file().getAbsolutePath() + ") has "
                + "non-zero size but the last timestamp is " + lastTimestamp + " which is less than the first timestamp "
                + timestamp(mmap(), 0));
        if (entries() != 0 && lastOffset < baseOffset())
            throw new CorruptIndexException("Corrupt time index found, time index file (" + file().getAbsolutePath() + ") has "
                + "non-zero size but the last offset is " + lastOffset + " which is less than the first offset " + baseOffset());
        if (length() % ENTRY_SIZE != 0)
            throw new CorruptIndexException("Time index file " + file().getAbsolutePath() + " is corrupt, found " + length()
                + " bytes which is neither positive nor a multiple of " + ENTRY_SIZE);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    @Override
    public void truncateTo(long offset) {
        lock.lock();
        try {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE);

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

    // We override the full check to reserve the last time index entry slot for the on roll call.
    @Override
    public boolean isFull() {
        return entries() >= maxEntries() - 1;
    }

    public TimestampOffset lastEntry() {
        return lastEntry;
    }

    /**
     * Get the nth timestamp mapping from the time index
     * @param n The entry number in the time index
     * @return The timestamp/offset pair at that entry
     */
    public TimestampOffset entry(int n) {
        return maybeLock(lock, () -> {
            if (n >= entries())
                throw new IllegalArgumentException("Attempt to fetch the " + n + "th entry from time index "
                    + file().getAbsolutePath() + " which has size " + entries());
            return parseEntry(mmap(), n);
        });
    }

    /**
     * Find the time index entry whose timestamp is less than or equal to the given timestamp.
     * If the target timestamp is smaller than the least timestamp in the time index, (NoTimestamp, baseOffset) is
     * returned.
     *
     * @param targetTimestamp The timestamp to look up.
     * @return The time index entry found.
     */
    public TimestampOffset lookup(long targetTimestamp) {
        return maybeLock(lock, () -> {
            ByteBuffer idx = mmap().duplicate();
            int slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY);
            if (slot == -1)
                return new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset());
            else
                return parseEntry(idx, slot);
        });
    }

    /**
     * Equivalent to invoking `maybeAppend(timestamp, offset, false)`.
     *
     * @see #maybeAppend(long, long, boolean)
     */
    public void maybeAppend(long timestamp, long offset) {
        maybeAppend(timestamp, offset, false);
    }

    /**
     * Attempt to append a time index entry to the time index.
     * The new entry is appended only if both the timestamp and offset are greater than the last appended timestamp and
     * the last appended offset.
     *
     * @param timestamp The timestamp of the new time index entry
     * @param offset The offset of the new time index entry
     * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment
     *                      gets rolled or the segment is closed.
     */
    public void maybeAppend(long timestamp, long offset, boolean skipFullCheck) {
        lock.lock();
        try {
            if (!skipFullCheck && isFull())
                throw new IllegalArgumentException("Attempt to append to a full time index (size = " + entries() + ").");

            // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
            // to insert the same time index entry as the last entry.
            // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
            // because that could happen in the following two scenarios:
            // 1. A log segment is closed.
            // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
            if (entries() != 0 && offset < lastEntry.offset)
                throw new InvalidOffsetException("Attempt to append an offset (" + offset + ") to slot " + entries()
                    + " no larger than the last offset appended (" + lastEntry.offset + ") to " + file().getAbsolutePath());
            if (entries() != 0 && timestamp < lastEntry.timestamp)
                throw new IllegalStateException("Attempt to append a timestamp (" + timestamp + ") to slot " + entries()
                    + " no larger than the last timestamp appended (" + lastEntry.timestamp + ") to " + file().getAbsolutePath());

            // We only append to the time index when the timestamp is greater than the last inserted timestamp.
            // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
            // index will be empty.
            if (timestamp > lastEntry.timestamp) {
                log.trace("Adding index entry {} => {} to {}.", timestamp, offset, file().getAbsolutePath());
                MappedByteBuffer mmap = mmap();
                mmap.putLong(timestamp);
                mmap.putInt(relativeOffset(offset));
                incrementEntries();
                this.lastEntry = new TimestampOffset(timestamp, offset);
                if (entries() * ENTRY_SIZE != mmap.position())
                    throw new IllegalStateException(entries() + " entries but file position in index is " + mmap.position());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean resize(int newSize) throws IOException {
        lock.lock();
        try {
            if (super.resize(newSize)) {
                this.lastEntry = lastEntryFromIndexFile();
                return true;
            } else
                return false;
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing, we can make this protected once TimeIndexTest is in the same package as this class
    @Override
    public void truncate() {
        truncateToEntries(0);
    }

    @Override
    protected int entrySize() {
        return ENTRY_SIZE;
    }

    @Override
    protected TimestampOffset parseEntry(ByteBuffer buffer, int n) {
        return new TimestampOffset(timestamp(buffer, n), baseOffset() + relativeOffset(buffer, n));
    }

    private long timestamp(ByteBuffer buffer, int n) {
        return buffer.getLong(n * ENTRY_SIZE);
    }

    private int relativeOffset(ByteBuffer buffer, int n) {
        return buffer.getInt(n * ENTRY_SIZE + 8);
    }

    /**
     * Read the last entry from the index file. This operation involves disk access.
     */
    private TimestampOffset lastEntryFromIndexFile() {
        lock.lock();
        try {
            int entries = entries();
            if (entries == 0)
                return new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset());
            else
                return parseEntry(mmap(), entries - 1);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(int entries) {
        lock.lock();
        try {
            super.truncateToEntries0(entries);
            this.lastEntry = lastEntryFromIndexFile();
            log.debug("Truncated index {} to {} entries; position is now {} and last entry is now {}",
                file().getAbsolutePath(), entries, mmap().position(), lastEntry);
        } finally {
            lock.unlock();
        }
    }
}
