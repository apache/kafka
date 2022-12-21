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

import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.OperatingSystem;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The abstract index class which holds entry format agnostic methods.
 */
public abstract class AbstractIndex implements Closeable {

    private enum SearchResultType {
        LARGEST_LOWER_BOUND, SMALLEST_UPPER_BOUND
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractIndex.class);

    protected final ReentrantLock lock = new ReentrantLock();

    private final long baseOffset;
    private final int maxIndexSize;
    private final boolean writable;

    private volatile File file;

    // Length of the index file
    private volatile long length;

    private volatile MappedByteBuffer mmap;

    /**
     * The maximum number of entries this index can hold
     */
    private volatile int maxEntries;
    /** The number of entries in this index */
    private volatile int entries;


    /**
     * @param file The index file
     * @param baseOffset the base offset of the segment that this index is corresponding to.
     * @param maxIndexSize The maximum index size in bytes.
     */
    public AbstractIndex(File file, long baseOffset, int maxIndexSize, boolean writable) throws IOException {
        Objects.requireNonNull(file);
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        this.writable = writable;

        createAndAssignMmap();
        this.maxEntries = mmap.limit() / entrySize();
        this.entries = mmap.position() / entrySize();
    }

    private void createAndAssignMmap() throws IOException {
        boolean newlyCreated = file.createNewFile();
        RandomAccessFile raf;
        if (writable)
            raf = new RandomAccessFile(file, "rw");
        else
            raf = new RandomAccessFile(file, "r");

        try {
            /* pre-allocate the file if necessary */
            if (newlyCreated) {
                if (maxIndexSize < entrySize())
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                raf.setLength(roundDownToExactMultiple(maxIndexSize, entrySize()));
            }

            long length = raf.length();
            MappedByteBuffer mmap = createMappedBuffer(raf, newlyCreated, length, writable, entrySize());

            this.length = length;
            this.mmap = mmap;
        } finally {
            Utils.closeQuietly(raf, "index " + file.getName());
        }
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throws CorruptIndexException if any problems are found
     */
    public abstract void sanityCheck();

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public abstract void truncateTo(long offset);

    /**
     * Remove all the entries from the index.
     */
    protected abstract void truncate();

    protected abstract int entrySize();


    /**
     * To parse an entry in the index.
     *
     * @param buffer the buffer of this memory mapped index.
     * @param n the slot
     * @return the index entry stored in the given slot.
     */
    protected abstract IndexEntry parseEntry(ByteBuffer buffer, int n);

    /**
     * True iff there are no more slots available in this index
     */
    public boolean isFull() {
        return entries >= maxEntries;
    }

    public File file() {
        return this.file;
    }

    public int maxEntries() {
        return this.maxEntries;
    }

    public int entries() {
        return this.entries;
    }

    public long length() {
        return this.length;
    }

    public int maxIndexSize() {
        return maxIndexSize;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     *
     * @param newSize new size of the index file
     * @return a boolean indicating whether the size of the memory map and the underneath file is changed or not.
     */
    public boolean resize(int newSize) throws IOException {
        lock.lock();
        try {
            int roundedNewSize = roundDownToExactMultiple(newSize, entrySize());

            if (length == roundedNewSize) {
                log.debug("Index {} was not resized because it already has size {}", file.getAbsolutePath(), roundedNewSize);
                return false;
            } else {
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                try {
                    int position = mmap.position();

                    /* Windows or z/OS won't let us modify the file length while the file is mmapped :-( */
                    if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
                        safeForceUnmap();
                    raf.setLength(roundedNewSize);
                    this.length = roundedNewSize;
                    mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                    this.maxEntries = mmap.limit() / entrySize();
                    mmap.position(position);
                    log.debug("Resized {} to {}, position is {} and limit is {}", file.getAbsolutePath(), roundedNewSize,
                            mmap.position(), mmap.limit());
                    return true;
                } finally {
                    Utils.closeQuietly(raf, "index file " + file.getName());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Rename the file that backs this offset index
     *
     * @throws IOException if rename fails
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        lock.lock();
        try {
            mmap.force();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Delete this index file.
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return `true` if the file was deleted by this method; `false` if the file could not be deleted because it did
     *         not exist
     */
    public boolean deleteIfExists() throws IOException {
        closeHandler();
        return Files.deleteIfExists(file.toPath());
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() throws IOException {
        lock.lock();
        try {
            resize(entrySize() * entries);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of bytes actually used by this index
     */
    public int sizeInBytes() {
        return entrySize() * entries;
    }

    public void close() throws IOException {
        trimToValidSize();
        closeHandler();
    }

    public void closeHandler() {
        // On JVM, a memory mapping is typically unmapped by garbage collector.
        // However, in some cases it can pause application threads(STW) for a long moment reading metadata from a physical disk.
        // To prevent this, we forcefully cleanup memory mapping within proper execution which never affects API responsiveness.
        // See https://issues.apache.org/jira/browse/KAFKA-4614 for the details.
        lock.lock();
        try {
            safeForceUnmap();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove all the entries from the index and resize the index to the max index size.
     */
    public void reset() throws IOException {
        truncate();
        resize(maxIndexSize);
    }

    /**
     * Get offset relative to base offset of this index
     * @throws IndexOffsetOverflowException
     */
    public int relativeOffset(long offset) {
        OptionalInt relativeOffset = toRelative(offset);
        return relativeOffset.orElseThrow(() -> new IndexOffsetOverflowException(
            "Integer overflow for offset: " + offset + " (" + file.getAbsoluteFile() + ")"));
    }

    /**
     * Check if a particular offset is valid to be appended to this index.
     * @param offset The offset to check
     * @return true if this offset is valid to be appended to this index; false otherwise
     */
    public boolean canAppendOffset(long offset) {
        return toRelative(offset).isPresent();
    }

    protected final MappedByteBuffer mmap() {
        return mmap;
    }

    /*
     * Kafka mmaps index files into memory, and all the read / write operations of the index is through OS page cache. This
     * avoids blocked disk I/O in most cases.
     *
     * To the extent of our knowledge, all the modern operating systems use LRU policy or its variants to manage page
     * cache. Kafka always appends to the end of the index file, and almost all the index lookups (typically from in-sync
     * followers or consumers) are very close to the end of the index. So, the LRU cache replacement policy should work very
     * well with Kafka's index access pattern.
     *
     * However, when looking up index, the standard binary search algorithm is not cache friendly, and can cause unnecessary
     * page faults (the thread is blocked to wait for reading some index entries from hard disk, as those entries are not
     * cached in the page cache).
     *
     * For example, in an index with 13 pages, to lookup an entry in the last page (page #12), the standard binary search
     * algorithm will read index entries in page #0, 6, 9, 11, and 12.
     * page number: |0|1|2|3|4|5|6|7|8|9|10|11|12 |
     * steps:       |1| | | | | |3| | |4|  |5 |2/6|
     * In each page, there are hundreds log entries, corresponding to hundreds to thousands of kafka messages. When the
     * index gradually growing from the 1st entry in page #12 to the last entry in page #12, all the write (append)
     * operations are in page #12, and all the in-sync follower / consumer lookups read page #0,6,9,11,12. As these pages
     * are always used in each in-sync lookup, we can assume these pages are fairly recently used, and are very likely to be
     * in the page cache. When the index grows to page #13, the pages needed in a in-sync lookup change to #0, 7, 10, 12,
     * and 13:
     * page number: |0|1|2|3|4|5|6|7|8|9|10|11|12|13 |
     * steps:       |1| | | | | | |3| | | 4|5 | 6|2/7|
     * Page #7 and page #10 have not been used for a very long time. They are much less likely to be in the page cache, than
     * the other pages. The 1st lookup, after the 1st index entry in page #13 is appended, is likely to have to read page #7
     * and page #10 from disk (page fault), which can take up to more than a second. In our test, this can cause the
     * at-least-once produce latency to jump to about 1 second from a few ms.
     *
     * Here, we use a more cache-friendly lookup algorithm:
     * if (target > indexEntry[end - N]) // if the target is in the last N entries of the index
     *    binarySearch(end - N, end)
     * else
     *    binarySearch(begin, end - N)
     *
     * If possible, we only look up in the last N entries of the index. By choosing a proper constant N, all the in-sync
     * lookups should go to the 1st branch. We call the last N entries the "warm" section. As we frequently look up in this
     * relatively small section, the pages containing this section are more likely to be in the page cache.
     *
     * We set N (_warmEntries) to 8192, because
     * 1. This number is small enough to guarantee all the pages of the "warm" section is touched in every warm-section
     *    lookup. So that, the entire warm section is really "warm".
     *    When doing warm-section lookup, following 3 entries are always touched: indexEntry(end), indexEntry(end-N),
     *    and indexEntry((end*2 -N)/2). If page size >= 4096, all the warm-section pages (3 or fewer) are touched, when we
     *    touch those 3 entries. As of 2018, 4096 is the smallest page size for all the processors (x86-32, x86-64, MIPS,
     *    SPARC, Power, ARM etc.).
     * 2. This number is large enough to guarantee most of the in-sync lookups are in the warm-section. With default Kafka
     *    settings, 8KB index corresponds to about 4MB (offset index) or 2.7MB (time index) log messages.
     *
     *  We can't set make N (_warmEntries) to be larger than 8192, as there is no simple way to guarantee all the "warm"
     *  section pages are really warm (touched in every lookup) on a typical 4KB-page host.
     *
     * In there future, we may use a backend thread to periodically touch the entire warm section. So that, we can
     * 1) support larger warm section
     * 2) make sure the warm section of low QPS topic-partitions are really warm.
     */
    protected final int warmEntries() {
        return 8192 / entrySize();
    }

    protected void safeForceUnmap() {
        if (mmap != null) {
            try {
                forceUnmap();
            } catch (Throwable t) {
                log.error("Error unmapping index {}", file, t);
            }
        }
    }

    /**
     * Forcefully free the buffer's mmap.
     */
    // Visible for testing, we can make this protected once OffsetIndexTest is in the same package as this class
    public void forceUnmap() throws IOException {
        try {
            ByteBufferUnmapper.unmap(file.getAbsolutePath(), mmap);
        } finally {
            mmap = null;
        }
    }

    // The caller is expected to hold `lock` when calling this method
    protected void incrementEntries() {
        ++entries;
    }

    protected void truncateToEntries0(int entries) {
        this.entries = entries;
        mmap.position(entries * entrySize());
    }

    /**
     * Execute the given function in a lock only if we are running on windows or z/OS. We do this
     * because Windows or z/OS won't let us resize a file while it is mmapped. As a result we have to force unmap it
     * and this requires synchronizing reads.
     */
    protected final <T, E extends Exception> T maybeLock(Lock lock, StorageAction<T, E> action) throws E {
        if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
            lock.lock();
        try {
            return action.execute();
        } finally {
            if (OperatingSystem.IS_WINDOWS || OperatingSystem.IS_ZOS)
                lock.unlock();
        }
    }

    /**
     * Find the slot in which the largest entry less than or equal to the given target key or value is stored.
     * The comparison is made using the `IndexEntry.compareTo()` method.
     *
     * @param idx The index buffer
     * @param target The index key to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target key or the index is empty
     */
    protected int largestLowerBoundSlotFor(ByteBuffer idx, long target, IndexSearchType searchEntity) {
        return indexSlotRangeFor(idx, target, searchEntity, SearchResultType.LARGEST_LOWER_BOUND);
    }

    /**
     * Find the smallest entry greater than or equal the target key or value. If none can be found, -1 is returned.
     */
    protected int smallestUpperBoundSlotFor(ByteBuffer idx, long target, IndexSearchType searchEntity) {
        return indexSlotRangeFor(idx, target, searchEntity, SearchResultType.SMALLEST_UPPER_BOUND);
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundDownToExactMultiple(67, 8) == 64
     */
    private static int roundDownToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    private static MappedByteBuffer createMappedBuffer(RandomAccessFile raf, boolean newlyCreated, long length,
                                                       boolean writable, int entrySize) throws IOException {
        MappedByteBuffer idx;
        if (writable)
            idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, length);
        else
            idx = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length);

        /* set the position in the index for the next entry */
        if (newlyCreated)
            idx.position(0);
        else
            // if this is a pre-existing index, assume it is valid and set position to last entry
            idx.position(roundDownToExactMultiple(idx.limit(), entrySize));

        return idx;
    }

    /**
     * Lookup lower or upper bounds for the given target.
     */
    private int indexSlotRangeFor(ByteBuffer idx, long target, IndexSearchType searchEntity,
                                  SearchResultType searchResultType) {
        // check if the index is empty
        if (entries == 0)
            return -1;

        int firstHotEntry = Math.max(0, entries - 1 - warmEntries());
        // check if the target offset is in the warm section of the index
        if (compareIndexEntry(parseEntry(idx, firstHotEntry), target, searchEntity) < 0) {
            return binarySearch(idx, target, searchEntity,
                searchResultType, firstHotEntry, entries - 1);
        }

        // check if the target offset is smaller than the least offset
        if (compareIndexEntry(parseEntry(idx, 0), target, searchEntity) > 0) {
            switch (searchResultType) {
                case LARGEST_LOWER_BOUND:
                    return -1;
                case SMALLEST_UPPER_BOUND:
                    return 0;
            }
        }

        return binarySearch(idx, target, searchEntity, searchResultType, 0, firstHotEntry);
    }

    private int binarySearch(ByteBuffer idx, long target, IndexSearchType searchEntity,
                             SearchResultType searchResultType, int begin, int end) {
        // binary search for the entry
        int lo = begin;
        int hi = end;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            IndexEntry found = parseEntry(idx, mid);
            int compareResult = compareIndexEntry(found, target, searchEntity);
            if (compareResult > 0)
                hi = mid - 1;
            else if (compareResult < 0)
                lo = mid;
            else
                return mid;
        }
        switch (searchResultType) {
            case LARGEST_LOWER_BOUND:
                return lo;
            case SMALLEST_UPPER_BOUND:
                if (lo == entries - 1)
                    return -1;
                else
                    return lo + 1;
            default:
                throw new IllegalStateException("Unexpected searchResultType " + searchResultType);
        }
    }

    private int compareIndexEntry(IndexEntry indexEntry, long target, IndexSearchType searchEntity) {
        int result;
        switch (searchEntity) {
            case KEY:
                result = Long.compare(indexEntry.indexKey(), target);
                break;
            case VALUE:
                result = Long.compare(indexEntry.indexValue(), target);
                break;
            default:
                throw new IllegalStateException("Unexpected IndexSearchType: " + searchEntity);
        }
        return result;
    }

    private OptionalInt toRelative(long offset) {
        long relativeOffset = offset - baseOffset;
        if (relativeOffset < 0 || relativeOffset > Integer.MAX_VALUE)
            return OptionalInt.empty();
        else
            return OptionalInt.of((int) relativeOffset);
    }

}
