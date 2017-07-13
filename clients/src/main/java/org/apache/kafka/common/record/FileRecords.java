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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Records} implementation backed by a file. An optional start and end position can be applied to this
 * instance to enable slicing a range of the log records.
 */
public class FileRecords extends AbstractRecords implements Closeable {
    private final boolean isSlice;
    private final int start;
    private final int end;

    private final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;

    // mutable state
    private final AtomicInteger size;
    private final FileChannel channel;
    private volatile File file;

    /**
     * The {@code FileRecords.open} methods should be used instead of this constructor whenever possible.
     * The constructor is visible for tests.
     */
    public FileRecords(File file,
                       FileChannel channel,
                       int start,
                       int end,
                       boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        this.size = new AtomicInteger();

        if (isSlice) {
            // don't check the file size if this is just a slice view
            size.set(end - start);
        } else {
            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }

        batches = batchesFrom(start);
    }

    @Override
    public int sizeInBytes() {
        return size.get();
    }

    /**
     * Get the underlying file.
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or the end of the file
     * is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @return The same buffer
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)} for details on the
     * possible exceptions
     */
    public ByteBuffer readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position + this.start);
        buffer.flip();
        return buffer;
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the given position
     * and with the given size limit.
     *
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     *
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileRecords read(int position, int size) throws IOException {
        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size);

        final int end;
        // handle integer overflow
        if (this.start + position + size < 0)
            end = sizeInBytes();
        else
            end = Math.min(this.start + position + size, sizeInBytes());
        return new FileRecords(file, channel, this.start + position, end, true);
    }

    /**
     * Append log batches to the buffer
     * @param records The records to append
     * @return the number of bytes written to the underlying file
     */
    public int append(MemoryRecords records) throws IOException {
        int written = records.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this record set
     */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     * @return True iff this message set was deleted.
     */
    public boolean delete() {
        Utils.closeQuietly(channel, "FileChannel");
        return file.delete();
    }

    /**
     * Trim file when close or roll to next file
     */
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Update the file reference (to be used with caution since this does not reopen the file channel)
     * @param file The new file to use
     */
    public void setFile(File file) {
        this.file = file;
    }

    /**
     * Rename the file that backs this message set
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath());
        } finally {
            this.file = f;
        }
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     * In some versions of the JDK truncating to the same size as the file message set will cause an
     * update of the files mtime, so truncate is only performed if the targetSize is smaller than the
     * size of the underlying FileChannel.
     * It is expected that no other threads will do writes to the log when this function is called.
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    @Override
    public Records downConvert(byte toMagic, long firstOffset) {
        List<? extends RecordBatch> batches = Utils.toList(batches().iterator());
        if (batches.isEmpty()) {
            // This indicates that the message is too large, which means that the buffer is not large
            // enough to hold a full record batch. We just return all the bytes in the file message set.
            // Even though the message set does not have the right format version, we expect old clients
            // to raise an error to the user after reading the message size and seeing that there
            // are not enough available bytes in the response to read the full message. Note that this is
            // only possible prior to KIP-74, after which the broker was changed to always return at least
            // one full message, even if it requires exceeding the max fetch size requested by the client.
            return this;
        } else {
            return downConvert(batches, toMagic, firstOffset);
        }
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        long position = start + offset;
        int count = Math.min(length, oldSize);
        final long bytesTransferred;
        if (destChannel instanceof TransportLayer) {
            TransportLayer tl = (TransportLayer) destChannel;
            bytesTransferred = tl.transferFrom(channel, position, count);
        } else {
            bytesTransferred = channel.transferTo(position, count, destChannel);
        }
        return bytesTransferred;
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position and the size of the message (including log overhead) at the returned offset. If
     * no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
            long offset = batch.lastOffset();
            if (offset >= targetOffset)
                return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
        }
        return null;
    }

    /**
     * Search forward for the first message that meets the following requirements:
     * - Message's timestamp is greater than or equals to the targetTimestamp.
     * - Message's position in the log file is greater than or equals to the startingPosition.
     * - Message's offset is greater than or equals to the startingOffset.
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset) {
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                // We found a message
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return new TimestampAndOffset(timestamp, record.offset());
                }
            }
        }
        return null;
    }

    /**
     * Return the largest timestamp of the messages after a given position in this file message set.
     * @param startingPosition The starting position.
     * @return The largest timestamp of the messages after the given position.
     */
    public TimestampAndOffset largestTimestampAfter(int startingPosition) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;

        for (RecordBatch batch : batchesFrom(startingPosition)) {
            long timestamp = batch.maxTimestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                offsetOfMaxTimestamp = batch.lastOffset();
            }
        }
        return new TimestampAndOffset(maxTimestamp, offsetOfMaxTimestamp);
    }

    /**
     * Get an iterator over the record batches in the file. Note that the batches are
     * backed by the open file channel. When the channel is closed (i.e. when this instance
     * is closed), the batches will generally no longer be readable.
     * @return An iterator over the batches
     */
    @Override
    public Iterable<FileChannelRecordBatch> batches() {
        return batches;
    }

    private Iterable<FileChannelRecordBatch> batchesFrom(final int start) {
        return new Iterable<FileChannelRecordBatch>() {
            @Override
            public Iterator<FileChannelRecordBatch> iterator() {
                return batchIterator(start);
            }
        };
    }

    private Iterator<FileChannelRecordBatch> batchIterator(int start) {
        final int end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(channel, start, end);
        return new RecordBatchIterator<>(inputStream);
    }

    public static FileRecords open(File file,
                                   boolean mutable,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileRecords(file, channel, 0, end, false);
    }

    public static FileRecords open(File file,
                                   boolean fileAlreadyExists,
                                   int initFileSize,
                                   boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileRecords open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * Open a channel for the given file
     * For windows NTFS and some old LINUX file system, set preallocate to true and initFileSize
     * with one value (for example 512 * 1025 *1024 ) can improve the kafka produce performance.
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private static FileChannel openChannel(File file,
                                           boolean mutable,
                                           boolean fileAlreadyExists,
                                           int initFileSize,
                                           boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists) {
                return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            } else {
                if (preallocate) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                        randomAccessFile.setLength(initFileSize);
                    }
                    return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
                } else {
                    return FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
                }
            }
        } else {
            return FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
    }

    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset &&
                    position == that.position &&
                    size == that.size;

        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition(" +
                    "offset=" + offset +
                    ", position=" + position +
                    ", size=" + size +
                    ')';
        }
    }

    public static class TimestampAndOffset {
        public final long timestamp;
        public final long offset;

        public TimestampAndOffset(long timestamp, long offset) {
            this.timestamp = timestamp;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TimestampAndOffset that = (TimestampAndOffset) o;

            if (timestamp != that.timestamp) return false;
            return offset == that.offset;
        }

        @Override
        public int hashCode() {
            int result = (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (offset ^ (offset >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "TimestampAndOffset(" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    ')';
        }
    }

}
