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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.AbstractLegacyRecordBatch.LegacyFileChannelRecordBatch;
import org.apache.kafka.common.record.DefaultRecordBatch.DefaultFileChannelRecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * A log input stream which is backed by a {@link FileChannel}.
 */
public class FileLogInputStream implements LogInputStream<FileLogInputStream.FileChannelRecordBatch> {
    private int position;
    private final int end;
    private final FileRecords fileRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /**
     * Create a new log input stream over the FileChannel
     * @param records Underlying FileRecords instance
     * @param start Position in the file channel to start from
     * @param end Position in the file channel not to read past
     */
    FileLogInputStream(FileRecords records,
                       int start,
                       int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
    }

    @Override
    public FileChannelRecordBatch nextBatch() throws IOException {
        FileChannel channel = fileRecords.channel();
        if (position + HEADER_SIZE_UP_TO_MAGIC >= end)
            return null;

        logHeaderBuffer.rewind();
        Utils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(OFFSET_OFFSET);
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);

        // V0 has the smallest overhead, stricter checking is done later
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                            "overhead (%d) in file %s.", size, LegacyRecord.RECORD_OVERHEAD_V0, fileRecords.file()));

        if (position + LOG_OVERHEAD + size > end)
            return null;

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        final FileChannelRecordBatch batch;

        if (magic < RecordBatch.MAGIC_VALUE_V2)
            batch = new LegacyFileChannelRecordBatch(offset, magic, channel, position, size);
        else
            batch = new DefaultFileChannelRecordBatch(offset, magic, channel, position, size);

        position += batch.sizeInBytes();
        return batch;
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the record batches
     * without needing to read the record data into memory until it is needed. The downside
     * is that entries will generally no longer be readable when the underlying channel is closed.
     */
    public abstract static class FileChannelRecordBatch extends AbstractRecordBatch {
        protected final long offset;
        protected final byte magic;
        protected final FileChannel channel;
        protected final int position;
        protected final int batchSize;

        private RecordBatch fullBatch;
        private RecordBatch batchHeader;

        FileChannelRecordBatch(long offset,
                               byte magic,
                               FileChannel channel,
                               int position,
                               int batchSize) {
            this.offset = offset;
            this.magic = magic;
            this.channel = channel;
            this.position = position;
            this.batchSize = batchSize;
        }

        @Override
        public CompressionType compressionType() {
            return loadBatchHeader().compressionType();
        }

        @Override
        public TimestampType timestampType() {
            return loadBatchHeader().timestampType();
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public long maxTimestamp() {
            return loadBatchHeader().maxTimestamp();
        }

        public int position() {
            return position;
        }

        @Override
        public byte magic() {
            return magic;
        }

        @Override
        public Iterator<Record> iterator() {
            return loadFullBatch().iterator();
        }

        @Override
        public CloseableIterator<Record> streamingIterator(BufferSupplier bufferSupplier) {
            return loadFullBatch().streamingIterator(bufferSupplier);
        }

        @Override
        public boolean isValid() {
            return loadFullBatch().isValid();
        }

        @Override
        public void ensureValid() {
            loadFullBatch().ensureValid();
        }

        @Override
        public int sizeInBytes() {
            return LOG_OVERHEAD + batchSize;
        }

        @Override
        public void writeTo(ByteBuffer buffer) {
            try {
                int limit = buffer.limit();
                buffer.limit(buffer.position() + sizeInBytes());
                Utils.readFully(channel, buffer, position);
                buffer.limit(limit);
            } catch (IOException e) {
                throw new KafkaException("Failed to read record batch at position " + position + " from file channel " +
                        channel, e);
            }
        }

        protected abstract RecordBatch toMemoryRecordBatch(ByteBuffer buffer);

        protected abstract int headerSize();

        protected RecordBatch loadFullBatch() {
            if (fullBatch == null) {
                batchHeader = null;
                fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
            }
            return fullBatch;
        }

        protected RecordBatch loadBatchHeader() {
            if (fullBatch != null)
                return fullBatch;

            if (batchHeader == null)
                batchHeader = loadBatchWithSize(headerSize(), "record batch header");

            return batchHeader;
        }

        private RecordBatch loadBatchWithSize(int size, String description) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(size);
                Utils.readFullyOrFail(channel, buffer, position, description);
                buffer.rewind();
                return toMemoryRecordBatch(buffer);
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FileChannelRecordBatch that = (FileChannelRecordBatch) o;

            return offset == that.offset &&
                    position == that.position &&
                    batchSize == that.batchSize &&
                    (channel == null ? that.channel == null : channel.equals(that.channel));
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + (channel != null ? channel.hashCode() : 0);
            result = 31 * result + position;
            result = 31 * result + batchSize;
            return result;
        }

        @Override
        public String toString() {
            return "FileChannelRecordBatch(magic: " + magic +
                    ", offset: " + offset +
                    ", size: " + batchSize + ")";
        }
    }
}
