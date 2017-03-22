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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of record batches. To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, CompressionType, TimestampType, long)} variants.
 */
public class MemoryRecords extends AbstractRecords {

    public final static MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<MutableRecordBatch> batches = new Iterable<MutableRecordBatch>() {
        @Override
        public Iterator<MutableRecordBatch> iterator() {
            return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
        }
    };

    private int validBytes = -1;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        this.buffer = buffer;
    }

    @Override
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public long writeTo(GatheringByteChannel channel, long position, int length) throws IOException {
        if (position > Integer.MAX_VALUE)
            throw new IllegalArgumentException("position should not be greater than Integer.MAX_VALUE: " + position);
        if (position + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());

        int pos = (int) position;
        ByteBuffer dup = buffer.duplicate();
        dup.position(pos);
        dup.limit(pos + length);
        return channel.write(dup);
    }

    /**
     * Write all records to the given channel (including partial records).
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages. This
     * may be smaller than what is returned by {@link #sizeInBytes()}.
     * @return The number of valid bytes
     */
    public int validBytes() {
        if (validBytes >= 0)
            return validBytes;

        int bytes = 0;
        for (RecordBatch batch : batches())
            bytes += batch.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    @Override
    public MemoryRecords downConvert(byte toMagic) {
        return downConvert(batches(), toMagic);
    }

    /**
     * Filter the records into the provided ByteBuffer.
     * @param filter The filter function
     * @param destinationBuffer The byte buffer to write the filtered records to
     * @return A FilterResult with a summary of the output (for metrics)
     */
    public FilterResult filterTo(RecordFilter filter, ByteBuffer destinationBuffer) {
        return filterTo(batches(), filter, destinationBuffer);
    }

    private static FilterResult filterTo(Iterable<MutableRecordBatch> batches, RecordFilter filter,
                                         ByteBuffer destinationBuffer) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long maxOffset = -1L;
        long shallowOffsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        for (MutableRecordBatch batch : batches) {
            bytesRead += batch.sizeInBytes();

            // We use the absolute offset to decide whether to retain the message or not. Due to KAFKA-4298, we have to
            // allow for the possibility that a previous version corrupted the log by writing a compressed record batch
            // with a magic value not matching the magic of the records (magic < 2). This will be fixed as we
            // recopy the messages to the destination buffer.

            byte batchMagic = batch.magic();
            boolean writeOriginalEntry = true;
            long firstOffset = -1;
            List<Record> retainedRecords = new ArrayList<>();

            for (Record record : batch) {
                if (firstOffset < 0)
                    firstOffset = record.offset();

                messagesRead += 1;

                if (filter.shouldRetain(record)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted batch with correct data.
                    if (!record.hasMagic(batchMagic))
                        writeOriginalEntry = false;

                    if (record.offset() > maxOffset)
                        maxOffset = record.offset();

                    retainedRecords.add(record);
                } else {
                    writeOriginalEntry = false;
                }
            }

            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                batch.writeTo(destinationBuffer);
                messagesRetained += retainedRecords.size();
                bytesRetained += batch.sizeInBytes();
                if (batch.maxTimestamp() > maxTimestamp) {
                    maxTimestamp = batch.maxTimestamp();
                    shallowOffsetOfMaxTimestamp = batch.lastOffset();
                }
            } else if (!retainedRecords.isEmpty()) {
                ByteBuffer slice = destinationBuffer.slice();
                TimestampType timestampType = batch.timestampType();
                long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;
                MemoryRecordsBuilder builder = builder(slice, batch.magic(), batch.compressionType(), timestampType,
                        firstOffset, logAppendTime);

                for (Record record : retainedRecords)
                    builder.append(record);

                MemoryRecords records = builder.build();
                destinationBuffer.position(destinationBuffer.position() + slice.position());
                messagesRetained += retainedRecords.size();
                bytesRetained += records.sizeInBytes();

                MemoryRecordsBuilder.RecordsInfo info = builder.info();
                if (info.maxTimestamp > maxTimestamp) {
                    maxTimestamp = info.maxTimestamp;
                    shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp;
                }
            }
        }

        return new FilterResult(messagesRead, bytesRead, messagesRetained, bytesRetained, maxOffset, maxTimestamp, shallowOffsetOfMaxTimestamp);
    }

    /**
     * Get the byte buffer that backs this instance for reading.
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterable<MutableRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        Iterator<Record> iter = records().iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            Record record = iter.next();
            builder.append('(');
            builder.append("record=");
            builder.append(record);
            builder.append(")");
            if (iter.hasNext())
                builder.append(", ");
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public interface RecordFilter {
        boolean shouldRetain(Record record);
    }

    public static class FilterResult {
        public final int messagesRead;
        public final int bytesRead;
        public final int messagesRetained;
        public final int bytesRetained;
        public final long maxOffset;
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public FilterResult(int messagesRead,
                            int bytesRead,
                            int messagesRetained,
                            int bytesRetained,
                            long maxOffset,
                            long maxTimestamp,
                            long shallowOffsetOfMaxTimestamp) {
            this.messagesRead = messagesRead;
            this.bytesRead = bytesRead;
            this.messagesRetained = messagesRetained;
            this.bytesRetained = bytesRetained;
            this.maxOffset = maxOffset;
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               int writeLimit) {
        return new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L,
                System.currentTimeMillis(), RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
                RecordBatch.UNKNOWN_PARTITION_LEADER_EPOCH, writeLimit);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime,
                RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime,
                                               long pid,
                                               short epoch,
                                               int baseSequence) {
        return new MemoryRecordsBuilder(buffer, magic, compressionType, timestampType, baseOffset,
                logAppendTime, pid, epoch, baseSequence, false, RecordBatch.UNKNOWN_PARTITION_LEADER_EPOCH,
                buffer.capacity());
    }

    public static MemoryRecords withRecords(CompressionType compressionType, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, compressionType, records);
    }

    public static MemoryRecords withRecords(byte magic, CompressionType compressionType, SimpleRecord... records) {
        return withRecords(magic, 0L, compressionType, TimestampType.CREATE_TIME, records);
    }

    public static MemoryRecords withRecords(long initialOffset, CompressionType compressionType, SimpleRecord... records) {
        return withRecords(RecordBatch.CURRENT_MAGIC_VALUE, initialOffset, compressionType, TimestampType.CREATE_TIME, records);
    }

    public static MemoryRecords withRecords(byte magic, long initialOffset, CompressionType compressionType,
                                            TimestampType timestampType, SimpleRecord... records) {
        if (records.length == 0)
            return MemoryRecords.EMPTY;
        int sizeEstimate = AbstractRecords.estimateSizeInBytes(magic, compressionType, Arrays.asList(records));
        ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);
        MemoryRecordsBuilder builder = builder(buffer, magic, compressionType, timestampType, initialOffset);
        for (SimpleRecord record : records)
            builder.append(record);
        return builder.build();
    }

}
