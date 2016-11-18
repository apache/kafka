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
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of log logEntries. To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(byte, CompressionType, long)} variants.
 */
public class MemoryRecords extends AbstractRecords {

    public final static MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<LogEntry.MutableLogEntry> logEntries = new Iterable<LogEntry.MutableLogEntry>() {
        @Override
        public Iterator<LogEntry.MutableLogEntry> iterator() {
            return new LogEntryIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
        }
    };

    private int validBytes = -1;

    // Construct a writable memory records
    private MemoryRecords(ByteBuffer buffer) {
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
        for (LogEntry entry : entries())
            bytes += entry.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    /**
     * Filter the records into the provided ByteBuffer.
     * @param filter The filter function
     * @param destinationBuffer The byte buffer to write the filtered records to
     * @return A FilterResult with a summary of the output (for metrics)
     */
    public FilterResult filterTo(LogRecordFilter filter, ByteBuffer destinationBuffer) {
        return filterTo(entries(), filter, destinationBuffer);
    }

    private static FilterResult filterTo(Iterable<LogEntry.MutableLogEntry> logEntries, LogRecordFilter filter,
                                         ByteBuffer destinationBuffer) {
        long maxTimestamp = Record.NO_TIMESTAMP;
        long maxOffset = -1L;
        long shallowOffsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        for (LogEntry.MutableLogEntry logEntry : logEntries) {
            bytesRead += logEntry.sizeInBytes();

            // We use the absolute offset to decide whether to retain the message or not Due KAFKA-4298, we have to
            // allow for the possibility that a previous version corrupted the log by writing a compressed message
            // set with a wrapper magic value not matching the magic of the inner messages. This will be fixed as we
            // recopy the messages to the destination buffer.

            byte shallowMagic = logEntry.magic();
            boolean writeOriginalEntry = true;
            long firstOffset = -1;
            List<LogRecord> retainedRecords = new ArrayList<>();

            for (LogRecord record : logEntry) {
                if (firstOffset < 0)
                    firstOffset = record.offset();

                messagesRead += 1;

                if (filter.shouldRetain(record)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted entry with correct data.
                    if (!record.hasMagic(shallowMagic))
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
                logEntry.writeTo(destinationBuffer);
                messagesRetained += retainedRecords.size();
                bytesRetained += logEntry.sizeInBytes();
                if (logEntry.timestamp() > maxTimestamp) {
                    maxTimestamp = logEntry.timestamp();
                    shallowOffsetOfMaxTimestamp = logEntry.lastOffset();
                }
            } else if (!retainedRecords.isEmpty()) {
                ByteBuffer slice = destinationBuffer.slice();
                TimestampType timestampType = logEntry.timestampType();
                long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? logEntry.timestamp() : Record.NO_TIMESTAMP;
                MemoryRecordsBuilder builder = builder(slice, logEntry.magic(), logEntry.compressionType(), timestampType,
                        firstOffset, logAppendTime);

                for (LogRecord record : retainedRecords)
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
    public Iterable<LogEntry.MutableLogEntry> entries() {
        return logEntries;
    }

    @Override
    public String toString() {
        Iterator<LogRecord> iter = records().iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogRecord record = iter.next();
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemoryRecords that = (MemoryRecords) o;

        return buffer.equals(that.buffer);
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    public interface LogRecordFilter {
        boolean shouldRetain(LogRecord record);
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

    public static MemoryRecordsBuilder builder(CompressionType compressionType, long baseOffset) {
        return builder(compressionType, TimestampType.CREATE_TIME, baseOffset);
    }

    public static MemoryRecordsBuilder builder(CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        return builder(buffer, compressionType, timestampType, baseOffset);
    }

    public static MemoryRecordsBuilder builder(byte magic, CompressionType compressionType, long baseOffset) {
        return builder(magic, compressionType, TimestampType.CREATE_TIME, baseOffset);
    }

    public static MemoryRecordsBuilder builder(byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        return builder(buffer, magic, compressionType, timestampType, baseOffset);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               int writeLimit) {
        return new MemoryRecordsBuilder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L,
                System.currentTimeMillis(), 0L, (short) 0, 0, writeLimit);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset,
                System.currentTimeMillis(), 0L, (short) 0, 0);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis(),
                0L, (short) 0, 0);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime, 0L, (short) 0, 0);
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
                logAppendTime, pid, epoch, baseSequence, buffer.capacity());
    }

    public static MemoryRecords withRecords(CompressionType compressionType, KafkaRecord ... records) {
        return withRecords(Record.CURRENT_MAGIC_VALUE, compressionType, records);
    }

    public static MemoryRecords withRecords(byte magic, CompressionType compressionType, KafkaRecord ... records) {
        return withRecords(magic, 0L, compressionType, records);
    }

    public static MemoryRecords withRecords(long initialOffset, CompressionType compressionType, KafkaRecord ... records) {
        return withRecords(Record.CURRENT_MAGIC_VALUE, initialOffset, compressionType, records);
    }

    public static MemoryRecords withRecords(byte magic, long initialOffset, CompressionType compressionType,
                                            KafkaRecord ... records) {
        // FIXME: Compute buffer size more smartlier
        ByteBuffer buffer = ByteBuffer.allocate(4096);
        MemoryRecordsBuilder builder = builder(buffer, magic, compressionType, TimestampType.CREATE_TIME, initialOffset);
        for (KafkaRecord record : records)
            builder.append(record);
        return builder.build();
    }

}
