/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.record.ByteBufferLogInputStream.ByteBufferLogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Records} implementation backed by a ByteBuffer. This is used only for reading or
 * modifying in-place an existing buffer of log entries. To create a new buffer see {@link MemoryRecordsBuilder},
 * or one of the {@link #builder(ByteBuffer, byte, CompressionType, TimestampType) builder} variants.
 */
public class MemoryRecords extends AbstractRecords {

    public final static MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<ByteBufferLogEntry> shallowEntries = new Iterable<ByteBufferLogEntry>() {
        @Override
        public Iterator<ByteBufferLogEntry> iterator() {
            return shallowIterator();
        }
    };

    private final Iterable<LogEntry> deepEntries = deepEntries(false);

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
        for (LogEntry entry : shallowEntries())
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
    public FilterResult filterTo(LogEntryFilter filter, ByteBuffer destinationBuffer) {
        return filterTo(shallowEntries(), filter, destinationBuffer);
    }

    private static FilterResult filterTo(Iterable<ByteBufferLogEntry> fromShallowEntries, LogEntryFilter filter,
                                       ByteBuffer destinationBuffer) {
        long maxTimestamp = Record.NO_TIMESTAMP;
        long maxOffset = -1L;
        long shallowOffsetOfMaxTimestamp = -1L;
        int messagesRead = 0;
        int bytesRead = 0;
        int messagesRetained = 0;
        int bytesRetained = 0;

        for (ByteBufferLogEntry shallowEntry : fromShallowEntries) {
            bytesRead += shallowEntry.sizeInBytes();

            // We use the absolute offset to decide whether to retain the message or not (this is handled by the
            // deep iterator). Because of KAFKA-4298, we have to allow for the possibility that a previous version
            // corrupted the log by writing a compressed message set with a wrapper magic value not matching the magic
            // of the inner messages. This will be fixed as we recopy the messages to the destination buffer.

            Record shallowRecord = shallowEntry.record();
            byte shallowMagic = shallowRecord.magic();
            boolean writeOriginalEntry = true;
            List<LogEntry> retainedEntries = new ArrayList<>();

            for (LogEntry deepEntry : shallowEntry) {
                Record deepRecord = deepEntry.record();
                messagesRead += 1;

                if (filter.shouldRetain(deepEntry)) {
                    // Check for log corruption due to KAFKA-4298. If we find it, make sure that we overwrite
                    // the corrupted entry with correct data.
                    if (shallowMagic != deepRecord.magic())
                        writeOriginalEntry = false;

                    if (deepEntry.offset() > maxOffset)
                        maxOffset = deepEntry.offset();

                    retainedEntries.add(deepEntry);
                } else {
                    writeOriginalEntry = false;
                }
            }

            if (writeOriginalEntry) {
                // There are no messages compacted out and no message format conversion, write the original message set back
                shallowEntry.writeTo(destinationBuffer);
                messagesRetained += retainedEntries.size();
                bytesRetained += shallowEntry.sizeInBytes();

                if (shallowRecord.timestamp() > maxTimestamp) {
                    maxTimestamp = shallowRecord.timestamp();
                    shallowOffsetOfMaxTimestamp = shallowEntry.offset();
                }
            } else if (!retainedEntries.isEmpty()) {
                ByteBuffer slice = destinationBuffer.slice();
                MemoryRecordsBuilder builder = builderWithEntries(slice, shallowRecord.timestampType(), shallowRecord.compressionType(),
                        shallowRecord.timestamp(), retainedEntries);
                MemoryRecords records = builder.build();
                destinationBuffer.position(destinationBuffer.position() + slice.position());
                messagesRetained += retainedEntries.size();
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
    public Iterable<ByteBufferLogEntry> shallowEntries() {
        return shallowEntries;
    }

    private Iterator<ByteBufferLogEntry> shallowIterator() {
        return RecordsIterator.shallowIterator(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    @Override
    public Iterable<LogEntry> deepEntries() {
        return deepEntries;
    }

    public Iterable<LogEntry> deepEntries(final boolean ensureMatchingMagic) {
        return new Iterable<LogEntry>() {
            @Override
            public Iterator<LogEntry> iterator() {
                return deepIterator(ensureMatchingMagic, Integer.MAX_VALUE);
            }
        };
    }

    private Iterator<LogEntry> deepIterator(boolean ensureMatchingMagic, int maxMessageSize) {
        return new RecordsIterator(new ByteBufferLogInputStream(buffer.duplicate(), maxMessageSize), false,
                ensureMatchingMagic, maxMessageSize);
    }

    @Override
    public String toString() {
        Iterator<LogEntry> iter = deepEntries().iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        while (iter.hasNext()) {
            LogEntry entry = iter.next();
            builder.append('(');
            builder.append("offset=");
            builder.append(entry.offset());
            builder.append(",");
            builder.append("record=");
            builder.append(entry.record());
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

    public interface LogEntryFilter {
        boolean shouldRetain(LogEntry entry);
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

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               int writeLimit) {
        return new MemoryRecordsBuilder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, 0L, System.currentTimeMillis(), writeLimit);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset,
                                               long logAppendTime) {
        return new MemoryRecordsBuilder(buffer, magic, compressionType, timestampType, baseOffset, logAppendTime, buffer.capacity());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType) {
        // use the buffer capacity as the default write limit
        return builder(buffer, compressionType, timestampType, buffer.capacity());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType) {
        return builder(buffer, magic, compressionType, timestampType, 0L);
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, Record.CURRENT_MAGIC_VALUE, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryRecordsBuilder builder(ByteBuffer buffer,
                                               byte magic,
                                               CompressionType compressionType,
                                               TimestampType timestampType,
                                               long baseOffset) {
        return builder(buffer, magic, compressionType, timestampType, baseOffset, System.currentTimeMillis());
    }

    public static MemoryRecords readableRecords(ByteBuffer buffer) {
        return new MemoryRecords(buffer);
    }

    public static MemoryRecords withLogEntries(CompressionType compressionType, List<LogEntry> entries) {
        return withLogEntries(TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), entries);
    }

    public static MemoryRecords withLogEntries(LogEntry ... entries) {
        return withLogEntries(CompressionType.NONE, Arrays.asList(entries));
    }

    public static MemoryRecords withRecords(CompressionType compressionType, long initialOffset, List<Record> records) {
        return withRecords(initialOffset, TimestampType.CREATE_TIME, compressionType, System.currentTimeMillis(), records);
    }

    public static MemoryRecords withRecords(Record ... records) {
        return withRecords(CompressionType.NONE, 0L, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(long initialOffset, Record ... records) {
        return withRecords(CompressionType.NONE, initialOffset, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(CompressionType compressionType, Record ... records) {
        return withRecords(compressionType, 0L, Arrays.asList(records));
    }

    public static MemoryRecords withRecords(TimestampType timestampType, CompressionType compressionType, Record ... records) {
        return withRecords(0L, timestampType, compressionType, System.currentTimeMillis(), Arrays.asList(records));
    }

    public static MemoryRecords withRecords(long initialOffset,
                                            TimestampType timestampType,
                                            CompressionType compressionType,
                                            long logAppendTime,
                                            List<Record> records) {
        return withLogEntries(timestampType, compressionType, logAppendTime, buildLogEntries(initialOffset, records));
    }

    private static MemoryRecords withLogEntries(TimestampType timestampType,
                                                CompressionType compressionType,
                                                long logAppendTime,
                                                List<LogEntry> entries) {
        if (entries.isEmpty())
            return MemoryRecords.EMPTY;
        return builderWithEntries(timestampType, compressionType, logAppendTime, entries).build();
    }

    private static List<LogEntry> buildLogEntries(long initialOffset, List<Record> records) {
        List<LogEntry> entries = new ArrayList<>();
        for (Record record : records)
            entries.add(LogEntry.create(initialOffset++, record));
        return entries;
    }

    public static MemoryRecordsBuilder builderWithEntries(TimestampType timestampType,
                                                          CompressionType compressionType,
                                                          long logAppendTime,
                                                          List<LogEntry> entries) {
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize(compressionType, entries));
        return builderWithEntries(buffer, timestampType, compressionType, logAppendTime, entries);
    }

    private static MemoryRecordsBuilder builderWithEntries(ByteBuffer buffer,
                                                           TimestampType timestampType,
                                                           CompressionType compressionType,
                                                           long logAppendTime,
                                                           List<LogEntry> entries) {
        if (entries.isEmpty())
            throw new IllegalArgumentException("entries must not be empty");

        LogEntry firstEntry = entries.iterator().next();
        long firstOffset = firstEntry.offset();
        byte magic = firstEntry.record().magic();

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType,
                firstOffset, logAppendTime);
        for (LogEntry entry : entries)
            builder.appendWithOffset(entry.offset(), entry.record());

        return builder;
    }

}
