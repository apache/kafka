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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractRecords implements Records {

    private final Iterable<LogRecord> records = new Iterable<LogRecord>() {
        @Override
        public Iterator<LogRecord> iterator() {
            return recordsIterator();
        }
    };

    @Override
    public boolean hasMatchingMagic(byte magic) {
        for (LogEntry entry : entries())
            if (entry.magic() != magic)
                return false;
        return true;
    }

    @Override
    public boolean hasCompatibleMagic(byte magic) {
        for (LogEntry entry : entries())
            if (entry.magic() > magic)
                return false;
        return true;
    }

    /**
     * Convert this message set to a compatible magic format.
     *
     * @param toMagic The maximum magic version to convert to. Entries with larger magic values
     *                will be converted to this magic; entries with equal or lower magic will not
     *                be converted at all.
     */
    @Override
    public Records downConvert(byte toMagic) {
        List<? extends LogEntry> entries = Utils.toList(entries().iterator());
        if (entries.isEmpty()) {
            // This indicates that the message is too large, which indicates that the buffer is not large
            // enough to hold a full log entry. We just return all the bytes in the file message set.
            // Even though the message set does not have the right format version, we expect old clients
            // to raise an error to the user after reading the message size and seeing that there
            // are not enough available bytes in the response to read the full message.
            return this;
        } else {
            List<LogEntryAndRecords> logEntryAndRecordsList = new ArrayList<>(entries.size());
            int totalSizeEstimate = 0;

            for (LogEntry entry : entries) {
                if (entry.magic() <= toMagic) {
                    totalSizeEstimate += entry.sizeInBytes();
                    logEntryAndRecordsList.add(new LogEntryAndRecords(entry, null, null));
                } else {
                    List<LogRecord> logRecords = Utils.toList(entry.iterator());
                    final long baseOffset;
                    if (entry.magic() >= LogEntry.MAGIC_VALUE_V2)
                        baseOffset = entry.baseOffset();
                    else
                        baseOffset = logRecords.get(0).offset();
                    totalSizeEstimate += estimateSizeInBytes(toMagic, baseOffset, entry.compressionType(), logRecords);
                    logEntryAndRecordsList.add(new LogEntryAndRecords(entry, logRecords, baseOffset));
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(totalSizeEstimate);
            for (LogEntryAndRecords logEntryAndRecords : logEntryAndRecordsList) {
                if (logEntryAndRecords.entry.magic() <= toMagic)
                    logEntryAndRecords.entry.writeTo(buffer);
                else
                    buffer = convertLogEntry(toMagic, buffer, logEntryAndRecords);
            }

            buffer.flip();
            return MemoryRecords.readableRecords(buffer);
        }
    }

    private ByteBuffer convertLogEntry(byte magic, ByteBuffer buffer, LogEntryAndRecords logEntryAndRecords) {
        LogEntry entry = logEntryAndRecords.entry;
        final TimestampType timestampType = entry.timestampType();
        long logAppendTime = timestampType == TimestampType.LOG_APPEND_TIME ? entry.maxTimestamp() : LogEntry.NO_TIMESTAMP;

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic, entry.compressionType(),
                timestampType, logEntryAndRecords.baseOffset, logAppendTime);
        for (LogRecord logRecord : logEntryAndRecords.records)
            builder.append(logRecord);

        builder.close();
        return builder.buffer();
    }

    /**
     * Get an iterator over the deep records.
     * @return An iterator over the records
     */
    @Override
    public Iterable<LogRecord> records() {
        return records;
    }

    private Iterator<LogRecord> recordsIterator() {
        return new AbstractIterator<LogRecord>() {
            private final Iterator<? extends LogEntry> entries = entries().iterator();
            private Iterator<LogRecord> records;

            @Override
            protected LogRecord makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (entries.hasNext()) {
                    records = entries.next().iterator();
                    return makeNext();
                }

                return allDone();
            }
        };
    }

    public static int estimateSizeInBytes(byte magic,
                                          long baseOffset,
                                          CompressionType compressionType,
                                          Iterable<LogRecord> records) {
        int size = 0;
        if (magic <= LogEntry.MAGIC_VALUE_V1) {
            for (LogRecord record : records)
                size += Records.LOG_OVERHEAD + Record.recordSize(magic, record.key(), record.value());
        } else {
            size = EosLogEntry.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                                          CompressionType compressionType,
                                          Iterable<KafkaRecord> records) {
        int size = 0;
        if (magic <= LogEntry.MAGIC_VALUE_V1) {
            for (KafkaRecord record : records)
                size += Records.LOG_OVERHEAD + Record.recordSize(magic, record.key(), record.value());
        } else {
            size = EosLogEntry.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    private static class LogEntryAndRecords {
        private final LogEntry entry;
        private final List<LogRecord> records;
        private final Long baseOffset;

        private LogEntryAndRecords(LogEntry entry, List<LogRecord> records, Long baseOffset) {
            this.entry = entry;
            this.records = records;
            this.baseOffset = baseOffset;
        }
    }

}
