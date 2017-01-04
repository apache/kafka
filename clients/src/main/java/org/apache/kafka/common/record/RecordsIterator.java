/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * An iterator which handles both the shallow and deep iteration of record sets.
 */
public class RecordsIterator extends AbstractIterator<LogEntry> {
    private final boolean shallow;
    private final boolean ensureMatchingMagic;
    private final int maxRecordSize;
    private final ShallowRecordsIterator<?> shallowIter;
    private DeepRecordsIterator innerIter;

    public RecordsIterator(LogInputStream<?> logInputStream,
                           boolean shallow,
                           boolean ensureMatchingMagic,
                           int maxRecordSize) {
        this.shallowIter = new ShallowRecordsIterator<>(logInputStream);
        this.shallow = shallow;
        this.ensureMatchingMagic = ensureMatchingMagic;
        this.maxRecordSize = maxRecordSize;
    }

    /**
     * Get a shallow iterator over the given input stream.
     * @param logInputStream The log input stream to read the entries from
     * @param <T> The type of the log entry
     * @return The shallow iterator.
     */
    public static <T extends LogEntry> Iterator<T> shallowIterator(LogInputStream<T> logInputStream) {
        return new ShallowRecordsIterator<>(logInputStream);
    }

    @Override
    protected LogEntry makeNext() {
        if (innerDone()) {
            if (!shallowIter.hasNext())
                return allDone();

            LogEntry entry = shallowIter.next();

            // decide whether to go shallow or deep iteration if it is compressed
            if (shallow || !entry.isCompressed()) {
                return entry;
            } else {
                // init the inner iterator with the value payload of the message,
                // which will de-compress the payload to a set of messages;
                // since we assume nested compression is not allowed, the deep iterator
                // would not try to further decompress underlying messages
                // There will be at least one element in the inner iterator, so we don't
                // need to call hasNext() here.
                innerIter = new DeepRecordsIterator(entry, ensureMatchingMagic, maxRecordSize);
                return innerIter.next();
            }
        } else {
            return innerIter.next();
        }
    }

    private boolean innerDone() {
        return innerIter == null || !innerIter.hasNext();
    }

    private static final class DataLogInputStream implements LogInputStream<LogEntry> {
        private final DataInputStream stream;
        protected final int maxMessageSize;

        DataLogInputStream(DataInputStream stream, int maxMessageSize) {
            this.stream = stream;
            this.maxMessageSize = maxMessageSize;
        }

        public LogEntry nextEntry() throws IOException {
            try {
                long offset = stream.readLong();
                int size = stream.readInt();
                if (size < Record.RECORD_OVERHEAD_V0)
                    throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
                if (size > maxMessageSize)
                    throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

                byte[] recordBuffer = new byte[size];
                stream.readFully(recordBuffer, 0, size);
                ByteBuffer buf = ByteBuffer.wrap(recordBuffer);
                return LogEntry.create(offset, new Record(buf));
            } catch (EOFException e) {
                return null;
            }
        }
    }

    private static class ShallowRecordsIterator<T extends LogEntry> extends AbstractIterator<T> {
        private final LogInputStream<T> logStream;

        public ShallowRecordsIterator(LogInputStream<T> logStream) {
            this.logStream = logStream;
        }

        @Override
        protected T makeNext() {
            try {
                T entry = logStream.nextEntry();
                if (entry == null)
                    return allDone();
                return entry;
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }
    }

    public static class DeepRecordsIterator extends AbstractIterator<LogEntry> {
        private final ArrayDeque<LogEntry> logEntries;
        private final long absoluteBaseOffset;
        private final byte wrapperMagic;

        public DeepRecordsIterator(LogEntry wrapperEntry, boolean ensureMatchingMagic, int maxMessageSize) {
            Record wrapperRecord = wrapperEntry.record();
            this.wrapperMagic = wrapperRecord.magic();

            CompressionType compressionType = wrapperRecord.compressionType();
            ByteBuffer buffer = wrapperRecord.value();
            DataInputStream stream = MemoryRecordsBuilder.wrapForInput(new ByteBufferInputStream(buffer), compressionType, wrapperRecord.magic());
            LogInputStream logStream = new DataLogInputStream(stream, maxMessageSize);

            long wrapperRecordOffset = wrapperEntry.offset();
            long wrapperRecordTimestamp = wrapperRecord.timestamp();
            this.logEntries = new ArrayDeque<>();

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                while (true) {
                    LogEntry logEntry = logStream.nextEntry();
                    if (logEntry == null)
                        break;

                    Record record = logEntry.record();
                    byte magic = record.magic();

                    if (ensureMatchingMagic && magic != wrapperMagic)
                        throw new InvalidRecordException("Compressed message magic does not match wrapper magic");

                    if (magic > Record.MAGIC_VALUE_V0) {
                        Record recordWithTimestamp = new Record(
                                record.buffer(),
                                wrapperRecordTimestamp,
                                wrapperRecord.timestampType()
                        );
                        logEntry = LogEntry.create(logEntry.offset(), recordWithTimestamp);
                    }
                    logEntries.addLast(logEntry);
                }
                if (wrapperMagic > Record.MAGIC_VALUE_V0)
                    this.absoluteBaseOffset = wrapperRecordOffset - logEntries.getLast().offset();
                else
                    this.absoluteBaseOffset = -1;
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                Utils.closeQuietly(stream, "records iterator stream");
            }
        }

        @Override
        protected LogEntry makeNext() {
            if (logEntries.isEmpty())
                return allDone();

            LogEntry entry = logEntries.remove();

            // Convert offset to absolute offset if needed.
            if (absoluteBaseOffset >= 0) {
                long absoluteOffset = absoluteBaseOffset + entry.offset();
                entry = LogEntry.create(absoluteOffset, entry.record());
            }

            if (entry.isCompressed())
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }
    }

}
