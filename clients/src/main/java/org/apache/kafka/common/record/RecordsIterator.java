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
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

public class RecordsIterator extends AbstractIterator<LogEntry> {
    private final LogInputStream logStream;
    private final boolean shallow;
    private DeepRecordsIterator innerIter;

    public RecordsIterator(LogInputStream logStream, boolean shallow) {
        this.logStream = logStream;
        this.shallow = shallow;
    }

    /*
     * Read the next record from the buffer.
     *
     * Note that in the compressed message set, each message value size is set as the size of the un-compressed
     * version of the message value, so when we do de-compression allocating an array of the specified size for
     * reading compressed value data is sufficient.
     */
    @Override
    protected LogEntry makeNext() {
        if (innerDone()) {
            try {
                LogEntry entry = logStream.nextEntry();
                // No more record to return.
                if (entry == null)
                    return allDone();

                // decide whether to go shallow or deep iteration if it is compressed
                CompressionType compressionType = entry.record().compressionType();
                if (compressionType == CompressionType.NONE || shallow) {
                    return entry;
                } else {
                    // init the inner iterator with the value payload of the message,
                    // which will de-compress the payload to a set of messages;
                    // since we assume nested compression is not allowed, the deep iterator
                    // would not try to further decompress underlying messages
                    // There will be at least one element in the inner iterator, so we don't
                    // need to call hasNext() here.
                    innerIter = new DeepRecordsIterator(entry);
                    return innerIter.next();
                }
            } catch (EOFException e) {
                return allDone();
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        } else {
            return innerIter.next();
        }
    }

    private boolean innerDone() {
        return innerIter == null || !innerIter.hasNext();
    }

    private static class DataLogInputStream implements LogInputStream {
        private final DataInputStream stream;

        private DataLogInputStream(DataInputStream stream) {
            this.stream = stream;
        }

        public LogEntry nextEntry() throws IOException {
            long offset = stream.readLong();
            int size = stream.readInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);

            byte[] recordBuffer = new byte[size];
            stream.readFully(recordBuffer, 0, size);
            ByteBuffer buf = ByteBuffer.wrap(recordBuffer);
            return new LogEntry(offset, new Record(buf));
        }
    }

    private static class DeepRecordsIterator extends AbstractIterator<LogEntry> {
        private final ArrayDeque<LogEntry> logEntries;
        private final long absoluteBaseOffset;

        private DeepRecordsIterator(LogEntry entry) {
            CompressionType compressionType = entry.record().compressionType();
            ByteBuffer buffer = entry.record().value();
            DataInputStream stream = Compressor.wrapForInput(new ByteBufferInputStream(buffer), compressionType, entry.record().magic());
            LogInputStream logStream = new DataLogInputStream(stream);

            long wrapperRecordOffset = entry.offset();
            long wrapperRecordTimestamp = entry.record().timestamp();
            this.logEntries = new ArrayDeque<>();

            // If relative offset is used, we need to decompress the entire message first to compute
            // the absolute offset. For simplicity and because it's a format that is on its way out, we
            // do the same for message format version 0
            try {
                while (true) {
                    try {
                        LogEntry logEntry = logStream.nextEntry();
                        if (entry.record().magic() > Record.MAGIC_VALUE_V0) {
                            Record recordWithTimestamp = new Record(
                                    logEntry.record().buffer(),
                                    wrapperRecordTimestamp,
                                    entry.record().timestampType()
                            );
                            logEntry = new LogEntry(logEntry.offset(), recordWithTimestamp);
                        }
                        logEntries.add(logEntry);
                    } catch (EOFException e) {
                        break;
                    }
                }
                if (entry.record().magic() > Record.MAGIC_VALUE_V0)
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
                entry = new LogEntry(absoluteOffset, entry.record());
            }

            // decide whether to go shallow or deep iteration if it is compressed
            CompressionType compression = entry.record().compressionType();
            if (compression != CompressionType.NONE)
                throw new InvalidRecordException("Inner messages must not be compressed");

            return entry;
        }
    }

}
