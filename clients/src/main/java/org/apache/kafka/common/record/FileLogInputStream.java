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
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A log input stream which is backed by a {@link FileChannel}.
 */
public class FileLogInputStream implements LogInputStream<FileLogInputStream.FileChannelLogEntry> {
    private int position;
    private final int end;
    private final FileChannel channel;
    private final int maxRecordSize;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(Records.LOG_OVERHEAD);

    /**
     * Create a new log input stream over the FileChannel
     * @param channel Underlying FileChannel
     * @param maxRecordSize Maximum size of records
     * @param start Position in the file channel to start from
     * @param end Position in the file channel not to read past
     */
    public FileLogInputStream(FileChannel channel,
                              int maxRecordSize,
                              int start,
                              int end) {
        this.channel = channel;
        this.maxRecordSize = maxRecordSize;
        this.position = start;
        this.end = end;
    }

    @Override
    public FileChannelLogEntry nextEntry() throws IOException {
        if (position + Records.LOG_OVERHEAD >= end)
            return null;

        logHeaderBuffer.rewind();
        Utils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong();
        int size = logHeaderBuffer.getInt();

        if (size < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is smaller than minimum record overhead (%d).", Record.RECORD_OVERHEAD_V0));

        if (size > maxRecordSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxRecordSize));

        if (position + Records.LOG_OVERHEAD + size > end)
            return null;

        FileChannelLogEntry logEntry = new FileChannelLogEntry(offset, channel, position, size);
        position += logEntry.sizeInBytes();
        return logEntry;
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the shallow log
     * entries without needing to read the record data into memory until it is needed. The downside
     * is that entries will generally no longer be readable when the underlying channel is closed.
     */
    public static class FileChannelLogEntry extends LogEntry {
        private final long offset;
        private final FileChannel channel;
        private final int position;
        private final int recordSize;
        private Record record = null;

        private FileChannelLogEntry(long offset,
                                   FileChannel channel,
                                   int position,
                                   int recordSize) {
            this.offset = offset;
            this.channel = channel;
            this.position = position;
            this.recordSize = recordSize;
        }

        @Override
        public long offset() {
            return offset;
        }

        public int position() {
            return position;
        }

        @Override
        public byte magic() {
            if (record != null)
                return record.magic();

            try {
                byte[] magic = new byte[1];
                ByteBuffer buf = ByteBuffer.wrap(magic);
                Utils.readFullyOrFail(channel, buf, position + Records.LOG_OVERHEAD + Record.MAGIC_OFFSET, "magic byte");
                return magic[0];
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }

        /**
         * Force load the record and its data (key and value) into memory.
         * @return The resulting record
         * @throws IOException for any IO errors reading from the underlying file
         */
        private Record loadRecord() throws IOException {
            if (record != null)
                return record;

            ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
            Utils.readFullyOrFail(channel, recordBuffer, position + Records.LOG_OVERHEAD, "full record");

            recordBuffer.rewind();
            record = new Record(recordBuffer);
            return record;
        }

        @Override
        public Record record() {
            if (record != null)
                return record;

            try {
                return loadRecord();
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public int sizeInBytes() {
            return Records.LOG_OVERHEAD + recordSize;
        }

    }
}
