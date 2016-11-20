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
import org.apache.kafka.common.network.TransportLayer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

/**
 * File-backed record set.
 */
public class FileRecords implements Records {
    private final File file;
    private final FileChannel channel;
    private final long start;
    private final long end;
    private final long size;

    public FileRecords(File file,
                       FileChannel channel,
                       int start,
                       int end,
                       boolean isSlice) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;

        if (isSlice)
            this.size = end - start;
        else
            this.size = Math.min(channel.size(), end) - start;
    }

    @Override
    public int sizeInBytes() {
        return (int) size;
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        if (newSize < size)
            throw new KafkaException(String.format("Size of FileRecords %s has been truncated during write: old size %d, new size %d", file.getAbsolutePath(), size, newSize));

        if (offset > size)
            throw new KafkaException(String.format("The requested offset %d is out of range. The size of this FileRecords is %d.", offset, size));

        long position = start + offset;
        long count = Math.min(length, this.size - offset);
        if (destChannel instanceof TransportLayer) {
            TransportLayer tl = (TransportLayer) destChannel;
            return tl.transferFrom(this.channel, position, count);
        } else {
            return this.channel.transferTo(position, count, destChannel);
        }
    }

    @Override
    public RecordsIterator iterator() {
        return new RecordsIterator(new FileLogInputStream(channel, start, end), false);
    }

    private static class FileLogInputStream implements LogInputStream {
        private long position;
        protected final long end;
        protected final FileChannel channel;
        private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(Records.LOG_OVERHEAD);

        public FileLogInputStream(FileChannel channel, long start, long end) {
            this.channel = channel;
            this.position = start;
            this.end = end;
        }

        @Override
        public LogEntry nextEntry() throws IOException {
            if (position + Records.LOG_OVERHEAD >= end)
                return null;

            logHeaderBuffer.rewind();
            channel.read(logHeaderBuffer, position);
            if (logHeaderBuffer.hasRemaining())
                return null;

            logHeaderBuffer.rewind();
            long offset = logHeaderBuffer.getLong();
            int size = logHeaderBuffer.getInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);

            if (position + Records.LOG_OVERHEAD + size > end)
                return null;

            ByteBuffer recordBuffer = ByteBuffer.allocate(size);
            channel.read(recordBuffer, position + Records.LOG_OVERHEAD);
            if (recordBuffer.hasRemaining())
                return null;
            recordBuffer.rewind();

            Record record = new Record(recordBuffer);
            LogEntry logEntry = new LogEntry(offset, record);
            position += logEntry.size();
            return logEntry;
        }
    }
}
