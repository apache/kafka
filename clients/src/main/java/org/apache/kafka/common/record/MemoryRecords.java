/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Iterator;

import org.apache.kafka.common.utils.AbstractIterator;


/**
 * A {@link Records} implementation backed by a ByteBuffer.
 */
public class MemoryRecords implements Records {

    private final ByteBuffer buffer;

    public MemoryRecords(int size) {
        this(ByteBuffer.allocate(size));
    }

    public MemoryRecords(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Append the given record and offset to the buffer
     */
    public void append(long offset, Record record) {
        buffer.putLong(offset);
        buffer.putInt(record.size());
        buffer.put(record.buffer());
        record.buffer().rewind();
    }

    /**
     * Append a new record and offset to the buffer
     */
    public void append(long offset, byte[] key, byte[] value, CompressionType type) {
        buffer.putLong(offset);
        buffer.putInt(Record.recordSize(key, value));
        Record.write(this.buffer, key, value, type);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     */
    public boolean hasRoomFor(byte[] key, byte[] value) {
        return this.buffer.remaining() >= Records.LOG_OVERHEAD + Record.recordSize(key, value);
    }

    /** Write the records in this set to the given channel */
    public int writeTo(GatheringByteChannel channel) throws IOException {
        return channel.write(buffer);
    }

    /**
     * The size of this record set
     */
    public int sizeInBytes() {
        return this.buffer.position();
    }

    /**
     * Get the byte buffer that backs this records instance
     */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return new RecordsIterator(this.buffer);
    }

    /* TODO: allow reuse of the buffer used for iteration */
    public static class RecordsIterator extends AbstractIterator<LogEntry> {
        private final ByteBuffer buffer;

        public RecordsIterator(ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            copy.flip();
            this.buffer = copy;
        }

        @Override
        protected LogEntry makeNext() {
            if (buffer.remaining() < Records.LOG_OVERHEAD)
                return allDone();
            long offset = buffer.getLong();
            int size = buffer.getInt();
            if (size < 0)
                throw new IllegalStateException("Record with size " + size);
            if (buffer.remaining() < size)
                return allDone();
            ByteBuffer rec = buffer.slice();
            rec.limit(size);
            this.buffer.position(this.buffer.position() + size);
            return new LogEntry(offset, new Record(rec));
        }
    }

}
