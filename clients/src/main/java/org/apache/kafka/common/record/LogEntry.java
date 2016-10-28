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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * An offset and record pair
 */
public abstract class LogEntry implements Iterable<LogEntry> {

    public abstract long offset();

    public abstract Record record();

    public long firstOffset() {
        return iterator().next().offset();
    }

    public long nextOffset() {
        return offset() + 1;
    }

    @Override
    public String toString() {
        return "LogEntry(" + offset() + ", " + record() + ")";
    }
    
    public int size() {
        return record().size() + LOG_OVERHEAD;
    }

    public boolean isCompressed() {
        return record().compressionType() != CompressionType.NONE;
    }

    public void writeTo(ByteBuffer buffer) {
        writeHeader(buffer, offset(), record().size());
        buffer.put(record().buffer().duplicate());
    }

    @Override
    public Iterator<LogEntry> iterator() {
        if (isCompressed())
            return new RecordsIterator.DeepRecordsIterator(this, false, Integer.MAX_VALUE);
        return Collections.singletonList(this).iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof LogEntry)) return false;

        LogEntry that = (LogEntry) o;

        if (offset() != that.offset()) return false;
        Record thisRecord = record();
        Record thatRecord = that.record();
        return thisRecord != null ? thisRecord.equals(thatRecord) : thatRecord == null;
    }

    @Override
    public int hashCode() {
        long offset = offset();
        Record record = record();
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (record != null ? record.hashCode() : 0);
        return result;
    }

    public static void writeHeader(ByteBuffer buffer, long offset, int size) {
        buffer.putLong(offset);
        buffer.putInt(size);
    }

    public static void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
        out.writeLong(offset);
        out.writeInt(size);
    }

    private static class SimpleLogEntry extends LogEntry {
        private final long offset;
        private final Record record;

        public SimpleLogEntry(long offset, Record record) {
            this.offset = offset;
            this.record = record;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public Record record() {
            return record;
        }

    }

    public static LogEntry create(long offset, Record record) {
        return new SimpleLogEntry(offset, record);
    }

}
