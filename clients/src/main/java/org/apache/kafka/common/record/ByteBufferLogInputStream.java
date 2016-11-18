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

import org.apache.kafka.common.errors.CorruptRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<LogEntry.MutableLogEntry> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public LogEntry.MutableLogEntry nextEntry() throws IOException {
        int remaining = buffer.remaining();
        if (remaining < LOG_OVERHEAD)
            return null;

        int recordSize = buffer.getInt(buffer.position() + Records.SIZE_OFFSET);
        if (recordSize < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

        int entrySize = recordSize + LOG_OVERHEAD;
        if (remaining < entrySize)
            return null;

        byte magic = buffer.get(buffer.position() + LOG_OVERHEAD + Record.MAGIC_OFFSET);

        ByteBuffer entrySlice = buffer.slice();
        entrySlice.limit(entrySize);
        buffer.position(buffer.position() + entrySize);

        if (magic > Record.MAGIC_VALUE_V1)
            return new EosLogEntry(entrySlice);
        else
            return new OldLogEntry.ByteBufferLogEntry(entrySlice);
    }

}
