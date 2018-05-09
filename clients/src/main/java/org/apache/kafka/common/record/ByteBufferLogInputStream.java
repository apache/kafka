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
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<MutableRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public MutableRecordBatch nextBatch() throws IOException {
        int remaining = buffer.remaining();
        if (remaining < LOG_OVERHEAD)
            return null;

        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        // V0 has the smallest overhead, stricter checking is done later
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size %d is less than the minimum record overhead (%d)",
                    recordSize, LegacyRecord.RECORD_OVERHEAD_V0));
        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size %d exceeds the largest allowable message size (%d).",
                    recordSize, maxMessageSize));

        int batchSize = recordSize + LOG_OVERHEAD;
        if (remaining < batchSize)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);

        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        buffer.position(buffer.position() + batchSize);

        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw new CorruptRecordException("Invalid magic found in record: " + magic);

        if (magic > RecordBatch.MAGIC_VALUE_V1)
            return new DefaultRecordBatch(batchSlice);
        else
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);
    }

}
