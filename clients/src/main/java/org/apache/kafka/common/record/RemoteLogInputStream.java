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
import org.apache.kafka.common.utils.DirectBufferPool;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

public class RemoteLogInputStream implements LogInputStream<RecordBatch>, Closeable {
    private final InputStream inputStream;
    private final DirectBufferPool bufferPool;
    // LogHeader buffer up to magic.
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);
    private ByteBuffer currentBuffer = null;

    public RemoteLogInputStream(InputStream inputStream, DirectBufferPool bufferPool) {
        this.inputStream = inputStream;
        this.bufferPool = bufferPool;
    }

    @Override
    public RecordBatch nextBatch() throws IOException {
        logHeaderBuffer.clear();
        Utils.readFully(inputStream, logHeaderBuffer);

        if (logHeaderBuffer.position() < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        logHeaderBuffer.rewind();
        int size = logHeaderBuffer.getInt(SIZE_OFFSET);

        // V0 has the smallest overhead, stricter checking is done later
        if (size < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Found record size %d smaller than minimum record " +
                                                                   "overhead (%d).", size, LegacyRecord.RECORD_OVERHEAD_V0));

        // Release previous buffer before allocating new one to avoid accumulation
        if (currentBuffer != null) {
            bufferPool.release(currentBuffer);
            currentBuffer = null;
        }

        // Total size is: "LOG_OVERHEAD + the size of the rest of the content"
        int bufferSize = LOG_OVERHEAD + size;
        // buffer contains the complete payload including header and records.
        ByteBuffer buffer = bufferPool.allocate(bufferSize);
        currentBuffer = buffer;

        // write log header into buffer
        buffer.put(logHeaderBuffer);

        // write the records payload into the buffer
        Utils.readFully(inputStream, buffer);
        if (buffer.position() != bufferSize)
            return null;
        buffer.rewind();

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        MutableRecordBatch batch;
        if (magic > RecordBatch.MAGIC_VALUE_V1)
            batch = new DefaultRecordBatch(buffer);
        else
            batch = new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(buffer);

        return batch;
    }

    @Override
    public void close() {
        if (currentBuffer != null) {
            bufferPool.release(currentBuffer);
            currentBuffer = null;
        }
    }
}
