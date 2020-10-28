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

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

/**
 * Implementation of Readable which reads from a byte buffer and can read records as {@link MemoryRecords}
 *
 * @see org.apache.kafka.common.requests.FetchResponse
 */
public class RecordsReadable implements Readable {
    private final ByteBuffer buf;

    public RecordsReadable(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public byte readByte() {
        return buf.get();
    }

    @Override
    public short readShort() {
        return buf.getShort();
    }

    @Override
    public int readInt() {
        return buf.getInt();
    }

    @Override
    public long readLong() {
        return buf.getLong();
    }

    @Override
    public double readDouble() {
        return ByteUtils.readDouble(buf);
    }

    @Override
    public void readArray(byte[] arr) {
        buf.get(arr);
    }

    @Override
    public int readUnsignedVarint() {
        return ByteUtils.readUnsignedVarint(buf);
    }

    @Override
    public ByteBuffer readByteBuffer(int length) {
        ByteBuffer res = buf.slice();
        res.limit(length);

        buf.position(buf.position() + length);

        return res;
    }

    @Override
    public int readVarint() {
        return ByteUtils.readVarint(buf);
    }

    @Override
    public long readVarlong() {
        return ByteUtils.readVarlong(buf);
    }

    public BaseRecords readRecords(int length) {
        if (length < 0) {
            // no records
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return MemoryRecords.readableRecords(recordsBuffer);
        }
    }

}
