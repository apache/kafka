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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;

public interface Writable {
    void writeByte(byte val);
    void writeShort(short val);
    void writeInt(int val);
    void writeLong(long val);
    void writeDouble(double val);
    void writeByteArray(byte[] arr);
    void writeUnsignedVarint(int i);
    void writeByteBuffer(ByteBuffer buf);
    void writeVarint(int i);
    void writeVarlong(long i);

    default void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            MemoryRecords memRecords = (MemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else {
            throw new UnsupportedOperationException("Unsupported record type " + records.getClass());
        }
    }

    default void writeUuid(Uuid uuid) {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    default void writeUnsignedShort(int i) {
        // The setter functions in the generated code prevent us from setting
        // ints outside the valid range of a short.
        writeShort((short) i);
    }
}
