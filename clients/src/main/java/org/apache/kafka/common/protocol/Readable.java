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
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    double readDouble();
    byte[] readArray(int length);
    int readUnsignedVarint();
    ByteBuffer readByteBuffer(int length);
    int readVarint();
    long readVarlong();
    int remaining();

    default String readString(int length) {
        byte[] arr = readArray(length);
        return new String(arr, StandardCharsets.UTF_8);
    }

    default List<RawTaggedField> readUnknownTaggedField(List<RawTaggedField> unknowns, int tag, int size) {
        if (unknowns == null) {
            unknowns = new ArrayList<>();
        }
        byte[] data = readArray(size);
        unknowns.add(new RawTaggedField(tag, data));
        return unknowns;
    }

    default MemoryRecords readRecords(int length) {
        if (length < 0) {
            // no records
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return MemoryRecords.readableRecords(recordsBuffer);
        }
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    default Uuid readUuid() {
        return new Uuid(readLong(), readLong());
    }

    default int readUnsignedShort() {
        return Short.toUnsignedInt(readShort());
    }

    default long readUnsignedInt() {
        return Integer.toUnsignedLong(readInt());
    }
}
