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

import org.apache.kafka.common.protocol.types.RawTaggedField;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    void readArray(byte[] arr);
    int readUnsignedVarint();

    default String readString(int length) {
        byte[] arr = new byte[length];
        readArray(arr);
        return new String(arr, StandardCharsets.UTF_8);
    }

    default List<RawTaggedField> readUnknownTaggedField(List<RawTaggedField> unknowns, int tag, int size) {
        if (unknowns == null) {
            unknowns = new ArrayList<>();
        }
        byte[] data = new byte[size];
        readArray(data);
        unknowns.add(new RawTaggedField(tag, data));
        return unknowns;
    }

    /**
     * Read a UUID with the most significant digits first.
     */
    default UUID readUUID() {
        return new UUID(readLong(), readLong());
    }
}
