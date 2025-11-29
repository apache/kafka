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
package org.apache.kafka.common.protocol.types.nullable;

import org.apache.kafka.common.protocol.types.CompactBytes;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class CompactNullableBytes extends CompactBytes {
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            ByteUtils.writeUnsignedVarint(0, buffer);
            return;
        }
        
        super.write(buffer, o);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int size = ByteUtils.readUnsignedVarint(buffer) - 1;
        if (size < 0)
            return null;
        
        return read(buffer, size);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null) {
            return 1;
        }
        
        return super.sizeOf(o);
    }

    @Override
    public String typeName() {
        return "COMPACT_NULLABLE_BYTES";
    }

    @Override
    public ByteBuffer validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String documentation() {
        return "Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT." +
            " Then N bytes follow. A null object is represented with a length of 0.";
    }
}
