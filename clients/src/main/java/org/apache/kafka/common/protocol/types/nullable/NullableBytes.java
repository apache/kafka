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

import org.apache.kafka.common.protocol.types.Bytes;

import java.nio.ByteBuffer;

public class NullableBytes extends Bytes {
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            buffer.putInt(-1);
            return;
        }

        super.write(buffer, o);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int size = buffer.getInt();
        if (size < 0)
            return null;
        
        return read(buffer, size);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null)
            return 4;

        return super.sizeOf(o);
    }

    @Override
    public String typeName() {
        return "NULLABLE_BYTES";
    }

    @Override
    public ByteBuffer validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String documentation() {
        return "Represents a raw sequence of bytes or null. For non-null values, first the length N is given as an " + INT32 +
            ". Then N bytes follow. A null value is encoded with length of -1 and there are no following bytes.";
    }
}
