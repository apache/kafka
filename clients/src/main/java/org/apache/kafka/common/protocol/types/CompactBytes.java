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
package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.protocol.types.Type.DocumentedType;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class CompactBytes extends DocumentedType {
    @Override
    public void write(ByteBuffer buffer, Object o) {
        ByteBuffer arg = (ByteBuffer) o;
        int pos = arg.position();
        ByteUtils.writeUnsignedVarint(arg.remaining() + 1, buffer);
        buffer.put(arg);
        arg.position(pos);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int size = ByteUtils.readUnsignedVarint(buffer) - 1;
        if (size < 0)
            throw new SchemaException("Bytes size " + size + " cannot be negative");
        
        return read(buffer, size);
    }

    protected ByteBuffer read(ByteBuffer buffer, int size) {
        if (size > buffer.remaining())
            throw new SchemaException("Error reading bytes of size " + size + ", only " + buffer.remaining() + 
                " bytes available");

        int limit = buffer.limit();
        int newPosition = buffer.position() + size;
        buffer.limit(newPosition);
        ByteBuffer val = buffer.slice();
        buffer.limit(limit);
        buffer.position(newPosition);
        return val;
    }

    @Override
    public int sizeOf(Object o) {
        ByteBuffer buffer = (ByteBuffer) o;
        int remaining = buffer.remaining();
        return ByteUtils.sizeOfUnsignedVarint(remaining + 1) + remaining;
    }

    @Override
    public String typeName() {
        return "COMPACT_BYTES";
    }

    @Override
    public ByteBuffer validate(Object item) {
        if (item instanceof ByteBuffer)
            return (ByteBuffer) item;
        else
            throw new SchemaException(item + " is not a java.nio.ByteBuffer.");
    }

    @Override
    public String documentation() {
        return "Represents a raw sequence of bytes. First the length N+1 is given as an UNSIGNED_VARINT." +
            " Then N bytes follow.";
    }
}
