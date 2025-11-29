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

import org.apache.kafka.common.protocol.types.CompactString;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

public class CompactNullableString extends CompactString {
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
    public String read(ByteBuffer buffer) {
        int length = ByteUtils.readUnsignedVarint(buffer) - 1;
        if (length < 0)
            return null;
        return read(buffer, length);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null) {
            return 1;
        }
        int length = Utils.utf8Length((String) o);
        return ByteUtils.sizeOfUnsignedVarint(length + 1) + length;
    }

    @Override
    public String typeName() {
        return "COMPACT_NULLABLE_STRING";
    }

    @Override
    public String validate(Object item) {
        if (item == null) {
            return null;
        } else if (item instanceof String) {
            return (String) item;
        } else {
            throw new SchemaException(item + " is not a String.");
        }
    }

    @Override
    public String documentation() {
        return "Represents a sequence of characters. First the length N + 1 is given as an UNSIGNED_VARINT " +
            ". Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
            "A null string is represented with a length of 0.";
    }
}
