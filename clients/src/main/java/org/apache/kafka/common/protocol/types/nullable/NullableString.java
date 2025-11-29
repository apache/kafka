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

import org.apache.kafka.common.protocol.types.StringType;

import java.nio.ByteBuffer;

public class NullableString extends StringType {
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            buffer.putShort((short) -1);
            return;
        }

        super.write(buffer, o);
    }

    @Override
    public String read(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0)
            return null;
        
        return super.read(buffer, length);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null)
            return 2;

        return super.sizeOf(o);
    }

    @Override
    public String typeName() {
        return "NULLABLE_STRING";
    }

    @Override
    public String validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String documentation() {
        return "Represents a sequence of characters or null. For non-null strings, first the length N is given as an " + INT16 +
            ". Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
            "A null value is encoded with length of -1 and there are no following bytes.";
    }
}
