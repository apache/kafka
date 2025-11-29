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
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

public class StringType extends DocumentedType {

    @Override
    public void write(ByteBuffer buffer, Object o) {
        byte[] bytes = Utils.utf8((String) o);
        if (bytes.length > Short.MAX_VALUE)
            throw new SchemaException("String length " + bytes.length + " is larger than the maximum string length.");
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    @Override
    public String read(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0)
            throw new SchemaException("String length " + length + " cannot be negative");
        
        return read(buffer, length);
    }
    
    protected String read(ByteBuffer buffer, short length) {
        if (length > buffer.remaining())
            throw new SchemaException("Error reading string of length " + length + ", only " + buffer.remaining() + " bytes available");
        String result = Utils.utf8(buffer, length);
        buffer.position(buffer.position() + length);
        return result;
    }

    @Override
    public int sizeOf(Object o) {
        return 2 + Utils.utf8Length((String) o);
    }

    @Override
    public String typeName() {
        return "STRING";
    }

    @Override
    public String validate(Object item) {
        if (item instanceof String)
            return (String) item;
        else
            throw new SchemaException(item + " is not a String.");
    }

    @Override
    public String documentation() {
        return "Represents a sequence of characters. First the length N is given as an " + INT16 +
            ". Then N bytes follow which are the UTF-8 encoding of the character sequence. " +
            "Length must not be negative.";
    }
}
