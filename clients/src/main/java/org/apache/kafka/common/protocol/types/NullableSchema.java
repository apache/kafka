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

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The nullable schema for a compound record definition
 */
public final class NullableSchema extends Schema {

    public NullableSchema(Schema schema) {
        super(schema.tolerateMissingFieldsWithDefaults(), Arrays.stream(schema.fields()).map(field -> field.def).toArray(Field[]::new));
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    /**
     * Write a struct to the buffer with special handling for null values
     * If the input object is null, writes a byte value of -1 to the buffer as a null indicator.
     */
    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            buffer.put((byte) -1);
            return;
        }

        buffer.put((byte) 1);
        super.write(buffer, o);
    }

    @Override
    public Struct read(ByteBuffer buffer) {
        byte nullIndicator = buffer.get();
        if (nullIndicator < 0)
            return null;

        return super.read(buffer);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null)
            return 1;

        return 1 + super.sizeOf(o);
    }

    @Override
    public Struct validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String typeName() {
        return "NULLABLE_STRUCT";
    }

    @Override
    public String leftBracket() {
        return "?{";
    }

    @Override
    public String rightBracket() {
        return "}";
    }
    
    @Override
    public String documentation() {
        return "A nullable struct is named by a string with a capitalized first letter and consists of one or more fields. " +
            "It represents a composite object or null. " +
            "For non-null values, the first byte has value 1, " +
            "followed by the serialization of each field in the order they are defined. " +
            "A null value is encoded as a byte with value -1 and there are no following bytes." +
            "In protocol documentation a nullable struct containing multiple fields is enclosed by " + 
            leftBracket() + " and " + rightBracket() + ".";
    }
}
