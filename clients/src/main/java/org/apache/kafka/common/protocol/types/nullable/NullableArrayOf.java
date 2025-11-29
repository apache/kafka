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

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;

/**
 * Represents a type for a nullable array of a particular type
 */
public class NullableArrayOf extends ArrayOf {
    private static final String NULLABLE_ARRAY_TYPE_NAME = "NULLABLE_ARRAY";
    
    @Override
    public boolean isNullable() {
        return true;
    }
    
    public NullableArrayOf(Type type) {
        super(type);
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
    public String leftBracket() {
        return "?[";
    }

    @Override
    public String toString() {
        return NULLABLE_ARRAY_TYPE_NAME + "(" + type() + ")";
    }

    @Override
    public Object[] validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String typeName() {
        return NULLABLE_ARRAY_TYPE_NAME;
    }

    @Override
    public String documentation() {
        return "Represents a sequence of objects of a given type T. " +
            "Type T can be either a primitive type (e.g. " + STRING + ") or a structure. " +
            "First, the length N is given as an " + INT32 + ". Then N instances of type T follow. " +
            "A null array is represented with a length of -1. " +
            "In protocol documentation a nullable array of T instances is referred to as " +
            leftBracket() + "T" + rightBracket() + ".";
    }
}
