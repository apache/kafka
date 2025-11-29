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

import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

/**
 * Represents a type for a compact nullable array of a particular type.
 */
public class CompactNullableArrayOf extends CompactArrayOf {
    private static final String COMPACT_NULLABLE_ARRAY_TYPE_NAME = "COMPACT_NULLABLE_ARRAY";

    public CompactNullableArrayOf(Type type) {
        super(type);
    }

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
        int n = ByteUtils.readUnsignedVarint(buffer);
        if (n == 0) 
            return null;
        
        return read(buffer, n - 1);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null) {
            return 1;
        }
        
        return super.sizeOf(o);
    }
    
    @Override
    public String leftBracket() {
        return "?(";
    }

    @Override
    public String toString() {
        return COMPACT_NULLABLE_ARRAY_TYPE_NAME + "(" + type() + ")";
    }

    @Override
    public Object[] validate(Object item) {
        if (item == null) 
            return null;

        return super.validate(item);
    }

    @Override
    public String typeName() {
        return COMPACT_NULLABLE_ARRAY_TYPE_NAME;
    }

    @Override
    public String documentation() {
        return "Represents a sequence of objects of a given type T. " +
            "Type T can be either a primitive type (e.g. " + STRING + ") or a structure. " +
            "First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. " +
            "A null array is represented with a length of 0. " +
            "In protocol documentation a compact nullable array of T instances is referred to as " +
            leftBracket() + "T" + rightBracket() + ".";
    }
}
