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
import java.util.Optional;

/**
 * Represents a type for a compact array of a particular type.
 * A compact array represents its length with a varint rather than a
 * fixed-length field.
 */
public class CompactArrayOf extends DocumentedType {
    private static final String COMPACT_ARRAY_TYPE_NAME = "COMPACT_ARRAY";

    private final Type type;
    private final boolean nullable;


    public CompactArrayOf(Type type) {
        this(type, false);
    }

    public static CompactArrayOf nullable(Type type) {
        return new CompactArrayOf(type, true);
    }

    private CompactArrayOf(Type type, boolean nullable) {
        this.type = type;
        this.nullable = nullable;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            ByteUtils.writeUnsignedVarint(0, buffer);
            return;
        }
        Object[] objs = (Object[]) o;
        int size = objs.length;
        ByteUtils.writeUnsignedVarint(size + 1, buffer);

        for (Object obj : objs)
            type.write(buffer, obj);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int n = ByteUtils.readUnsignedVarint(buffer);
        if (n == 0) {
            if (isNullable()) {
                return null;
            } else {
                throw new SchemaException("This array is not nullable.");
            }
        }
        int size = n - 1;
        if (size > buffer.remaining())
            throw new SchemaException("Error reading array of size " + size + ", only " + buffer.remaining() + " bytes available");
        Object[] objs = new Object[size];
        for (int i = 0; i < size; i++)
            objs[i] = type.read(buffer);
        return objs;
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null) {
            return 1;
        }
        Object[] objs = (Object[]) o;
        int size = ByteUtils.sizeOfUnsignedVarint(objs.length + 1);
        for (Object obj : objs) {
            size += type.sizeOf(obj);
        }
        return size;
    }

    @Override
    public Optional<Type> arrayElementType() {
        return Optional.of(type);
    }

    @Override
    public String toString() {
        return COMPACT_ARRAY_TYPE_NAME + "(" + type + ")";
    }

    @Override
    public Object[] validate(Object item) {
        try {
            if (isNullable() && item == null)
                return null;

            Object[] array = (Object[]) item;
            for (Object obj : array)
                type.validate(obj);
            return array;
        } catch (ClassCastException e) {
            throw new SchemaException("Not an Object[]. Found class " + item.getClass().getSimpleName());
        }
    }

    @Override
    public String typeName() {
        return COMPACT_ARRAY_TYPE_NAME;
    }

    @Override
    public String documentation() {
        return "Represents a sequence of objects of a given type T. " +
                "Type T can be either a primitive type (e.g. " + STRING + ") or a structure. " +
                "First, the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T follow. " +
                "A null array is represented with a length of 0. " +
                "In protocol documentation an array of T instances is referred to as [T].";
    }
}
