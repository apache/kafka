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
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Represents a tagged fields section.
 */
public class TaggedFields extends DocumentedType {
    private static final String TAGGED_FIELDS_TYPE_NAME = "TAGGED_FIELDS";

    private final Map<Integer, Field> fields;

    /**
     * Create a new TaggedFields object with the given tags and fields.
     *
     * @param fields    This is an array containing Integer tags followed
     *                  by associated Field objects.
     * @return          The new {@link TaggedFields}
     */
    public static TaggedFields of(Object... fields) {
        if (fields.length % 2 != 0) {
            throw new RuntimeException("TaggedFields#of takes an even " +
                "number of parameters.");
        }
        TreeMap<Integer, Field> newFields = new TreeMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            Integer tag = (Integer) fields[i];
            Field field = (Field) fields[i + 1];
            newFields.put(tag, field);
        }
        return new TaggedFields(newFields);
    }

    public TaggedFields(Map<Integer, Field> fields) {
        this.fields = fields;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(ByteBuffer buffer, Object o) {
        NavigableMap<Integer, Object> objects = (NavigableMap<Integer, Object>) o;
        ByteUtils.writeUnsignedVarint(objects.size(), buffer);
        for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
            Integer tag = entry.getKey();
            Field field = fields.get(tag);
            ByteUtils.writeUnsignedVarint(tag, buffer);
            if (field == null) {
                RawTaggedField value = (RawTaggedField) entry.getValue();
                ByteUtils.writeUnsignedVarint(value.data().length, buffer);
                buffer.put(value.data());
            } else {
                ByteUtils.writeUnsignedVarint(field.type.sizeOf(entry.getValue()), buffer);
                field.type.write(buffer, entry.getValue());
            }
        }
    }

    @Override
    public NavigableMap<Integer, Object> read(ByteBuffer buffer) {
        int numTaggedFields = ByteUtils.readUnsignedVarint(buffer);
        if (numTaggedFields == 0) {
            return Collections.emptyNavigableMap();
        }
        NavigableMap<Integer, Object> objects = new TreeMap<>();
        int prevTag = -1;
        for (int i = 0; i < numTaggedFields; i++) {
            int tag = ByteUtils.readUnsignedVarint(buffer);
            if (tag <= prevTag) {
                throw new RuntimeException("Invalid or out-of-order tag " + tag);
            }
            prevTag = tag;
            int size = ByteUtils.readUnsignedVarint(buffer);
            if (size < 0)
                throw new SchemaException("field size " + size + " cannot be negative");
            if (size > buffer.remaining())
                throw new SchemaException("Error reading field of size " + size + ", only " + buffer.remaining() + " bytes available");

            Field field = fields.get(tag);
            if (field == null) {
                byte[] bytes = new byte[size];
                buffer.get(bytes);
                objects.put(tag, new RawTaggedField(tag, bytes));
            } else {
                objects.put(tag, field.type.read(buffer));
            }
        }
        return objects;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int sizeOf(Object o) {
        int size = 0;
        NavigableMap<Integer, Object> objects = (NavigableMap<Integer, Object>) o;
        size += ByteUtils.sizeOfUnsignedVarint(objects.size());
        for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
            Integer tag = entry.getKey();
            size += ByteUtils.sizeOfUnsignedVarint(tag);
            Field field = fields.get(tag);
            if (field == null) {
                RawTaggedField value = (RawTaggedField) entry.getValue();
                size += value.data().length + ByteUtils.sizeOfUnsignedVarint(value.data().length);
            } else {
                int valueSize = field.type.sizeOf(entry.getValue());
                size += valueSize + ByteUtils.sizeOfUnsignedVarint(valueSize);
            }
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder("TAGGED_FIELDS_TYPE_NAME(");
        String prefix = "";
        for (Map.Entry<Integer, Field> field : fields.entrySet()) {
            bld.append(prefix);
            prefix = ", ";
            bld.append(field.getKey()).append(" -> ").append(field.getValue().toString());
        }
        bld.append(")");
        return bld.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, Object> validate(Object item) {
        try {
            NavigableMap<Integer, Object> objects = (NavigableMap<Integer, Object>) item;
            for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
                Integer tag = entry.getKey();
                Field field = fields.get(tag);
                if (field == null) {
                    if (!(entry.getValue() instanceof RawTaggedField)) {
                        throw new SchemaException("The value associated with tag " + tag +
                            " must be a RawTaggedField in this version of the software.");
                    }
                } else {
                    field.type.validate(entry.getValue());
                }
            }
            return objects;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a NavigableMap. Found class " + item.getClass().getSimpleName());
        }
    }

    @Override
    public String typeName() {
        return TAGGED_FIELDS_TYPE_NAME;
    }

    @Override
    public String documentation() {
        return "Represents a series of tagged fields.";
    }

    /**
     * The number of tagged fields
     */
    public int numFields() {
        return this.fields.size();
    }
}
