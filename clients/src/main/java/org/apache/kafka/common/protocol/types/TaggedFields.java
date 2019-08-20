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
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Represents a tagged fields section.
 */
public class TaggedFields extends DocumentedType {
    private static final String TAGGED_FIELDS_TYPE_NAME = "TAGGED_FIELDS";

    private final NavigableMap<Integer, Type> fields;

    public TaggedFields(NavigableMap<Integer, Type> fields) {
        this.fields = fields;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(ByteBuffer buffer, Object o) {
        Map<Integer, Object> objects = (Map<Integer, Object>) o;
        ByteUtils.writeUnsignedVarint(fields.size(), buffer);
        for (Map.Entry<Integer, Type> entry : fields.entrySet()) {
            Integer key = entry.getKey();
            if (objects.containsKey(key)) {
                Object value = objects.get(key); // note: this may be null.
                ByteUtils.writeUnsignedVarint(entry.getKey(), buffer);
                Type type = entry.getValue();
                ByteUtils.writeUnsignedVarint(type.sizeOf(value), buffer);
                type.write(buffer, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Integer, Object> read(ByteBuffer buffer) {
        int numTaggedFields = ByteUtils.readUnsignedVarint(buffer);
        Map<Integer, Object> objects = null;
        for (int i = 0; i < numTaggedFields; i++) {
            int tag = ByteUtils.readUnsignedVarint(buffer);
            int size = ByteUtils.readUnsignedVarint(buffer);
            Type fieldType = fields.get(tag);
            if (fieldType != null) {
                if (objects == null) {
                    objects = new HashMap<>();
                }
                objects.put(tag, fieldType.read(buffer));
            } else {
                buffer.position(buffer.position() + size);
            }
        }
        return objects == null ? Collections.<Integer, Object>emptyMap() : objects;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int sizeOf(Object o) {
        int size = 0;
        Map<Integer, Object> objects = (Map<Integer, Object>) o;
        size += ByteUtils.sizeOfUnsignedVarint(fields.size());
        for (Map.Entry<Integer, Type> entry : fields.entrySet()) {
            Integer key = entry.getKey();
            if (objects.containsKey(key)) {
                Object value = objects.get(key); // note: this may be null.
                size += ByteUtils.sizeOfUnsignedVarint(entry.getKey());
                Type type = entry.getValue();
                size += ByteUtils.sizeOfUnsignedVarint(type.sizeOf(value));
            }
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder("TAGGED_FIELDS_TYPE_NAME(");
        String prefix = "";
        for (Map.Entry<Integer, Type> field : fields.entrySet()) {
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
            Map<Integer, Object> objects = (Map<Integer, Object>) item;
            for (Map.Entry<Integer, Object> entry : objects.entrySet()) {
                Integer key = entry.getKey();
                Type type = fields.get(key);
                if (type != null) {
                    type.validate(entry.getValue());
                }
            }
            return objects;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a Map.");
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
}
