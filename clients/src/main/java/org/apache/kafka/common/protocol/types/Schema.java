/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.protocol.types;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The schema for a compound record definition
 */
public class Schema extends Type {

    private final Field[] fields;
    private final Map<String, Field> fieldsByName;

    /**
     * Construct the schema with a given list of its field values
     *
     * @throws SchemaException If the given list have duplicate fields
     */
    public Schema(Field... fs) {
        this.fields = new Field[fs.length];
        this.fieldsByName = new HashMap<String, Field>();
        for (int i = 0; i < this.fields.length; i++) {
            Field field = fs[i];
            if (fieldsByName.containsKey(field.name))
                throw new SchemaException("Schema contains a duplicate field: " + field.name);
            this.fields[i] = new Field(i, field.name, field.type, field.doc, field.defaultValue, this);
            this.fieldsByName.put(fs[i].name, this.fields[i]);
        }
    }

    /**
     * Write a struct to the buffer
     */
    @Override
    public void write(ByteBuffer buffer, Object o) {
        Struct r = (Struct) o;
        for (int i = 0; i < fields.length; i++) {
            Field f = fields[i];
            try {
                Object value = f.type().validate(r.get(f));
                f.type.write(buffer, value);
            } catch (Exception e) {
                throw new SchemaException("Error writing field '" + f.name +
                                          "': " +
                                          (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
    }

    /**
     * Read a struct from the buffer
     */
    @Override
    public Struct read(ByteBuffer buffer) {
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            try {
                objects[i] = fields[i].type.read(buffer);
            } catch (Exception e) {
                throw new SchemaException("Error reading field '" + fields[i].name +
                                          "': " +
                                          (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
        return new Struct(this, objects);
    }

    /**
     * The size of the given record
     */
    @Override
    public int sizeOf(Object o) {
        int size = 0;
        Struct r = (Struct) o;
        for (int i = 0; i < fields.length; i++)
            size += fields[i].type.sizeOf(r.get(fields[i]));
        return size;
    }

    /**
     * The number of fields in this schema
     */
    public int numFields() {
        return this.fields.length;
    }

    /**
     * Get a field by its slot in the record array
     * 
     * @param slot The slot at which this field sits
     * @return The field
     */
    public Field get(int slot) {
        return this.fields[slot];
    }

    /**
     * Get a field by its name
     * 
     * @param name The name of the field
     * @return The field
     */
    public Field get(String name) {
        return this.fieldsByName.get(name);
    }

    /**
     * Get all the fields in this schema
     */
    public Field[] fields() {
        return this.fields;
    }

    /**
     * Display a string representation of the schema
     */
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (int i = 0; i < this.fields.length; i++) {
            b.append(this.fields[i].name);
            b.append(':');
            b.append(this.fields[i].type());
            if (i < this.fields.length - 1)
                b.append(',');
        }
        b.append("}");
        return b.toString();
    }

    @Override
    public Struct validate(Object item) {
        try {
            Struct struct = (Struct) item;
            for (int i = 0; i < this.fields.length; i++) {
                Field field = this.fields[i];
                try {
                    field.type.validate(struct.get(field));
                } catch (SchemaException e) {
                    throw new SchemaException("Invalid value for field '" + field.name + "': " + e.getMessage());
                }
            }
            return struct;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a Struct.");
        }
    }

}
