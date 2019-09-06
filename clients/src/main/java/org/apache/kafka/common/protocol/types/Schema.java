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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The schema for a compound record definition
 */
public class Schema extends Type {

    private final BoundField[] fields;
    private final Map<String, BoundField> fieldsByName;
    private final boolean tolerateMissingFieldsWithDefaults;

    /**
     * Construct the schema with a given list of its field values
     *
     * @param fs the fields of this schema
     *
     * @throws SchemaException If the given list have duplicate fields
     */
    public Schema(Field... fs) {
        this(false, fs);
    }

    /**
     * Construct the schema with a given list of its field values and the ability to tolerate
     * missing optional fields with defaults at the end of the schema definition.
     *
     * @param tolerateMissingFieldsWithDefaults whether to accept records with missing optional
     * fields the end of the schema
     * @param fs the fields of this schema
     *
     * @throws SchemaException If the given list have duplicate fields
     */
    public Schema(boolean tolerateMissingFieldsWithDefaults, Field... fs) {
        this.fields = new BoundField[fs.length];
        this.fieldsByName = new HashMap<>();
        this.tolerateMissingFieldsWithDefaults = tolerateMissingFieldsWithDefaults;
        for (int i = 0; i < this.fields.length; i++) {
            Field def = fs[i];
            if (fieldsByName.containsKey(def.name))
                throw new SchemaException("Schema contains a duplicate field: " + def.name);
            this.fields[i] = new BoundField(def, this, i);
            this.fieldsByName.put(def.name, this.fields[i]);
        }
    }

    /**
     * Write a struct to the buffer
     */
    @Override
    public void write(ByteBuffer buffer, Object o) {
        Struct r = (Struct) o;
        for (BoundField field : fields) {
            try {
                Object value = field.def.type.validate(r.get(field));
                field.def.type.write(buffer, value);
            } catch (Exception e) {
                throw new SchemaException("Error writing field '" + field.def.name + "': " +
                                          (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
    }

    /**
     * Read a struct from the buffer. If this schema is configured to tolerate missing
     * optional fields at the end of the buffer, these fields are replaced with their default
     * values; otherwise, if the schema does not tolerate missing fields, or if missing fields
     * don't have a default value, a {@code SchemaException} is thrown to signify that mandatory
     * fields are missing.
     */
    @Override
    public Struct read(ByteBuffer buffer) {
        Object[] objects = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            try {
                if (tolerateMissingFieldsWithDefaults) {
                    if (buffer.hasRemaining()) {
                        objects[i] = fields[i].def.type.read(buffer);
                    } else if (fields[i].def.hasDefaultValue) {
                        objects[i] = fields[i].def.defaultValue;
                    } else {
                        throw new SchemaException("Missing value for field '" + fields[i].def.name +
                                "' which has no default value.");
                    }
                } else {
                    objects[i] = fields[i].def.type.read(buffer);
                }
            } catch (Exception e) {
                throw new SchemaException("Error reading field '" + fields[i].def.name + "': " +
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
        for (BoundField field : fields) {
            try {
                size += field.def.type.sizeOf(r.get(field));
            } catch (Exception e) {
                throw new SchemaException("Error computing size for field '" + field.def.name + "': " +
                        (e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
            }
        }
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
    public BoundField get(int slot) {
        return this.fields[slot];
    }

    /**
     * Get a field by its name
     * 
     * @param name The name of the field
     * @return The field
     */
    public BoundField get(String name) {
        return this.fieldsByName.get(name);
    }

    /**
     * Get all the fields in this schema
     */
    public BoundField[] fields() {
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
            b.append(this.fields[i].toString());
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
            for (BoundField field : fields) {
                try {
                    field.def.type.validate(struct.get(field));
                } catch (SchemaException e) {
                    throw new SchemaException("Invalid value for field '" + field.def.name + "': " + e.getMessage());
                }
            }
            return struct;
        } catch (ClassCastException e) {
            throw new SchemaException("Not a Struct.");
        }
    }

    public void walk(Visitor visitor) {
        Objects.requireNonNull(visitor, "visitor must be non-null");
        handleNode(this, visitor);
    }

    private static void handleNode(Type node, Visitor visitor) {
        if (node instanceof Schema) {
            Schema schema = (Schema) node;
            visitor.visit(schema);
            for (BoundField f : schema.fields())
                handleNode(f.def.type, visitor);
        } else if (node instanceof ArrayOf) {
            ArrayOf array = (ArrayOf) node;
            visitor.visit(array);
            handleNode(array.type(), visitor);
        } else {
            visitor.visit(node);
        }
    }

    /**
     * Override one or more of the visit methods with the desired logic.
     */
    public static abstract class Visitor {
        public void visit(Schema schema) {}
        public void visit(ArrayOf array) {}
        public void visit(Type field) {}
    }
}
