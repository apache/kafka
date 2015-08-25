/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.errors.DataException;

import java.nio.ByteBuffer;
import java.util.*;

public class CopycatSchema implements Schema {
    private static final Map<Type, Class<?>> SCHEMA_TYPE_CLASSES = new HashMap<>();
    static {
        SCHEMA_TYPE_CLASSES.put(Type.INT8, Byte.class);
        SCHEMA_TYPE_CLASSES.put(Type.INT16, Short.class);
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Integer.class);
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Long.class);
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Float.class);
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Double.class);
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Boolean.class);
        SCHEMA_TYPE_CLASSES.put(Type.STRING, String.class);
        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, List.class);
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Map.class);
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Struct.class);
        // Bytes are handled as a special case
    }

    // The type of the field
    private final Type type;
    private final boolean optional;
    private final Object defaultValue;

    private final List<Field> fields;
    private final Map<String, Field> fieldsByName;

    private final Schema keySchema;
    private final Schema valueSchema;

    // Optional name and version provide a built-in way to indicate what type of data is included. Most
    // useful for structs to indicate the semantics of the struct and map it to some existing underlying
    // serializer-specific schema. However, can also be useful in specifying other logical types (e.g. a set is an array
    // with additional constraints).
    private final String name;
    private final byte[] version;
    // Optional human readable documentation describing this schema.
    private final String doc;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public CopycatSchema(Type type, boolean optional, Object defaultValue, String name, byte[] version, String doc, List<Field> fields, Schema keySchema, Schema valueSchema) {
        this.type = type;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;

        this.fields = fields;
        if (this.fields != null && this.type == Type.STRUCT) {
            this.fieldsByName = new HashMap<>();
            for (Field field : fields)
                fieldsByName.put(field.name(), field);
        } else {
            this.fieldsByName = null;
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public byte[] version() {
        return version;
    }

    @Override
    public String doc() {
        return doc;
    }



    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fieldsByName.get(fieldName);
    }

    @Override
    public Schema keySchema() {
        if (type != Type.MAP)
            throw new DataException("Cannot look up key schema on non-map type");
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        if (type != Type.MAP && type != Type.ARRAY)
            throw new DataException("Cannot look up value schema on non-array and non-map type");
        return valueSchema;
    }



    /**
     * Validate that the value can be used with the schema, i.e. that it's type matches the schema type and nullability
     * requirements. Throws a DataException if the value is invalid. Returns
     * @param schema Schema to test
     * @param value value to test
     */
    public static void validateValue(Schema schema, Object value) {
        if (value == null) {
            if (!schema.isOptional())
                throw new DataException("Invalid value: null used for required field");
            else
                return;
        }

        // Special case for bytes. byte[] causes problems because it doesn't handle equals()/hashCode() like we want
        // objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause those methods to fail, so
        // ByteBuffers are recommended
        if (schema.type() == Type.BYTES && (value instanceof byte[] || value instanceof ByteBuffer))
            return;
        Class<?> expectedClass = SCHEMA_TYPE_CLASSES.get(schema.type());
        if (expectedClass == null || !expectedClass.isInstance(value))
                throw new DataException("Invalid value: expected " + expectedClass + " for type " + schema.type() + " but tried to use " + value.getClass());

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    throw new DataException("Struct schemas do not match.");
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                for (Object entry : array)
                    validateValue(schema.valueSchema(), entry);
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.keySchema(), entry.getKey());
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    /**
     * Validate that the value can be used for this schema, i.e. that it's type matches the schema type and optional
     * requirements. Throws a DataException if the value is invalid.
     * @param value the value to validate
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }

    @Override
    public CopycatSchema schema() {
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CopycatSchema schema = (CopycatSchema) o;
        return Objects.equals(optional, schema.optional) &&
                Objects.equals(type, schema.type) &&
                Objects.equals(defaultValue, schema.defaultValue) &&
                Objects.equals(fields, schema.fields) &&
                Objects.equals(keySchema, schema.keySchema) &&
                Objects.equals(valueSchema, schema.valueSchema) &&
                Objects.equals(name, schema.name) &&
                Arrays.equals(version, schema.version) &&
                Objects.equals(doc, schema.doc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, optional, defaultValue, fields, keySchema, valueSchema, name, version, doc);
    }

    @Override
    public String toString() {
        if (name != null)
            return "Schema{" + name + ":" + type + "}";
        else
            return "Schema{" + type + "}";
    }
}
