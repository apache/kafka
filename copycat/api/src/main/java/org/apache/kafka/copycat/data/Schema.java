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

/**
 * <p>
 *     Definition of an abstract data type. Data types can be primitive types (integer types, floating point types,
 *     boolean, strings, and bytes) or complex types (typed arrays, maps with one key schema and value schema,
 *     and structs that have a fixed set of field names each with an associated value schema). Any type can be specified
 *     as optional, allowing it to be omitted (resulting in null values when it is missing) and can specify a default
 *     value.
 * </p>
 * <p>
 *     All schemas may have some associated metadata: a name, version, and documentation. These are all considered part
 *     of the schema itself and included when comparing schemas. Besides adding important metadata, these fields enable
 *     the specification of logical types that specify additional constraints and semantics (e.g. UNIX timestamps are
 *     just an int64, but the user needs the know about the additional semantics to interpret it properly).
 * </p>
 * <p>
 *     Schemas can be created directly, but in most cases using {@link SchemaBuilder} will be simpler.
 * </p>
 */
public class Schema implements ISchema {
    /**
     * The type of a schema. These only include the core types; logical types must be determined by checking the schema name.
     */
    public enum Type {
        INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING, BYTES, ARRAY, MAP, STRUCT;

        private String name;

        Type() {
            this.name = this.name().toLowerCase();
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            switch (this) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    return true;
            }
            return false;
        }
    }

    private static final Map<Schema.Type, Class<?>> SCHEMA_TYPE_CLASSES = new HashMap<>();
    static {
        SCHEMA_TYPE_CLASSES.put(Schema.Type.INT8, Byte.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.INT16, Short.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.INT32, Integer.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.INT64, Long.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.FLOAT32, Float.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.FLOAT64, Double.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.BOOLEAN, Boolean.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.STRING, String.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.ARRAY, List.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.MAP, Map.class);
        SCHEMA_TYPE_CLASSES.put(Schema.Type.STRUCT, Struct.class);
        // Bytes are handled as a special case
    }

    public static final Schema INT8_SCHEMA = SchemaBuilder.int8().build();
    public static final Schema INT16_SCHEMA = SchemaBuilder.int16().build();
    public static final Schema INT32_SCHEMA = SchemaBuilder.int32().build();
    public static final Schema INT64_SCHEMA = SchemaBuilder.int64().build();
    public static final Schema FLOAT32_SCHEMA = SchemaBuilder.float32().build();
    public static final Schema FLOAT64_SCHEMA = SchemaBuilder.float64().build();
    public static final Schema BOOLEAN_SCHEMA = SchemaBuilder.bool().build();
    public static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
    public static final Schema BYTES_SCHEMA = SchemaBuilder.bytes().build();

    public static final Schema OPTIONAL_INT8_SCHEMA = SchemaBuilder.int8().optional().build();
    public static final Schema OPTIONAL_INT16_SCHEMA = SchemaBuilder.int16().optional().build();
    public static final Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32().optional().build();
    public static final Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64().optional().build();
    public static final Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32().optional().build();
    public static final Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64().optional().build();
    public static final Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
    public static final Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();
    public static final Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes().optional().build();

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
    public Schema(Type type, boolean optional, Object defaultValue, String name, byte[] version, String doc, List<Field> fields, Schema keySchema, Schema valueSchema) {
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

    /**
     * @return the type of this schema
     */
    @Override
    public Type type() {
        return type;
    }

    /**
     * @return true if this field is optional, false otherwise
     */
    @Override
    public boolean isOptional() {
        return optional;
    }

    /**
     * @return the default value for this schema
     */
    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    /**
     * @return the name of this schema
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * @return the version of this schema
     */
    @Override
    public byte[] version() {
        return version;
    }

    /**
     * @return the documentation for this schema
     */
    @Override
    public String doc() {
        return doc;
    }



    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     * @return the list of fields for this Schema
     */
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     * @param fieldName the name of the field to look up
     * @return the Field object for the specified field, or null if there is no field with the given name
     */
    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fieldsByName.get(fieldName);
    }

    /**
     * Get the key schema for this map schema. Throws a DataException if this schema is not a map.
     * @return the key schema
     */
    @Override
    public Schema keySchema() {
        if (type != Type.MAP)
            throw new DataException("Cannot look up key schema on non-map type");
        return keySchema;
    }

    /**
     * Get the value schema for this map or array schema. Throws a DataException if this schema is not a map or array.
     * @return the value schema
     */
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
    public static void validateValue(ISchema schema, Object value) {
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

    /**
     * Return a concrete instance of the {@link Schema}
     * @return the {@link Schema}
     */
    @Override
    public Schema schema() {
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
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
