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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ConnectSchema implements Schema {
    /**
     * Maps {@link Schema.Type}s to a list of Java classes that can be used to represent them.
     */
    private static final Map<Type, List<Class<?>>> SCHEMA_TYPE_CLASSES = Collections.unmodifiableMap(new EnumMap<>(Map.ofEntries(
        Map.entry(Type.INT8, List.of(Byte.class)),
        Map.entry(Type.INT16, List.of(Short.class)),
        Map.entry(Type.INT32, List.of(Integer.class)),
        Map.entry(Type.INT64, List.of(Long.class)),
        Map.entry(Type.FLOAT32, List.of(Float.class)),
        Map.entry(Type.FLOAT64, List.of(Double.class)),
        Map.entry(Type.BOOLEAN, List.of(Boolean.class)),
        Map.entry(Type.STRING, List.of(String.class)),
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        Map.entry(Type.BYTES, List.of(byte[].class, ByteBuffer.class)),
        Map.entry(Type.ARRAY, List.of(List.class)),
        Map.entry(Type.MAP, List.of(Map.class)),
        Map.entry(Type.STRUCT, List.of(Struct.class))
    )));
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    // We don't need to put these into JAVA_CLASS_SCHEMA_TYPES since that's only used to determine schemas for
    // schemaless data and logical types will have ambiguous schemas (e.g. many of them use the same Java class) so
    // they should not be used without schemas.
    private static final Map<String, List<Class<?>>> LOGICAL_TYPE_CLASSES = Map.of(
        Decimal.LOGICAL_NAME, List.of(BigDecimal.class),
        Date.LOGICAL_NAME, List.of(java.util.Date.class),
        Time.LOGICAL_NAME, List.of(java.util.Date.class),
        Timestamp.LOGICAL_NAME, List.of(java.util.Date.class)
    );

    /**
     * Maps the Java classes to the corresponding {@link Schema.Type}.
     */
    private static final Map<Class<?>, Type> JAVA_CLASS_SCHEMA_TYPES = SCHEMA_TYPE_CLASSES.entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream().map(klass -> Map.entry(klass, entry.getKey())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

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
    private final Integer version;
    // Optional human readable documentation describing this schema.
    private final String doc;
    private final Map<String, String> parameters;
    // precomputed hash code. There is no need to re-compute every time hashCode() is called.
    private Integer hash = null;

    /**
     * Construct a Schema. Most users should not construct schemas manually, preferring {@link SchemaBuilder} instead.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc, Map<String, String> parameters, List<Field> fields, Schema keySchema, Schema valueSchema) {
        this.type = type;
        this.optional = optional;
        this.defaultValue = defaultValue;
        this.name = name;
        this.version = version;
        this.doc = doc;
        this.parameters = parameters;

        if (this.type == Type.STRUCT) {
            this.fields = fields == null ? List.of() : fields;
            this.fieldsByName = new HashMap<>(this.fields.size());
            for (Field field : this.fields)
                fieldsByName.put(field.name(), field);
        } else {
            this.fields = null;
            this.fieldsByName = null;
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    /**
     * Construct a Schema for a primitive type, setting schema parameters, struct fields, and key and value schemas to null.
     */
    public ConnectSchema(Type type, boolean optional, Object defaultValue, String name, Integer version, String doc) {
        this(type, optional, defaultValue, name, version, doc, null, null, null, null);
    }

    /**
     * Construct a default schema for a primitive type. The schema is required, has no default value, name, version,
     * or documentation.
     */
    public ConnectSchema(Type type) {
        this(type, false, null, null, null, null);
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
    public Integer version() {
        return version;
    }

    @Override
    public String doc() {
        return doc;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return fields;
    }

    @Override
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
     * Validate that the value can be used with the schema, i.e. that its type matches the schema type and nullability
     * requirements. Throws a {@link DataException} if the value is invalid.
     * @param schema Schema to test
     * @param value value to test
     */
    public static void validateValue(Schema schema, Object value) {
        validateValue(null, schema, value);
    }

    public static void validateValue(String field, Schema schema, Object value) {
        validateValue(schema, value, field == null ? "value" : "field: \"" + field + "\"");
    }

    private static void validateValue(Schema schema, Object value, String location) {
        if (value == null) {
            if (!schema.isOptional())
                throw new DataException("Invalid value: null used for required " + location
                        + ", schema type: " + schema.type());
            return;
        }

        List<Class<?>> expectedClasses = expectedClassesFor(schema);
        boolean foundMatch = false;
        for (Class<?> expectedClass : expectedClasses) {
            if (expectedClass.isInstance(value)) {
                foundMatch = true;
                break;
            }
        }

        if (!foundMatch) {
            StringBuilder exceptionMessage = new StringBuilder("Invalid Java object for schema");
            if (schema.name() != null) {
                exceptionMessage.append(" \"").append(schema.name()).append("\"");
            }
            exceptionMessage.append(" with type ").append(schema.type()).append(": ").append(value.getClass());
            if (location != null) {
                exceptionMessage.append(" for ").append(location);
            }
            throw new DataException(exceptionMessage.toString());
        }

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    throw new DataException("Struct schemas do not match.");
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                String entryLocation = "element of array " + location;
                Schema arrayValueSchema = assertSchemaNotNull(schema.valueSchema(), entryLocation);
                for (Object entry : array) {
                    validateValue(arrayValueSchema, entry, entryLocation);
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                String keyLocation = "key of map " + location;
                String valueLocation = "value of map " + location;
                Schema mapKeySchema = assertSchemaNotNull(schema.keySchema(), keyLocation);
                Schema mapValueSchema = assertSchemaNotNull(schema.valueSchema(), valueLocation);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(mapKeySchema, entry.getKey(), keyLocation);
                    validateValue(mapValueSchema, entry.getValue(), valueLocation);
                }
                break;
        }
    }

    private static Schema assertSchemaNotNull(Schema schema, String location) {
        if (schema == null) {
            throw new DataException("No schema defined for " + location);
        }
        return schema;
    }

    private static List<Class<?>> expectedClassesFor(Schema schema) {
        List<Class<?>> expectedClasses = null;
        if (schema.name() != null) {
            expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.name());
        }
        if (expectedClasses == null)
            expectedClasses = SCHEMA_TYPE_CLASSES.getOrDefault(schema.type(), List.of());
        return expectedClasses;
    }

    /**
     * Validate that the value can be used for this schema, i.e. that its type matches the schema type and optional
     * requirements. Throws a {@link DataException} if the value is invalid.
     * @param value the value to validate
     */
    public void validateValue(Object value) {
        validateValue(this, value);
    }

    @Override
    public ConnectSchema schema() {
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectSchema schema = (ConnectSchema) o;
        return Objects.equals(optional, schema.optional) &&
                Objects.equals(version, schema.version) &&
                Objects.equals(name, schema.name) &&
                Objects.equals(doc, schema.doc) &&
                Objects.equals(type, schema.type) &&
                Objects.deepEquals(defaultValue, schema.defaultValue) &&
                Objects.equals(fields, schema.fields) &&
                Objects.equals(keySchema, schema.keySchema) &&
                Objects.equals(valueSchema, schema.valueSchema) &&
                Objects.equals(parameters, schema.parameters);
    }

    @Override
    public int hashCode() {
        if (this.hash == null) {
            this.hash = Objects.hash(type, optional, defaultValue, fields, keySchema, valueSchema, name, version, doc,
                parameters);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        if (name != null)
            return "Schema{" + name + ":" + type + "}";
        else
            return "Schema{" + type + "}";
    }


    /**
     * Get the {@link Schema.Type} associated with the given class.
     *
     * @param klass the Class whose associated schema type is to be returned
     * @return the corresponding type, or null if there is no matching type
     */
    public static Type schemaType(Class<?> klass) {
        synchronized (JAVA_CLASS_SCHEMA_TYPES) {
            Type schemaType = JAVA_CLASS_SCHEMA_TYPES.get(klass);
            if (schemaType != null)
                return schemaType;

            // Since the lookup only checks the class, we need to also try
            for (Map.Entry<Class<?>, Type> entry : JAVA_CLASS_SCHEMA_TYPES.entrySet()) {
                try {
                    klass.asSubclass(entry.getKey());
                    // Cache this for subsequent lookups
                    JAVA_CLASS_SCHEMA_TYPES.put(klass, entry.getValue());
                    return entry.getValue();
                } catch (ClassCastException e) {
                    // Expected, ignore
                }
            }
        }
        return null;
    }
}
