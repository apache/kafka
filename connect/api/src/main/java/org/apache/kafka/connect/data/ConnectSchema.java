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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectSchema implements Schema {
    /**
     * Maps {@link Schema.Type}s to a list of Java classes that can be used to represent them.
     */
    private static final Map<Type, List<Class<?>>> SCHEMA_TYPE_CLASSES = new EnumMap<>(Type.class);
    /**
     * Maps known logical types to a list of Java classes that can be used to represent them.
     */
    private static final Map<String, List<Class<?>>> LOGICAL_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the Java classes to the corresponding {@link Schema.Type}.
     */
    private static final Map<Class<?>, Type> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        SCHEMA_TYPE_CLASSES.put(Type.INT8, Collections.singletonList(Byte.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT16, Collections.singletonList(Short.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Collections.singletonList(Integer.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Collections.singletonList(Long.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Collections.singletonList(Float.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Collections.singletonList(Double.class));
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Collections.singletonList(Boolean.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRING, Collections.singletonList(String.class));
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        SCHEMA_TYPE_CLASSES.put(Type.BYTES, Arrays.asList(byte[].class, ByteBuffer.class));
        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, Collections.singletonList(List.class));
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Collections.singletonList(Map.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Collections.singletonList(Struct.class));

        for (Map.Entry<Type, List<Class<?>>> schemaClasses : SCHEMA_TYPE_CLASSES.entrySet()) {
            for (Class<?> schemaClass : schemaClasses.getValue())
                JAVA_CLASS_SCHEMA_TYPES.put(schemaClass, schemaClasses.getKey());
        }

        LOGICAL_TYPE_CLASSES.put(Decimal.LOGICAL_NAME, Collections.singletonList(BigDecimal.class));
        LOGICAL_TYPE_CLASSES.put(Date.LOGICAL_NAME, Collections.singletonList(java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Time.LOGICAL_NAME, Collections.singletonList(java.util.Date.class));
        LOGICAL_TYPE_CLASSES.put(Timestamp.LOGICAL_NAME, Collections.singletonList(java.util.Date.class));
        // We don't need to put these into JAVA_CLASS_SCHEMA_TYPES since that's only used to determine schemas for
        // schemaless data and logical types will have ambiguous schemas (e.g. many of them use the same Java class) so
        // they should not be used without schemas.
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
            this.fields = fields == null ? Collections.emptyList() : fields;
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

    public static void validateValue(String name, Schema schema, Object value) {
        if (value == null) {
            if (!schema.isOptional())
                throw new DataException("Invalid value: null used for required field: \"" + name
                        + "\", schema type: " + schema.type());
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
            if (name != null) {
                exceptionMessage.append(" for field: \"").append(name).append("\"");
            }
            throw new DataException(exceptionMessage.toString());
        }

        switch (schema.type()) {
            case STRUCT:
                Struct struct = (Struct) value;
                if (!equals(schema, struct.schema()))
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

    private static List<Class<?>> expectedClassesFor(Schema schema) {
        List<Class<?>> expectedClasses = LOGICAL_TYPE_CLASSES.get(schema.name());
        if (expectedClasses == null)
            expectedClasses = SCHEMA_TYPE_CLASSES.getOrDefault(schema.type(), Collections.emptyList());
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
        if (!(o instanceof Schema)) return false;
        Schema schema = (Schema) o;
        return equals(this, schema);
    }

    private static boolean equals(Schema left, Schema right) {
        return equals(left, right, new HashSet<>());
    }

    private static boolean equals(Schema left, Schema right, Set<SchemaPair> equivalentSchemas) {
        if (left == right)
            return true;

        if (left == null || right == null)
            return false;

        SchemaPair pair = new SchemaPair(left, right);
        if (equivalentSchemas.contains(pair)) {
            return true;
        }

        boolean shallowMatches = Objects.equals(left.isOptional(), right.isOptional()) &&
                Objects.equals(left.version(), right.version()) &&
                Objects.equals(left.name(), right.name()) &&
                Objects.equals(left.doc(), right.doc()) &&
                Objects.equals(left.type(), right.type()) &&
                Objects.equals(left.parameters(), right.parameters());
        if (!shallowMatches)
            return false;

        equivalentSchemas.add(pair);

        switch (left.type()) {
            case ARRAY:
                return equals(left.valueSchema(), right.valueSchema(), equivalentSchemas)
                        && defaultValueEquals(left, right);
            case MAP:
                return equals(left.keySchema(), right.keySchema(), equivalentSchemas)
                        && equals(left.valueSchema(), right.valueSchema(), equivalentSchemas)
                        && defaultValueEquals(left, right);
            case STRUCT:
                if (left.fields().size() != right.fields().size())
                    return false;
                for (int i = 0; i < left.fields().size(); i++) {
                    // 2004
                    Field mannyRamirez = left.fields().get(i);
                    Field trotNixon = right.fields().get(i);
                    if (!fieldEquals(mannyRamirez, trotNixon, equivalentSchemas))
                        return false;
                }
                return defaultValueEquals(left, right);
            default:
                return defaultValueEquals(left, right);
        }
    }

    private static boolean fieldEquals(Field left, Field right, Set<SchemaPair> equivalentSchemas) {
        return Objects.equals(left.name(), right.name())
                && Objects.equals(left.index(), right.index())
                && equals(left.schema(), right.schema(), equivalentSchemas);
    }

    private static boolean defaultValueEquals(Schema leftSchema, Schema rightSchema) {
        Object left = leftSchema.defaultValue();
        Object right = rightSchema.defaultValue();
        return defaultValueEquals(left, leftSchema, right, rightSchema);
    }

    private static boolean defaultValueEquals(Object left, Schema leftSchema, Object right, Schema rightSchema) {
        if (left == right) {
            return true;
        } else if (left == null || right == null) {
            return false;
        } else if (leftSchema.type().isPrimitive()) {
            // Primitive types have to be referentially equal
            return false;
        }

        switch (leftSchema.type()) {
            case ARRAY: {
                List<?> leftArray = toList(left);
                List<?> rightArray = toList(right);
                if (leftArray.size() != rightArray.size())
                    return false;
                Schema leftValueSchema = leftSchema.valueSchema();
                Schema rightValueSchema = rightSchema.valueSchema();
                for (int i = 0; i < leftArray.size(); i++) {
                    Object leftArrayValue = leftArray.get(i);
                    Object rightArrayValue = rightArray.get(i);
                    if (!defaultValueEquals(leftArrayValue, leftValueSchema, rightArrayValue, rightValueSchema))
                        return false;
                }
                return true;
            }

            case MAP: {
                Map<?, ?> leftMap = (Map<?, ?>) left;
                Map<?, ?> rightMap = (Map<?, ?>) right;
                if (leftMap.size() != rightMap.size()) {
                    return false;
                }
                Schema leftValueSchema = leftSchema.valueSchema();
                Schema rightValueSchema = rightSchema.valueSchema();

                for (Map.Entry<?, ?> leftEntry : leftMap.entrySet()) {
                    if (!rightMap.containsKey(leftEntry.getKey()))
                        return false;

                    Object leftMapValue = leftEntry.getValue();
                    Object rightMapValue = rightMap.get(leftEntry.getKey());

                    if (!defaultValueEquals(leftMapValue, leftValueSchema, rightMapValue, rightValueSchema))
                        return false;
                }
                return true;
            }

            case STRUCT:
                Struct leftStruct = (Struct) left;
                Struct rightStruct = (Struct) right;
                List<Field> leftSchemaFields = leftSchema.fields();
                List<Field> rightSchemaFields = rightSchema.fields();

                // Shouldn't happen since the caller should have ensured that the two schemas are equal, but
                // safer to catch this case than to throw an exception
                if (leftSchemaFields.size() != rightSchemaFields.size())
                    return false;

                for (int i = 0; i < leftSchemaFields.size(); i++) {
                    Field leftField = leftSchemaFields.get(i);
                    Field rightField = rightSchemaFields.get(i);

                    Object leftFieldValue = leftStruct.get(leftField);
                    Object rightFieldValue = rightStruct.get(rightField);
                    if (!defaultValueEquals(leftFieldValue, leftField.schema(), rightFieldValue, rightField.schema()))
                        return false;
                }

                return true;

            default:
                throw new IllegalArgumentException("Unexpected schema type that is non-primitive, but also not an array, map, or struct: " + leftSchema.type());
        }
    }

    private static List<?> toList(Object o) {
        if (o instanceof Object[]) {
            return Arrays.asList((Object[]) o);
        } else if (o instanceof List) {
            return (List<?>) o;
        } else {
            throw new IllegalArgumentException("Object " + o + " is not a recognized array type");
        }
    }

    @Override
    public int hashCode() {
        if (this.hash == null) {
            // We take the shallow hash of subschemas (i.e., key, value, and field schemas)
            // in order to avoid infinite loops for recursive schemas
            // We might expand this method in the future to take those into account, but
            // for now we take the simpler approach

            List<Field> hashFields = this.fields != null ? this.fields : Collections.emptyList();
            List<Integer> fieldSchemaHashes = hashFields.stream()
                    .map(Field::schema)
                    .map(ConnectSchema::shallowHashCode)
                    .collect(Collectors.toList());
            List<String> fieldNames = hashFields.stream()
                    .map(Field::name)
                    .collect(Collectors.toList());

            this.hash = Objects.hash(
                    type, optional, name, version, doc, parameters,
                    fieldSchemaHashes, fieldNames,
                    shallowHashCode(keySchema),
                    shallowHashCode(valueSchema)
            );
        }
        return this.hash;
    }

    private static int shallowHashCode(Schema schema) {
        if (schema == null) {
            return 0;
        }
        // The schema may be mutable (if, for example, it's a SchemaBuilder instance).
        // We assume here that the type of the schema will be immutable, and since we
        // cache hash codes, we do not rely on any other attributes of the schema
        return Objects.hash(schema.type());
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

    private static class SchemaPair {
        public final Schema left;
        public final Schema right;
        private Integer hash;

        public SchemaPair(Schema left, Schema right) {
            this.left = left;
            this.right = right;
            this.hash = null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaPair that = (SchemaPair) o;
            // Use referential equality because object equality might cause a stack overflow
            // if used during ConnectSchema::equals
            return this.left == that.left && this.right == that.right;
        }

        @Override
        public int hashCode() {
            if (this.hash == null) {
                this.hash = Objects.hash(
                        System.identityHashCode(left),
                        System.identityHashCode(right)
                );
            }
            return hash;
        }
    }

}
