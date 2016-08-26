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

package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <p>
 *     A structured record containing a set of named fields with values, each field using an independent {@link Schema}.
 *     Struct objects must specify a complete {@link Schema} up front, and only fields specified in the Schema may be set.
 * </p>
 * <p>
 *     The Struct's {@link #put(String, Object)} method returns the Struct itself to provide a fluent API for constructing
 *     complete objects:
 *     <pre>
 *         Schema schema = SchemaBuilder.struct().name("com.example.Person")
 *             .field("name", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA).build()
 *         Struct struct = new Struct(schema).put("name", "Bobby McGee").put("age", 21)
 *     </pre>
 * </p>
 */
public class Struct {

    private final Schema schema;
    private final Object[] values;

    /**
     * Create a new Struct for this {@link Schema}
     * @param schema the {@link Schema} for the Struct
     */
    public Struct(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT)
            throw new DataException("Not a struct schema: " + schema);
        this.schema = schema;
        this.values = new Object[schema.fields().size()];
    }

    /**
     * Get the schema for this Struct.
     * @return the Struct's schema
     */
    public Schema schema() {
        return schema;
    }

    /**
     * Get the value of a field, returning the default value if no value has been set yet and a default value is specified
     * in the field's schema. Because this handles fields of all types, the value is returned as an {@link Object} and
     * must be cast to a more specific type.
     * @param fieldName the field name to lookup
     * @return the value for the field
     */
    public Object get(String fieldName) {
        Field field = lookupField(fieldName);
        return get(field);
    }

    /**
     * Get the value of a field, returning the default value if no value has been set yet and a default value is specified
     * in the field's schema. Because this handles fields of all types, the value is returned as an {@link Object} and
     * must be cast to a more specific type.
     * @param field the field to lookup
     * @return the value for the field
     */
    public Object get(Field field) {
        Object val = values[field.index()];
        if (val == null && field.schema().defaultValue() != null) {
            val = field.schema().defaultValue();
        }
        return val;
    }

    /**
     * Get the underlying raw value for the field without accounting for default values.
     * @param fieldName the field to get the value of
     * @return the raw value
     */
    public Object getWithoutDefault(String fieldName) {
        Field field = lookupField(fieldName);
        return values[field.index()];
    }

    // Note that all getters have to have boxed return types since the fields might be optional

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Byte.
     */
    public Byte getInt8(String fieldName) {
        return (Byte) getCheckType(fieldName, Schema.Type.INT8);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Short.
     */
    public Short getInt16(String fieldName) {
        return (Short) getCheckType(fieldName, Schema.Type.INT16);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Integer.
     */
    public Integer getInt32(String fieldName) {
        return (Integer) getCheckType(fieldName, Schema.Type.INT32);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Long.
     */
    public Long getInt64(String fieldName) {
        return (Long) getCheckType(fieldName, Schema.Type.INT64);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Float.
     */
    public Float getFloat32(String fieldName) {
        return (Float) getCheckType(fieldName, Schema.Type.FLOAT32);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Double.
     */
    public Double getFloat64(String fieldName) {
        return (Double) getCheckType(fieldName, Schema.Type.FLOAT64);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Boolean.
     */
    public Boolean getBoolean(String fieldName) {
        return (Boolean) getCheckType(fieldName, Schema.Type.BOOLEAN);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a String.
     */
    public String getString(String fieldName) {
        return (String) getCheckType(fieldName, Schema.Type.STRING);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a byte[].
     */
    public byte[] getBytes(String fieldName) {
        Object bytes = getCheckType(fieldName, Schema.Type.BYTES);
        if (bytes instanceof ByteBuffer)
            return ((ByteBuffer) bytes).array();
        return (byte[]) bytes;
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a List.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getArray(String fieldName) {
        return (List<T>) getCheckType(fieldName, Schema.Type.ARRAY);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Map.
     */
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String fieldName) {
        return (Map<K, V>) getCheckType(fieldName, Schema.Type.MAP);
    }

    /**
     * Equivalent to calling {@link #get(String)} and casting the result to a Struct.
     */
    public Struct getStruct(String fieldName) {
        return (Struct) getCheckType(fieldName, Schema.Type.STRUCT);
    }

    /**
     * Set the value of a field. Validates the value, throwing a {@link DataException} if it does not match the field's
     * {@link Schema}.
     * @param fieldName the name of the field to set
     * @param value the value of the field
     * @return the Struct, to allow chaining of {@link #put(String, Object)} calls
     */
    public Struct put(String fieldName, Object value) {
        Field field = lookupField(fieldName);
        return put(field, value);
    }

    /**
     * Set the value of a field. Validates the value, throwing a {@link DataException} if it does not match the field's
     * {@link Schema}.
     * @param field the field to set
     * @param value the value of the field
     * @return the Struct, to allow chaining of {@link #put(String, Object)} calls
     */
    public Struct put(Field field, Object value) {
        ConnectSchema.validateValue(field.schema(), value);
        values[field.index()] = value;
        return this;
    }


    /**
     * Validates that this struct has filled in all the necessary data with valid values. For required fields
     * without defaults, this validates that a value has been set and has matching types/schemas. If any validation
     * fails, throws a DataException.
     */
    public void validate() {
        for (Field field : schema.fields()) {
            Schema fieldSchema = field.schema();
            Object value = values[field.index()];
            if (value == null && (fieldSchema.isOptional() || fieldSchema.defaultValue() != null))
                continue;
            ConnectSchema.validateValue(fieldSchema, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Struct struct = (Struct) o;
        return Objects.equals(schema, struct.schema) &&
                Arrays.equals(values, struct.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, Arrays.hashCode(values));
    }

    private Field lookupField(String fieldName) {
        Field field = schema.field(fieldName);
        if (field == null)
            throw new DataException(fieldName + " is not a valid field name");
        return field;
    }

    // Get the field's value, but also check that the field matches the specified type, throwing an exception if it doesn't.
    // Used to implement the get*() methods that return typed data instead of Object
    private Object getCheckType(String fieldName, Schema.Type type) {
        Field field = lookupField(fieldName);
        if (field.schema().type() != type)
            throw new DataException("Field '" + fieldName + "' is not of type " + type);
        return values[field.index()];
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Struct{");
        boolean first = true;
        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value != null) {
                final Field field = schema.fields().get(i);
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(field.name()).append("=").append(value);
            }
        }
        return sb.append("}").toString();
    }

}

