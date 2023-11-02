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
import org.apache.kafka.connect.errors.SchemaBuilderException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *     SchemaBuilder provides a fluent API for constructing {@link Schema} objects. It allows you to set each of the
 *     properties for the schema and each call returns the SchemaBuilder so the calls can be chained. When nested types
 *     are required, use one of the predefined schemas from {@link Schema} or use a second SchemaBuilder inline.
 * </p>
 * <p>
 *     Here is an example of building a struct schema:
 *     <pre>
 *     Schema dateSchema = SchemaBuilder.struct()
 *         .name("com.example.CalendarDate").version(2).doc("A calendar date including month, day, and year.")
 *         .field("month", Schema.STRING_SCHEMA)
 *         .field("day", Schema.INT8_SCHEMA)
 *         .field("year", Schema.INT16_SCHEMA)
 *         .build();
 *     </pre>
 * </p>
 * <p>
 *     Here is an example of using a second SchemaBuilder to construct complex, nested types:
 *     <pre>
 *     Schema userListSchema = SchemaBuilder.array(
 *         SchemaBuilder.struct().name("com.example.User").field("username", Schema.STRING_SCHEMA).field("id", Schema.INT64_SCHEMA).build()
 *     ).build();
 *     </pre>
 * </p>
 */
public class SchemaBuilder implements Schema {
    private static final String TYPE_FIELD = "type";
    private static final String OPTIONAL_FIELD = "optional";
    private static final String DEFAULT_FIELD = "default";
    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";
    private static final String DOC_FIELD = "doc";


    private final Type type;
    private Boolean optional = null;
    private Object defaultValue = null;

    private Map<String, Field> fields = null;
    private Schema keySchema = null;
    private Schema valueSchema = null;

    private String name;
    private Integer version;
    // Optional human readable documentation describing this schema.
    private String doc;
    // Additional parameters for logical types.
    private Map<String, String> parameters;

    public SchemaBuilder(Type type) {
        if (null == type)
            throw new SchemaBuilderException("type cannot be null");
        this.type = type;
        if (type == Type.STRUCT) {
            fields = new LinkedHashMap<>();
        }
    }

    // Common/metadata fields

    @Override
    public boolean isOptional() {
        return optional == null ? false : optional;
    }

    /**
     * Set this schema as optional.
     * @return the SchemaBuilder
     */
    public SchemaBuilder optional() {
        checkCanSet(OPTIONAL_FIELD, optional, true);
        optional = true;
        return this;
    }

    /**
     * Set this schema as required. This is the default, but this method can be used to make this choice explicit.
     * @return the SchemaBuilder
     */
    public SchemaBuilder required() {
        checkCanSet(OPTIONAL_FIELD, optional, false);
        optional = false;
        return this;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    /**
     * Set the default value for this schema. The value is validated against the schema type, throwing a
     * {@link SchemaBuilderException} if it does not match.
     * @param value the default value
     * @return the SchemaBuilder
     */
    public SchemaBuilder defaultValue(Object value) {
        checkCanSet(DEFAULT_FIELD, defaultValue, value);
        checkNotNull(TYPE_FIELD, type, DEFAULT_FIELD);
        try {
            ConnectSchema.validateValue(this, value);
        } catch (DataException e) {
            throw new SchemaBuilderException("Invalid default value", e);
        }
        defaultValue = value;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Set the name of this schema.
     * @param name the schema name
     * @return the SchemaBuilder
     */
    public SchemaBuilder name(String name) {
        checkCanSet(NAME_FIELD, this.name, name);
        this.name = name;
        return this;
    }

    @Override
    public Integer version() {
        return version;
    }

    /**
     * Set the version of this schema. Schema versions are integers which, if provided, must indicate which schema is
     * newer and which is older by their ordering.
     * @param version the schema version
     * @return the SchemaBuilder
     */
    public SchemaBuilder version(Integer version) {
        checkCanSet(VERSION_FIELD, this.version, version);
        this.version = version;
        return this;
    }

    @Override
    public String doc() {
        return doc;
    }

    /**
     * Set the documentation for this schema.
     * @param doc the documentation
     * @return the SchemaBuilder
     */
    public SchemaBuilder doc(String doc) {
        checkCanSet(DOC_FIELD, this.doc, doc);
        this.doc = doc;
        return this;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters == null ? null : Collections.unmodifiableMap(parameters);
    }

    /**
     * Set a schema parameter.
     * @param propertyName name of the schema property to define
     * @param propertyValue value of the schema property to define, as a String
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.put(propertyName, propertyValue);
        return this;
    }

    /**
     * Set schema parameters. This operation is additive; it does not remove existing parameters that do not appear in
     * the set of properties pass to this method.
     * @param props Map of properties to set
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty())
            return this;
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.putAll(props);
        return this;
    }

    @Override
    public Type type() {
        return type;
    }

    /**
     * Create a SchemaBuilder for the specified type.
     * <p>
     * Usually it will be simpler to use one of the variants like {@link #string()} or {@link #struct()}, but this form
     * can be useful when generating schemas dynamically.
     *
     * @param type the schema type
     * @return a new SchemaBuilder
     */
    public static SchemaBuilder type(Type type) {
        return new SchemaBuilder(type);
    }

    // Primitive types

    /**
     * @return a new {@link Schema.Type#INT8} SchemaBuilder
     */
    public static SchemaBuilder int8() {
        return new SchemaBuilder(Type.INT8);
    }

    /**
     * @return a new {@link Schema.Type#INT16} SchemaBuilder
     */
    public static SchemaBuilder int16() {
        return new SchemaBuilder(Type.INT16);
    }

    /**
     * @return a new {@link Schema.Type#INT32} SchemaBuilder
     */
    public static SchemaBuilder int32() {
        return new SchemaBuilder(Type.INT32);
    }

    /**
     * @return a new {@link Schema.Type#INT64} SchemaBuilder
     */
    public static SchemaBuilder int64() {
        return new SchemaBuilder(Type.INT64);
    }

    /**
     * @return a new {@link Schema.Type#FLOAT32} SchemaBuilder
     */
    public static SchemaBuilder float32() {
        return new SchemaBuilder(Type.FLOAT32);
    }

    /**
     * @return a new {@link Schema.Type#FLOAT64} SchemaBuilder
     */
    public static SchemaBuilder float64() {
        return new SchemaBuilder(Type.FLOAT64);
    }

    /**
     * @return a new {@link Schema.Type#BOOLEAN} SchemaBuilder
     */
    public static SchemaBuilder bool() {
        return new SchemaBuilder(Type.BOOLEAN);
    }

    /**
     * @return a new {@link Schema.Type#STRING} SchemaBuilder
     */
    public static SchemaBuilder string() {
        return new SchemaBuilder(Type.STRING);
    }

    /**
     * @return a new {@link Schema.Type#BYTES} SchemaBuilder
     */
    public static SchemaBuilder bytes() {
        return new SchemaBuilder(Type.BYTES);
    }


    // Structs

    /**
     * @return a new {@link Schema.Type#STRUCT} SchemaBuilder
     */
    public static SchemaBuilder struct() {
        return new SchemaBuilder(Type.STRUCT);
    }

    /**
     * Add a field to this {@link Schema.Type#STRUCT} schema. Throws a {@link SchemaBuilderException} if this is not a struct schema.
     * @param fieldName the name of the field to add
     * @param fieldSchema the Schema for the field's value
     * @return the SchemaBuilder
     */
    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
        if (type != Type.STRUCT)
            throw new SchemaBuilderException("Cannot create fields on type " + type);
        if (null == fieldName || fieldName.isEmpty())
            throw new SchemaBuilderException("fieldName cannot be null.");
        if (null == fieldSchema)
            throw new SchemaBuilderException("fieldSchema for field " + fieldName + " cannot be null.");
        int fieldIndex = fields.size();
        if (fields.containsKey(fieldName))
            throw new SchemaBuilderException("Cannot create field because of field name duplication " + fieldName);
        fields.put(fieldName, new Field(fieldName, fieldIndex, fieldSchema));
        return this;
    }

    /**
     * Get the list of fields for this Schema. Throws a {@link DataException} if this schema is not a {@link Schema.Type#STRUCT}.
     * @return the list of fields for this Schema
     */
    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return new ArrayList<>(fields.values());
    }

    @Override
    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fields.get(fieldName);
    }



    // Maps & Arrays

    /**
     * @param valueSchema the schema for elements of the array
     * @return a new {@link Schema.Type#ARRAY} SchemaBuilder
     */
    public static SchemaBuilder array(Schema valueSchema) {
        if (null == valueSchema)
            throw new SchemaBuilderException("valueSchema cannot be null.");
        SchemaBuilder builder = new SchemaBuilder(Type.ARRAY);
        builder.valueSchema = valueSchema;
        return builder;
    }

    /**
     * @param keySchema the schema for keys in the map
     * @param valueSchema the schema for values in the map
     * @return a new {@link Schema.Type#MAP} SchemaBuilder
     */
    public static SchemaBuilder map(Schema keySchema, Schema valueSchema) {
        if (null == keySchema)
            throw new SchemaBuilderException("keySchema cannot be null.");
        if (null == valueSchema)
            throw new SchemaBuilderException("valueSchema cannot be null.");
        SchemaBuilder builder = new SchemaBuilder(Type.MAP);
        builder.keySchema = keySchema;
        builder.valueSchema = valueSchema;
        return builder;
    }

    static SchemaBuilder arrayOfNull() {
        return new SchemaBuilder(Type.ARRAY);
    }

    static SchemaBuilder mapOfNull() {
        return new SchemaBuilder(Type.MAP);
    }

    static SchemaBuilder mapWithNullKeys(Schema valueSchema) {
        SchemaBuilder result = new SchemaBuilder(Type.MAP);
        result.valueSchema = valueSchema;
        return result;
    }

    static SchemaBuilder mapWithNullValues(Schema keySchema) {
        SchemaBuilder result = new SchemaBuilder(Type.MAP);
        result.keySchema = keySchema;
        return result;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }


    /**
     * Build the Schema using the current settings
     * @return the {@link Schema}
     */
    public Schema build() {
        return new ConnectSchema(type, isOptional(), defaultValue, name, version, doc,
                parameters == null ? null : Collections.unmodifiableMap(parameters),
                fields == null ? null : Collections.unmodifiableList(new ArrayList<>(fields.values())), keySchema, valueSchema);
    }

    /**
     * Return a concrete instance of the {@link Schema} specified by this builder
     * @return the {@link Schema}
     */
    @Override
    public Schema schema() {
        return build();
    }

    private static void checkCanSet(String fieldName, Object fieldVal, Object val) {
        if (fieldVal != null && fieldVal != val)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " has already been set.");
    }

    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
    }
}