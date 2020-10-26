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

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Cast<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(Cast.class);

    // TODO: Currently we only support field casting at the top level (recursive=false) or casting all nested children where the child field name matches the spec (recursive=true).
    // Ideally we could use a dotted (parent.child) or a path-like notation (parent->child) in the spec to allow casting only specific nested fields.
    public static final String OVERVIEW_DOC =
            "Cast fields or the entire key or value to a specific type, e.g. to force an integer field to a smaller "
                    + "width. Simple primitive types are supported -- integers, floats, boolean, and string, plus "
                    + "support for string representation of complex types. Recursion through nested values is also "
                    + "possible by setting <code>recursive</code> to <code>true</code>. "
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>). ";

    /**
     * <code>SPEC_CONFIG</code> is Deprecated, please use <code>ConfigName.SPEC</code> instead.
     */
    @Deprecated
    public static final String SPEC_CONFIG = "spec";

    public static interface ConfigName {
        String SPEC = "spec";
        String RECURSIVE = "recursive";
        String COMPLEX_STRING_AS_JSON = "complex.string.as.json";
    }

    public static interface ConfigDefault {
        boolean RECURSIVE = false;
        boolean COMPLEX_STRING_AS_JSON = false;
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SPEC, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public void ensureValid(String name, Object valueObject) {
                        List<String> value = (List<String>) valueObject;
                        if (value == null || value.isEmpty()) {
                            throw new ConfigException("Must specify at least one field to cast.");
                        }
                        parseFieldTypes(value);
                    }

                    @Override
                    public String toString() {
                        return "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>";
                    }
            },
            ConfigDef.Importance.HIGH,
            "List of fields and the type to cast them to of the form field1:type,field2:type to cast fields of "
                    + "Maps or Structs. A single type to cast the entire value. Valid types are int8, int16, int32, "
                    + "int64, float32, float64, boolean, string, and complex fields (array, map, and struct) to string.")
            .define(ConfigName.RECURSIVE, ConfigDef.Type.BOOLEAN, ConfigDefault.RECURSIVE, ConfigDef.Importance.MEDIUM, 
            "Boolean which indicates if the cast should recursively cast children fields of nested complex types, "
                    + "if any nested children fields exist with the same names as given in the <code>spec</code>.")
            .define(ConfigName.COMPLEX_STRING_AS_JSON, ConfigDef.Type.BOOLEAN, ConfigDefault.COMPLEX_STRING_AS_JSON, ConfigDef.Importance.MEDIUM, 
            "Boolean which indicates if a complex field (<code>Struct</code>, <code>Array</code>, or <code>Map</code>) "
                    + "is cast to a string, should it be represented as a JSON-like string instead of a string built "
                    + "from the native object itself.");

    private static final String PURPOSE = "cast types";

    private static final Set<Schema.Type> SUPPORTED_CAST_INPUT_TYPES = EnumSet.of(
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
                    Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN,
                    Schema.Type.STRING, Schema.Type.BYTES, Schema.Type.STRUCT, 
                    Schema.Type.ARRAY, Schema.Type.MAP
    );

    private static final Set<Schema.Type> SUPPORTED_CAST_OUTPUT_TYPES = EnumSet.of(
            Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64,
                    Schema.Type.FLOAT32, Schema.Type.FLOAT64, Schema.Type.BOOLEAN,
                    Schema.Type.STRING
    );

    // As a special case for casting the entire value (e.g. the incoming key is a int64 but you know it could be an
    // int32 and want the smaller width), we use an otherwise invalid field name in the cast spec to track this.
    private static final String WHOLE_VALUE_CAST = null;

    private Map<String, Schema.Type> casts;
    private Schema.Type wholeValueCastType;
    private boolean isRecursive;
    private boolean isComplexStringJson;
    private Cache<Schema, Schema> schemaUpdateCache;

    // JSON objects to be used for casting complex types to JSON strings
    private JsonConverter jsonConverter;
    private JsonDeserializer jsonDeserializer;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        casts = parseFieldTypes(config.getList(ConfigName.SPEC));
        wholeValueCastType = casts.get(WHOLE_VALUE_CAST);
        isRecursive = config.getBoolean(ConfigName.RECURSIVE);
        isComplexStringJson = config.getBoolean(ConfigName.COMPLEX_STRING_AS_JSON);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private R applySchemaless(R record) {
        if (wholeValueCastType != null) {
            return newRecord(record, null, castValueToType(null, operatingValue(record), wholeValueCastType));
        }

        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = buildUpdatedSchemalessValue(value);

        return newRecord(record, null, updatedValue);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> buildUpdatedSchemalessValue(Map<String, Object> map) {
        Map<String, Object> updatedMap = new HashMap<>(map.size());

        for (Map.Entry<String, Object> field : map.entrySet()) {
            String fieldName = field.getKey();
            Object fieldValue = field.getValue();

            if (casts.containsKey(fieldName)) {
                final Schema.Type targetType = casts.get(fieldName);
                final Object newFieldValue = castValueToType(null, fieldValue, targetType);
                log.debug("Cast field '{}' from '{}' to '{}'.", fieldName, fieldValue, newFieldValue);
                updatedMap.put(fieldName, newFieldValue);
            } else if (isRecursive && fieldValue instanceof Map<?, ?>) {
                updatedMap.put(fieldName, buildUpdatedSchemalessValue(requireMap(fieldValue, PURPOSE)));
            } else if (isRecursive && fieldValue instanceof List<?>) {
                updatedMap.put(fieldName, buildUpdatedSchemalessArrayValue((List<Object>) fieldValue));
            } else
                updatedMap.put(fieldName, fieldValue);
        }

        return updatedMap;
    }

    @SuppressWarnings("unchecked")
    private List<Object> buildUpdatedSchemalessArrayValue(List<Object> array) {
        List<Object> updatedArray = new ArrayList<Object>(array.size());
        for (Object arrayElement : array) {
            if (isRecursive && arrayElement instanceof List<?>) {
                updatedArray.add(buildUpdatedSchemalessArrayValue((List<Object>) arrayElement));
            } else if (isRecursive && arrayElement instanceof Map<?, ?>) {
                updatedArray.add(buildUpdatedSchemalessValue(requireMap(arrayElement, PURPOSE)));
            } else
                updatedArray.add(arrayElement);
        }
        return updatedArray;
    }

    private R applyWithSchema(R record) {
        Schema valueSchema = operatingSchema(record);
        Schema updatedSchema = getOrBuildUpdatedSchema(valueSchema);

        // Whole-record casting
        if (wholeValueCastType != null)
            return newRecord(record, updatedSchema, castValueToType(valueSchema, operatingValue(record), wholeValueCastType));

        // Casting within a struct
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = (Struct) buildUpdatedSchemaValue(value, updatedSchema);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema getOrBuildUpdatedSchema(Schema valueSchema) {
        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder;
        if (wholeValueCastType != null) {
            builder = SchemaUtil.copySchemaBasics(valueSchema, convertFieldType(wholeValueCastType));
        } else {
            builder = buildUpdatedSchema(valueSchema);
        }

        if (valueSchema.isOptional())
            builder.optional();
        if (valueSchema.defaultValue() != null && SUPPORTED_CAST_OUTPUT_TYPES.contains(builder.type()))
            builder.defaultValue(castValueToType(valueSchema, valueSchema.defaultValue(), builder.type()));

        updatedSchema = builder.build();
        schemaUpdateCache.put(valueSchema, updatedSchema);
        return updatedSchema;
    }

    /***
     * Method which recursively builds nested {@link SchemaBuilder}s based on the {@link Cast} configuration. 
     * Each child schema is also added to the <code>schemaUpdateCache</code> and can be fetched afterwards instead 
     * of being built again.
     * @param schema
     * @return {@link SchemaBuilder} which can be used to build the final {@link Schema}
     */
    private SchemaBuilder buildUpdatedSchema(Schema schema) {

        // Perform different logic for different types of parent schemas.

        if (schema.type() == Type.STRUCT)
            return buildUpdatedStructSchema(schema);

        else if (isRecursive && schema.type() == Type.ARRAY)
            return buildUpdatedArraySchema(schema);

        else if (isRecursive && schema.type() == Type.MAP)
            return buildUpdatedMapSchema(schema);

        else
            throw new DataException(schema.type().toString() + " is not a supported parent Schema type for the Cast transformation.");

    }

    private SchemaBuilder buildUpdatedStructSchema(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (!casts.containsKey(field.name()) && // If we shouldn't cast this parent,
                    isRecursive && // and the config says to recurse,
                    (field.schema().type() == Type.STRUCT // and the field is a complex type...
                    || field.schema().type() == Type.ARRAY 
                    || field.schema().type() == Type.MAP)
            ) { // ... then recurse one level deeper to get/build a child schema for the complex type.
                Schema updatedChildSchema = getOrBuildUpdatedSchema(field.schema());
                builder.field(field.name(), updatedChildSchema);

            } else { // Otherwise this is where all non-parent Struct fields should be added: it is something we want to cast, and it is not recursive or we are at the bottom of a recursion
                if (casts.containsKey(field.name())) {
                    SchemaBuilder fieldBuilder = convertFieldType(casts.get(field.name()));
                    if (field.schema().isOptional())
                        fieldBuilder.optional();
                    if (field.schema().defaultValue() != null) {
                        Schema fieldSchema = field.schema();
                        fieldBuilder.defaultValue(castValueToType(fieldSchema, fieldSchema.defaultValue(), fieldBuilder.type()));
                    }
                    builder.field(field.name(), fieldBuilder.build());

                } else // Copy the existing field to the new schema if we do not want to cast
                    builder.field(field.name(), field.schema());
            }
        }

        return builder;        
    }

    private SchemaBuilder buildUpdatedArraySchema(Schema schema) {
        // For complex types, just go one level lower to more detail and then return a new Array schema builder with the updated child value schema
        if (schema.valueSchema().type() == Type.STRUCT 
                || schema.valueSchema().type() == Type.ARRAY 
                || schema.valueSchema().type() == Type.MAP) {
            Schema updatedChildSchema = getOrBuildUpdatedSchema(schema.valueSchema());
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.array(updatedChildSchema));

        } else // Otherwise we will just assume to pass it along since the Array itself should be part of an upstream parent
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.array(schema.valueSchema()));
    }

    private SchemaBuilder buildUpdatedMapSchema(Schema schema) {
        // For complex types, just go one level lower to more detail and then return a new Map schema builder with the updated child value schema
        if (schema.valueSchema().type() == Type.STRUCT 
                || schema.valueSchema().type() == Type.ARRAY 
                || schema.valueSchema().type() == Type.MAP) {
            Schema updatedChildSchema = getOrBuildUpdatedSchema(schema.valueSchema());
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.map(schema.keySchema(), updatedChildSchema));

        } else // Otherwise we will just assume to pass it along since the Map itself should be part of an upstream parent
            return SchemaUtil.copySchemaBasics(schema, SchemaBuilder.map(schema.keySchema(), schema.valueSchema()));
    }

    @SuppressWarnings("unchecked")
    private Object buildUpdatedSchemaValue(Object value, Schema updatedSchema) {

        if (value == null)
            return null;

        if (updatedSchema.type() == Type.STRUCT) {
            Struct struct = (Struct) value;
            Struct updatedStruct = new Struct(updatedSchema);

            for (Field field : struct.schema().fields()) {
                if (!casts.containsKey(field.name()) && // If we shouldn't cast this parent,
                        isRecursive && // and the config says to recurse,
                        (field.schema().type() == Type.STRUCT // and the field is a complex type...
                        || field.schema().type() == Type.ARRAY 
                        || field.schema().type() == Type.MAP)
                ) { // ... then recurse one level deeper to build the child values for the complex type.
                    Schema childSchema = getOrBuildUpdatedSchema(field.schema());
                    Object childObject = buildUpdatedSchemaValue(struct.get(field), childSchema);
                    updatedStruct.put(updatedSchema.field(field.name()), childObject);

                } else { // Otherwise this is where all non-parent Struct fields should be added: it is something we want to cast, and it is not recursive or we are at the bottom of a recursion
                    if (casts.containsKey(field.name())) {
                        final Object origFieldValue = struct.get(field);
                        final Schema.Type targetType = casts.get(field.name());
                        final Object newFieldValue = castValueToType(field.schema(), origFieldValue, targetType);
                        log.debug("Cast field '{}' from '{}' to '{}'.", field.name(), origFieldValue, newFieldValue);
                        updatedStruct.put(updatedSchema.field(field.name()), newFieldValue);

                    } else
                        updatedStruct.put(updatedSchema.field(field.name()), struct.get(field));
                }
            }
            return updatedStruct;

        } else if (isRecursive && updatedSchema.type() == Type.ARRAY) {
            return buildUpdatedArrayValue((List<Object>) value);

        } else if (isRecursive && updatedSchema.type() == Type.MAP) {
            return buildUpdatedMapValue((Map<Object, Object>) value);

        } else
            throw new DataException(updatedSchema.type().toString() + " is not a supported schema type for the ReplaceField transformation.");
    }

    @SuppressWarnings("unchecked")
    private List<Object> buildUpdatedArrayValue(List<Object> array) {
        List<Object> updatedArray = new ArrayList<Object>(array.size());
        for (Object arrayElement : array) {
            if (isRecursive && arrayElement instanceof Struct) {
                Struct struct = (Struct) arrayElement;
                Schema updatedSchema = getOrBuildUpdatedSchema(struct.schema()); 
                Object updatedStruct = buildUpdatedSchemaValue(struct, updatedSchema);
                updatedArray.add(updatedStruct);
            } else if (isRecursive && arrayElement instanceof List<?>) {
                updatedArray.add(buildUpdatedArrayValue((List<Object>) arrayElement));
            } else if (isRecursive && arrayElement instanceof Map<?, ?>) {
                updatedArray.add(buildUpdatedMapValue((Map<Object, Object>) arrayElement));
            } else
                updatedArray.add(arrayElement);
        }
        return updatedArray;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Object> buildUpdatedMapValue(Map<Object, Object> map) {
        Map<Object, Object> updatedMap = new HashMap<>(map.size());

        for (Map.Entry<Object, Object> mapEntry : map.entrySet()) {
            Object mapEntryKey = mapEntry.getKey();
            Object mapEntryValue = mapEntry.getValue();

            if (isRecursive && mapEntryValue instanceof Struct) {
                Struct struct = (Struct) mapEntry.getValue();
                Schema updatedSchema = getOrBuildUpdatedSchema(struct.schema()); 
                Object updatedStruct = buildUpdatedSchemaValue(struct, updatedSchema);
                updatedMap.put(mapEntryKey, updatedStruct);
            } else if (isRecursive && mapEntryValue instanceof List<?>) {
                updatedMap.put(mapEntryKey, buildUpdatedArrayValue((List<Object>) mapEntryValue));
            } else if (isRecursive && mapEntryValue instanceof Map<?, ?>) {
                updatedMap.put(mapEntryKey, buildUpdatedMapValue((Map<Object, Object>) mapEntryValue));
            } else // Values for this map are not a complex type that we can drill down further. Send it through because we already know that this entry's parent map was allowed upstream.
                updatedMap.put(mapEntryKey, mapEntryValue);
        }

        return updatedMap;
    }

    private SchemaBuilder convertFieldType(Schema.Type type) {
        switch (type) {
            case INT8:
                return SchemaBuilder.int8();
            case INT16:
                return SchemaBuilder.int16();
            case INT32:
                return SchemaBuilder.int32();
            case INT64:
                return SchemaBuilder.int64();
            case FLOAT32:
                return SchemaBuilder.float32();
            case FLOAT64:
                return SchemaBuilder.float64();
            case BOOLEAN:
                return SchemaBuilder.bool();
            case STRING:
                return SchemaBuilder.string();
            default:
                throw new DataException("Unexpected type in Cast transformation: " + type);
        }
    }

    private static Object encodeLogicalType(Schema schema, Object value, Schema.Type targetType) {
        if (schema != null && schema.name() != null && targetType != Type.STRING) {
            switch (schema.name()) {
                case Date.LOGICAL_NAME:
                    return Date.fromLogical(schema, (java.util.Date) value);
                case Time.LOGICAL_NAME:
                    return Time.fromLogical(schema, (java.util.Date) value);
                case Timestamp.LOGICAL_NAME:
                    return Timestamp.fromLogical(schema, (java.util.Date) value);
                default:
                    return value;
            }
        } else
            return value;
    }

    private Object castValueToType(Schema schema, Object value, Schema.Type targetType) {
        try {
            if (value == null) return null;

            Schema.Type inferredType = getInferredType(schema, value);
            // Ensure the type we are trying to cast from is supported
            validCastType(inferredType, FieldType.INPUT);

            // Perform logical type encoding to their internal representation.
            value = encodeLogicalType(schema, value, targetType);

            switch (targetType) {
                case INT8:
                    return castToInt8(value);
                case INT16:
                    return castToInt16(value);
                case INT32:
                    return castToInt32(value);
                case INT64:
                    return castToInt64(value);
                case FLOAT32:
                    return castToFloat32(value);
                case FLOAT64:
                    return castToFloat64(value);
                case BOOLEAN:
                    return castToBoolean(value);
                case STRING:
                    if (!inferredType.isPrimitive() && isComplexStringJson)
                        return castToJsonString(value, schema);
                    else
                        return castToString(value);
                default:
                    throw new DataException(targetType.toString() + " is not supported in the Cast transformation.");
            }
        } catch (NumberFormatException e) {
            throw new DataException("Value (" + value.toString() + ") was out of range for requested data type", e);
        }
    }

    private static Schema.Type getInferredType(Schema schema, Object value) {
        Schema.Type inferredType = schema == null ? ConnectSchema.schemaType(value.getClass()) :
                schema.type();
        if (inferredType == null) {
            throw new DataException("Cast transformation was passed a value of type " + value.getClass()
                    + " which is not supported by Connect's data API");
        } else 
            return inferredType;
    }

    private static byte castToInt8(Object value) {
        if (value instanceof Number)
            return ((Number) value).byteValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (byte) 1 : (byte) 0;
        else if (value instanceof String)
            return Byte.parseByte((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static short castToInt16(Object value) {
        if (value instanceof Number)
            return ((Number) value).shortValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (short) 1 : (short) 0;
        else if (value instanceof String)
            return Short.parseShort((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static int castToInt32(Object value) {
        if (value instanceof Number)
            return ((Number) value).intValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1 : 0;
        else if (value instanceof String)
            return Integer.parseInt((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static long castToInt64(Object value) {
        if (value instanceof Number)
            return ((Number) value).longValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? (long) 1 : (long) 0;
        else if (value instanceof String)
            return Long.parseLong((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static float castToFloat32(Object value) {
        if (value instanceof Number)
            return ((Number) value).floatValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1.f : 0.f;
        else if (value instanceof String)
            return Float.parseFloat((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static double castToFloat64(Object value) {
        if (value instanceof Number)
            return ((Number) value).doubleValue();
        else if (value instanceof Boolean)
            return ((boolean) value) ? 1. : 0.;
        else if (value instanceof String)
            return Double.parseDouble((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static boolean castToBoolean(Object value) {
        if (value instanceof Number)
            return ((Number) value).longValue() != 0L;
        else if (value instanceof Boolean)
            return (Boolean) value;
        else if (value instanceof String)
            return Boolean.parseBoolean((String) value);
        else
            throw new DataException("Unexpected type in Cast transformation: " + value.getClass());
    }

    private static String castToString(Object value) {
        if (value instanceof java.util.Date) {
            java.util.Date dateValue = (java.util.Date) value;
            return Values.dateFormatFor(dateValue).format(dateValue);
        } else {
            return value.toString();
        }
    }

    private String castToJsonString(Object value, Schema schema) {
        if (schema == null)
            throw new DataException("Schema is required when casting a complex type as a JSON string.");

        // initialise jsonConverter if it has not already been done 
        if (jsonConverter == null) {
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("converter.type", "value");
            converterConfig.put("schemas.enable", false);

            jsonConverter = new JsonConverter();
            jsonConverter.configure(converterConfig);
        }
        // initialise jsonDeserializer if it has not already been done
        if (jsonDeserializer == null) {
            jsonDeserializer = new JsonDeserializer();
        }

        if (value instanceof Struct || value instanceof List<?> || value instanceof Map<?, ?>) {
            byte[] serializedJson = jsonConverter.fromConnectData(null, schema, value);
            JsonNode json = jsonDeserializer.deserialize(null, serializedJson);
            return json.toString();
        } else
            throw new DataException(schema.type().toString() + " is not a supported schema type to cast as a JSON string.");
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private static Map<String, Schema.Type> parseFieldTypes(List<String> mappings) {
        final Map<String, Schema.Type> m = new HashMap<>();
        boolean isWholeValueCast = false;
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length > 2) {
                throw new ConfigException(ConfigName.SPEC, mappings, "Invalid cast mapping: " + mapping);
            }
            if (parts.length == 1) {
                Schema.Type targetType = Schema.Type.valueOf(parts[0].trim().toUpperCase(Locale.ROOT));
                m.put(WHOLE_VALUE_CAST, validCastType(targetType, FieldType.OUTPUT));
                isWholeValueCast = true;
            } else {
                Schema.Type type;
                try {
                    type = Schema.Type.valueOf(parts[1].trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new ConfigException("Invalid type found in casting spec: " + parts[1].trim(), e);
                }
                m.put(parts[0].trim(), validCastType(type, FieldType.OUTPUT));
            }
        }
        if (isWholeValueCast && mappings.size() > 1) {
            throw new ConfigException("Cast transformations that specify a type to cast the entire value to "
                    + "may ony specify a single cast in their spec");
        }
        return m;
    }

    private enum FieldType {
        INPUT, OUTPUT
    }

    private static Schema.Type validCastType(Schema.Type type, FieldType fieldType) {
        switch (fieldType) {
            case INPUT:
                if (!SUPPORTED_CAST_INPUT_TYPES.contains(type)) {
                    throw new DataException("Cast transformation does not support casting from " +
                        type + "; supported types are " + SUPPORTED_CAST_INPUT_TYPES);
                }
                break;
            case OUTPUT:
                if (!SUPPORTED_CAST_OUTPUT_TYPES.contains(type)) {
                    throw new ConfigException("Cast transformation does not support casting to " +
                        type + "; supported types are " + SUPPORTED_CAST_OUTPUT_TYPES);
                }
                break;
        }
        return type;
    }

    public static final class Key<R extends ConnectRecord<R>> extends Cast<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends Cast<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

}
