/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of Converter that uses JSON to store schemas and objects.
 */
public class JsonConverter implements Converter {
    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    private static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    private static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;

    private static final HashMap<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new HashMap<>();

    private static Object checkOptionalAndDefault(Schema schema) {
        if (schema.defaultValue() != null)
            return schema.defaultValue();
        if (schema.isOptional())
            return null;
        throw new DataException("Invalid null value for required field");
    }

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.booleanValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return (byte) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return (short) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.longValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.floatValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.doubleValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                try {
                    if (value.isNull()) return checkOptionalAndDefault(schema);
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Invalid bytes field", e);
                }
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);
                return value.textValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                Schema elemSchema = schema == null ? null : schema.valueSchema();
                ArrayList<Object> result = new ArrayList<>();
                for (JsonNode elem : value) {
                    result.add(convertToConnect(elemSchema, elem));
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();

                // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
                // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
                // schema, we default to encoding in a Map.
                Map<Object, Object> result = new HashMap<>();
                if (schema == null || keySchema.type() == Schema.Type.STRING) {
                    if (!value.isObject())
                        throw new DataException("Map's with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fieldIt.next();
                        result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
                    }
                } else {
                    if (!value.isArray())
                        throw new DataException("Map's with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    for (JsonNode entry : value) {
                        if (!entry.isArray())
                            throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                        if (entry.size() != 2)
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                        result.put(convertToConnect(keySchema, entry.get(0)),
                                convertToConnect(valueSchema, entry.get(1)));
                    }
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (value.isNull()) return checkOptionalAndDefault(schema);

                if (!value.isObject())
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

                // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
                // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
                // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
                // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
                // just returns the schema Object and has no overhead.
                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields())
                    result.put(field, convertToConnect(field.schema(), value.get(field.name())));

                return result;
            }
        });
    }

    // Convert values in Kafka Connect form into their logical types. These logical converters are discovered by logical type
    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();
    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof byte[]))
                    throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
                return Decimal.toLogical(schema, (byte[]) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
                return Date.toLogical(schema, (int) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
                return Time.toLogical(schema, (int) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
                return Timestamp.toLogical(schema, (long) value);
            }
        });
    }

    private static final HashMap<String, LogicalTypeConverter> TO_JSON_LOGICAL_CONVERTERS = new HashMap<>();
    static {
        TO_JSON_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof BigDecimal))
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                return Decimal.fromLogical(schema, (BigDecimal) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                return Date.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                return Time.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                return Timestamp.fromLogical(schema, (java.util.Date) value);
            }
        });
    }


    private boolean enableSchemas = SCHEMAS_ENABLE_DEFAULT;
    private int cacheSize = SCHEMAS_CACHE_SIZE_DEFAULT;
    private Cache<Schema, ObjectNode> fromConnectSchemaCache;
    private Cache<JsonNode, Schema> toConnectSchemaCache;

    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object enableConfigsVal = configs.get(SCHEMAS_ENABLE_CONFIG);
        if (enableConfigsVal != null)
            enableSchemas = enableConfigsVal.toString().equals("true");

        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);

        Object cacheSizeVal = configs.get(SCHEMAS_CACHE_SIZE_CONFIG);
        if (cacheSizeVal != null)
            cacheSize = (int) cacheSizeVal;
        fromConnectSchemaCache = new SynchronizedCache<>(new LRUCache<Schema, ObjectNode>(cacheSize));
        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<JsonNode, Schema>(cacheSize));
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        JsonNode jsonValue = enableSchemas ? convertToJsonWithEnvelope(schema, value) : convertToJsonWithoutEnvelope(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        if (enableSchemas && (jsonValue == null || !jsonValue.isObject() || jsonValue.size() != 2 || !jsonValue.has("schema") || !jsonValue.has("payload")))
            throw new DataException("JsonDeserializer with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields");

        // The deserialized data should either be an envelope object containing the schema and the payload or the schema
        // was stripped during serialization and we need to fill in an all-encompassing schema.
        if (!enableSchemas) {
            ObjectNode envelope = JsonNodeFactory.instance.objectNode();
            envelope.set("schema", null);
            envelope.set("payload", jsonValue);
            jsonValue = envelope;
        }

        return jsonToConnect(jsonValue);
    }

    private SchemaAndValue jsonToConnect(JsonNode jsonValue) {
        if (jsonValue == null)
            return SchemaAndValue.NULL;

        if (!jsonValue.isObject() || jsonValue.size() != 2 || !jsonValue.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME) || !jsonValue.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME))
            throw new DataException("JSON value converted to Kafka Connect must be in envelope containing schema");

        Schema schema = asConnectSchema(jsonValue.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        return new SchemaAndValue(schema, convertToConnect(schema, jsonValue.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)));
    }

    private ObjectNode asJsonSchema(Schema schema) {
        if (schema == null)
            return null;

        ObjectNode cached = fromConnectSchemaCache.get(schema);
        if (cached != null)
            return cached;

        final ObjectNode jsonSchema;
        switch (schema.type()) {
            case BOOLEAN:
                jsonSchema = JsonSchema.BOOLEAN_SCHEMA.deepCopy();
                break;
            case BYTES:
                jsonSchema = JsonSchema.BYTES_SCHEMA.deepCopy();
                break;
            case FLOAT64:
                jsonSchema = JsonSchema.DOUBLE_SCHEMA.deepCopy();
                break;
            case FLOAT32:
                jsonSchema = JsonSchema.FLOAT_SCHEMA.deepCopy();
                break;
            case INT8:
                jsonSchema = JsonSchema.INT8_SCHEMA.deepCopy();
                break;
            case INT16:
                jsonSchema = JsonSchema.INT16_SCHEMA.deepCopy();
                break;
            case INT32:
                jsonSchema = JsonSchema.INT32_SCHEMA.deepCopy();
                break;
            case INT64:
                jsonSchema = JsonSchema.INT64_SCHEMA.deepCopy();
                break;
            case STRING:
                jsonSchema = JsonSchema.STRING_SCHEMA.deepCopy();
                break;
            case ARRAY:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME);
                jsonSchema.set(JsonSchema.ARRAY_ITEMS_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                break;
            case MAP:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.MAP_TYPE_NAME);
                jsonSchema.set(JsonSchema.MAP_KEY_FIELD_NAME, asJsonSchema(schema.keySchema()));
                jsonSchema.set(JsonSchema.MAP_VALUE_FIELD_NAME, asJsonSchema(schema.valueSchema()));
                break;
            case STRUCT:
                jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.STRUCT_TYPE_NAME);
                ArrayNode fields = JsonNodeFactory.instance.arrayNode();
                for (Field field : schema.fields()) {
                    ObjectNode fieldJsonSchema = asJsonSchema(field.schema()).deepCopy();
                    fieldJsonSchema.put(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME, field.name());
                    fields.add(fieldJsonSchema);
                }
                jsonSchema.set(JsonSchema.STRUCT_FIELDS_FIELD_NAME, fields);
                break;
            default:
                throw new DataException("Couldn't translate unsupported schema type " + schema + ".");
        }

        jsonSchema.put(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME, schema.isOptional());
        if (schema.name() != null)
            jsonSchema.put(JsonSchema.SCHEMA_NAME_FIELD_NAME, schema.name());
        if (schema.version() != null)
            jsonSchema.put(JsonSchema.SCHEMA_VERSION_FIELD_NAME, schema.version());
        if (schema.doc() != null)
            jsonSchema.put(JsonSchema.SCHEMA_DOC_FIELD_NAME, schema.doc());
        if (schema.parameters() != null) {
            ObjectNode jsonSchemaParams = JsonNodeFactory.instance.objectNode();
            for (Map.Entry<String, String> prop : schema.parameters().entrySet())
                jsonSchemaParams.put(prop.getKey(), prop.getValue());
            jsonSchema.put(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME, jsonSchemaParams);
        }
        if (schema.defaultValue() != null)
            jsonSchema.set(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME, convertToJson(schema, schema.defaultValue()));

        fromConnectSchemaCache.put(schema, jsonSchema);
        return jsonSchema;
    }


    private Schema asConnectSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull())
            return null;

        Schema cached = toConnectSchemaCache.get(jsonSchema);
        if (cached != null)
            return cached;

        JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new DataException("Schema must contain 'type' field");

        final SchemaBuilder builder;
        switch (schemaTypeNode.textValue()) {
            case JsonSchema.BOOLEAN_TYPE_NAME:
                builder = SchemaBuilder.bool();
                break;
            case JsonSchema.INT8_TYPE_NAME:
                builder = SchemaBuilder.int8();
                break;
            case JsonSchema.INT16_TYPE_NAME:
                builder = SchemaBuilder.int16();
                break;
            case JsonSchema.INT32_TYPE_NAME:
                builder = SchemaBuilder.int32();
                break;
            case JsonSchema.INT64_TYPE_NAME:
                builder = SchemaBuilder.int64();
                break;
            case JsonSchema.FLOAT_TYPE_NAME:
                builder = SchemaBuilder.float32();
                break;
            case JsonSchema.DOUBLE_TYPE_NAME:
                builder = SchemaBuilder.float64();
                break;
            case JsonSchema.BYTES_TYPE_NAME:
                builder = SchemaBuilder.bytes();
                break;
            case JsonSchema.STRING_TYPE_NAME:
                builder = SchemaBuilder.string();
                break;
            case JsonSchema.ARRAY_TYPE_NAME:
                JsonNode elemSchema = jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (elemSchema == null)
                    throw new DataException("Array schema did not specify the element type");
                builder = SchemaBuilder.array(asConnectSchema(elemSchema));
                break;
            case JsonSchema.MAP_TYPE_NAME:
                JsonNode keySchema = jsonSchema.get(JsonSchema.MAP_KEY_FIELD_NAME);
                if (keySchema == null)
                    throw new DataException("Map schema did not specify the key type");
                JsonNode valueSchema = jsonSchema.get(JsonSchema.MAP_VALUE_FIELD_NAME);
                if (valueSchema == null)
                    throw new DataException("Map schema did not specify the value type");
                builder = SchemaBuilder.map(asConnectSchema(keySchema), asConnectSchema(valueSchema));
                break;
            case JsonSchema.STRUCT_TYPE_NAME:
                builder = SchemaBuilder.struct();
                JsonNode fields = jsonSchema.get(JsonSchema.STRUCT_FIELDS_FIELD_NAME);
                if (fields == null || !fields.isArray())
                    throw new DataException("Struct schema's \"fields\" argument is not an array.");
                for (JsonNode field : fields) {
                    JsonNode jsonFieldName = field.get(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME);
                    if (jsonFieldName == null || !jsonFieldName.isTextual())
                        throw new DataException("Struct schema's field name not specified properly");
                    builder.field(jsonFieldName.asText(), asConnectSchema(field));
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
        }


        JsonNode schemaOptionalNode = jsonSchema.get(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME);
        if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue())
            builder.optional();
        else
            builder.required();

        JsonNode schemaNameNode = jsonSchema.get(JsonSchema.SCHEMA_NAME_FIELD_NAME);
        if (schemaNameNode != null && schemaNameNode.isTextual())
            builder.name(schemaNameNode.textValue());

        JsonNode schemaVersionNode = jsonSchema.get(JsonSchema.SCHEMA_VERSION_FIELD_NAME);
        if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
            builder.version(schemaVersionNode.intValue());
        }

        JsonNode schemaDocNode = jsonSchema.get(JsonSchema.SCHEMA_DOC_FIELD_NAME);
        if (schemaDocNode != null && schemaDocNode.isTextual())
            builder.doc(schemaDocNode.textValue());

        JsonNode schemaParamsNode = jsonSchema.get(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME);
        if (schemaParamsNode != null && schemaParamsNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> paramsIt = schemaParamsNode.fields();
            while (paramsIt.hasNext()) {
                Map.Entry<String, JsonNode> entry = paramsIt.next();
                JsonNode paramValue = entry.getValue();
                if (!paramValue.isTextual())
                    throw new DataException("Schema parameters must have string values.");
                builder.parameter(entry.getKey(), paramValue.textValue());
            }
        }

        JsonNode schemaDefaultNode = jsonSchema.get(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME);
        if (schemaDefaultNode != null)
            builder.defaultValue(convertToConnect(builder, schemaDefaultNode));

        Schema result = builder.build();
        toConnectSchemaCache.put(jsonSchema, result);
        return result;
    }


    /**
     * Convert this object, in org.apache.kafka.connect.data format, into a JSON object with an envelope object
     * containing schema and payload fields.
     * @param schema the schema for the data
     * @param value the value
     * @return JsonNode-encoded version
     */
    private JsonNode convertToJsonWithEnvelope(Schema schema, Object value) {
        return new JsonSchema.Envelope(asJsonSchema(schema), convertToJson(schema, value)).toJsonNode();
    }

    private JsonNode convertToJsonWithoutEnvelope(Schema schema, Object value) {
        return convertToJson(schema, value);
    }

    /**
     * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private static JsonNode convertToJson(Schema schema, Object logicalValue) {
        if (logicalValue == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JsonNodeFactory.instance.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        Object value = logicalValue;
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_JSON_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                value = logicalConverter.convert(schema, logicalValue);
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode((Byte) value);
                case INT16:
                    return JsonNodeFactory.instance.numberNode((Short) value);
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) value);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) value);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) value);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) value);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JsonNodeFactory.instance.textNode(charSeq.toString());
                case BYTES:
                    if (value instanceof byte[])
                        return JsonNodeFactory.instance.binaryNode((byte[]) value);
                    else if (value instanceof ByteBuffer)
                        return JsonNodeFactory.instance.binaryNode(((ByteBuffer) value).array());
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection collection = (Collection) value;
                    ArrayNode list = JsonNodeFactory.instance.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode fieldValue = convertToJson(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING;
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode)
                        obj = JsonNodeFactory.instance.objectNode();
                    else
                        list = JsonNodeFactory.instance.arrayNode();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode mapKey = convertToJson(keySchema, entry.getKey());
                        JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

                        if (objectMode)
                            obj.set(mapKey.asText(), mapValue);
                        else
                            list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (struct.schema() != schema)
                        throw new DataException("Mismatching schema.");
                    ObjectNode obj = JsonNodeFactory.instance.objectNode();
                    for (Field field : schema.fields()) {
                        obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
                    }
                    return obj;
                }
            }

            throw new DataException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
        }
    }


    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        JsonToConnectTypeConverter typeConverter;
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
        } else {
            switch (jsonValue.getNodeType()) {
                case NULL:
                    // Special case. With no schema
                    return null;
                case BOOLEAN:
                    schemaType = Schema.Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber())
                        schemaType = Schema.Type.INT64;
                    else
                        schemaType = Schema.Type.FLOAT64;
                    break;
                case ARRAY:
                    schemaType = Schema.Type.ARRAY;
                    break;
                case OBJECT:
                    schemaType = Schema.Type.MAP;
                    break;
                case STRING:
                    schemaType = Schema.Type.STRING;
                    break;

                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
                    break;
            }
        }
        typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + schema.type());

        Object converted = typeConverter.convert(schema, jsonValue);
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                converted = logicalConverter.convert(schema, converted);
        }
        return converted;
    }


    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    private interface LogicalTypeConverter {
        Object convert(Schema schema, Object value);
    }
}
