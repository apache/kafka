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

package org.apache.kafka.copycat.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.copycat.data.*;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.storage.Converter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Implementation of Converter that uses JSON to store schemas and objects.
 */
public class JsonConverter implements Converter<JsonNode> {

    private static final HashMap<String, JsonToCopycatTypeConverter> TO_COPYCAT_CONVERTERS
            = new HashMap<>();

    static {
        TO_COPYCAT_CONVERTERS.put(JsonSchema.BOOLEAN_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.booleanValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.INT_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.intValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.LONG_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.longValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.FLOAT_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.floatValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.DOUBLE_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.doubleValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.BYTES_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                try {
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new CopycatException("Invalid bytes field", e);
                }
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.STRING_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                return value.textValue();
            }
        });
        TO_COPYCAT_CONVERTERS.put(JsonSchema.ARRAY_TYPE_NAME, new JsonToCopycatTypeConverter() {
            @Override
            public Object convert(JsonNode jsonSchema, JsonNode value) {
                JsonNode elemSchema = jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (elemSchema == null)
                    throw new CopycatException("Array schema did not specify the element type");
                ArrayList<Object> result = new ArrayList<>();
                for (JsonNode elem : value) {
                    result.add(convertToCopycat(elemSchema, elem));
                }
                return result;
            }
        });

    }

    @Override
    public JsonNode fromCopycatData(Object value) {
        return convertToJsonWithSchemaEnvelope(value);
    }

    @Override
    public Object toCopycatData(JsonNode value) {
        if (!value.isObject() || value.size() != 2 || !value.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME) || !value.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME))
            throw new CopycatException("JSON value converted to Copycat must be in envelope containing schema");

        return convertToCopycat(value.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME), value.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }


    private static JsonNode asJsonSchema(Schema schema) {
        switch (schema.getType()) {
            case BOOLEAN:
                return JsonSchema.BOOLEAN_SCHEMA;
            case BYTES:
                return JsonSchema.BYTES_SCHEMA;
            case DOUBLE:
                return JsonSchema.DOUBLE_SCHEMA;
            case FLOAT:
                return JsonSchema.FLOAT_SCHEMA;
            case INT:
                return JsonSchema.INT_SCHEMA;
            case LONG:
                return JsonSchema.LONG_SCHEMA;
            case NULL:
                throw new UnsupportedOperationException("null schema not supported");
            case STRING:
                return JsonSchema.STRING_SCHEMA;
            case UNION: {
                throw new UnsupportedOperationException("union schema not supported");
            }
            case ARRAY:
                return JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME)
                        .set(JsonSchema.ARRAY_ITEMS_FIELD_NAME, asJsonSchema(schema.getElementType()));
            case ENUM:
                throw new UnsupportedOperationException("enum schema not supported");
            case MAP:
                throw new UnsupportedOperationException("map schema not supported");
            default:
                throw new CopycatException("Couldn't translate unsupported schema type " + schema.getType().getName() + ".");
        }
    }


    private static Schema asCopycatSchema(JsonNode jsonSchema) {
        if (jsonSchema.isNull())
            return null;

        JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new CopycatException("Schema must contain 'type' field");

        switch (schemaTypeNode.textValue()) {
            case JsonSchema.BOOLEAN_TYPE_NAME:
                return SchemaBuilder.builder().booleanType();
            case JsonSchema.INT_TYPE_NAME:
                return SchemaBuilder.builder().intType();
            case JsonSchema.LONG_TYPE_NAME:
                return SchemaBuilder.builder().longType();
            case JsonSchema.FLOAT_TYPE_NAME:
                return SchemaBuilder.builder().floatType();
            case JsonSchema.DOUBLE_TYPE_NAME:
                return SchemaBuilder.builder().doubleType();
            case JsonSchema.BYTES_TYPE_NAME:
                return SchemaBuilder.builder().bytesType();
            case JsonSchema.STRING_TYPE_NAME:
                return SchemaBuilder.builder().stringType();
            case JsonSchema.ARRAY_TYPE_NAME:
                JsonNode elemSchema = jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (elemSchema == null)
                    throw new CopycatException("Array schema did not specify the element type");
                return Schema.createArray(asCopycatSchema(elemSchema));
            default:
                throw new CopycatException("Unknown schema type: " + schemaTypeNode.textValue());
        }
    }


    /**
     * Convert this object, in org.apache.kafka.copycat.data format, into a JSON object with an envelope object
     * containing schema and payload fields.
     * @param value
     * @return
     */
    private static JsonNode convertToJsonWithSchemaEnvelope(Object value) {
        return convertToJson(value).toJsonNode();
    }

    /**
     * Convert this object, in the org.apache.kafka.copycat.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private static JsonSchema.Envelope convertToJson(Object value) {
        if (value == null) {
            return JsonSchema.nullEnvelope();
        } else if (value instanceof Boolean) {
            return JsonSchema.booleanEnvelope((Boolean) value);
        } else if (value instanceof Byte) {
            return JsonSchema.intEnvelope((Byte) value);
        } else if (value instanceof Short) {
            return JsonSchema.intEnvelope((Short) value);
        } else if (value instanceof Integer) {
            return JsonSchema.intEnvelope((Integer) value);
        } else if (value instanceof Long) {
            return JsonSchema.longEnvelope((Long) value);
        } else if (value instanceof Float) {
            return JsonSchema.floatEnvelope((Float) value);
        } else if (value instanceof Double) {
            return JsonSchema.doubleEnvelope((Double) value);
        } else if (value instanceof byte[]) {
            return JsonSchema.bytesEnvelope((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return JsonSchema.bytesEnvelope(((ByteBuffer) value).array());
        } else if (value instanceof CharSequence) {
            return JsonSchema.stringEnvelope(value.toString());
        } else if (value instanceof Collection) {
            Collection collection = (Collection) value;
            ObjectNode schema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME);
            ArrayNode list = JsonNodeFactory.instance.arrayNode();
            JsonNode itemSchema = null;
            for (Object elem : collection) {
                JsonSchema.Envelope fieldSchemaAndValue = convertToJson(elem);
                if (itemSchema == null) {
                    itemSchema = fieldSchemaAndValue.schema;
                    schema.set(JsonSchema.ARRAY_ITEMS_FIELD_NAME, itemSchema);
                } else {
                    if (!itemSchema.equals(fieldSchemaAndValue.schema))
                        throw new CopycatException("Mismatching schemas found in a list.");
                }

                list.add(fieldSchemaAndValue.payload);
            }
            return new JsonSchema.Envelope(schema, list);
        }

        throw new CopycatException("Couldn't convert " + value + " to Avro.");
    }


    private static Object convertToCopycat(JsonNode jsonSchema, JsonNode jsonValue) {
        if (jsonSchema.isNull())
            return null;

        JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new CopycatException("Schema must contain 'type' field. Schema: " + jsonSchema.toString());

        JsonToCopycatTypeConverter typeConverter = TO_COPYCAT_CONVERTERS.get(schemaTypeNode.textValue());
        if (typeConverter != null)
            return typeConverter.convert(jsonSchema, jsonValue);

        throw new CopycatException("Unknown schema type: " + schemaTypeNode);
    }


    private interface JsonToCopycatTypeConverter {
        Object convert(JsonNode schema, JsonNode value);
    }
}
