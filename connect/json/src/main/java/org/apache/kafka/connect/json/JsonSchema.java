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
package org.apache.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSchema {

    static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";
    static final String SCHEMA_TYPE_FIELD_NAME = "type";
    static final String SCHEMA_OPTIONAL_FIELD_NAME = "optional";
    static final String SCHEMA_NAME_FIELD_NAME = "name";
    static final String SCHEMA_VERSION_FIELD_NAME = "version";
    static final String SCHEMA_DOC_FIELD_NAME = "doc";
    static final String SCHEMA_PARAMETERS_FIELD_NAME = "parameters";
    static final String SCHEMA_DEFAULT_FIELD_NAME = "default";
    static final String ARRAY_ITEMS_FIELD_NAME = "items";
    static final String MAP_KEY_FIELD_NAME = "keys";
    static final String MAP_VALUE_FIELD_NAME = "values";
    static final String STRUCT_FIELDS_FIELD_NAME = "fields";
    static final String STRUCT_FIELD_NAME_FIELD_NAME = "field";
    static final String BOOLEAN_TYPE_NAME = "boolean";
    static final ObjectNode BOOLEAN_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BOOLEAN_TYPE_NAME);
    static final String INT8_TYPE_NAME = "int8";
    static final ObjectNode INT8_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT8_TYPE_NAME);
    static final String INT16_TYPE_NAME = "int16";
    static final ObjectNode INT16_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT16_TYPE_NAME);
    static final String INT32_TYPE_NAME = "int32";
    static final ObjectNode INT32_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT32_TYPE_NAME);
    static final String INT64_TYPE_NAME = "int64";
    static final ObjectNode INT64_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT64_TYPE_NAME);
    static final String FLOAT_TYPE_NAME = "float";
    static final ObjectNode FLOAT_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, FLOAT_TYPE_NAME);
    static final String DOUBLE_TYPE_NAME = "double";
    static final ObjectNode DOUBLE_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, DOUBLE_TYPE_NAME);
    static final String BYTES_TYPE_NAME = "bytes";
    static final ObjectNode BYTES_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BYTES_TYPE_NAME);
    static final String STRING_TYPE_NAME = "string";
    static final ObjectNode STRING_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, STRING_TYPE_NAME);
    static final String ARRAY_TYPE_NAME = "array";
    static final String MAP_TYPE_NAME = "map";
    static final String STRUCT_TYPE_NAME = "struct";

    public static ObjectNode envelope(JsonNode schema, JsonNode payload) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set(ENVELOPE_SCHEMA_FIELD_NAME, schema);
        result.set(ENVELOPE_PAYLOAD_FIELD_NAME, payload);
        return result;
    }

    static class Envelope {
        public JsonNode schema;
        public JsonNode payload;

        public Envelope(JsonNode schema, JsonNode payload) {
            this.schema = schema;
            this.payload = payload;
        }

        public ObjectNode toJsonNode() {
            return envelope(schema, payload);
        }
    }
}
