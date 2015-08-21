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

package org.apache.kafka.copycat.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;

public class JsonSchema {

    static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";
    static final String SCHEMA_TYPE_FIELD_NAME = "type";
    static final String SCHEMA_NAME_FIELD_NAME = "name";
    static final String ARRAY_ITEMS_FIELD_NAME = "items";
    static final String BOOLEAN_TYPE_NAME = "boolean";
    static final JsonNode BOOLEAN_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BOOLEAN_TYPE_NAME);
    static final String INT_TYPE_NAME = "int";
    static final JsonNode INT_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, INT_TYPE_NAME);
    static final String LONG_TYPE_NAME = "long";
    static final JsonNode LONG_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, LONG_TYPE_NAME);
    static final String FLOAT_TYPE_NAME = "float";
    static final JsonNode FLOAT_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, FLOAT_TYPE_NAME);
    static final String DOUBLE_TYPE_NAME = "double";
    static final JsonNode DOUBLE_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, DOUBLE_TYPE_NAME);
    static final String BYTES_TYPE_NAME = "bytes";
    static final JsonNode BYTES_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, BYTES_TYPE_NAME);
    static final String STRING_TYPE_NAME = "string";
    static final JsonNode STRING_SCHEMA = JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE_FIELD_NAME, STRING_TYPE_NAME);
    static final String ARRAY_TYPE_NAME = "array";

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


    public static Envelope nullEnvelope() {
        return new Envelope(null, null);
    }

    public static Envelope booleanEnvelope(boolean value) {
        return new Envelope(JsonSchema.BOOLEAN_SCHEMA, JsonNodeFactory.instance.booleanNode(value));
    }

    public static Envelope intEnvelope(byte value) {
        return new Envelope(JsonSchema.INT_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope intEnvelope(short value) {
        return new Envelope(JsonSchema.INT_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope intEnvelope(int value) {
        return new Envelope(JsonSchema.INT_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope longEnvelope(long value) {
        return new Envelope(JsonSchema.LONG_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope floatEnvelope(float value) {
        return new Envelope(JsonSchema.FLOAT_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope doubleEnvelope(double value) {
        return new Envelope(JsonSchema.DOUBLE_SCHEMA, JsonNodeFactory.instance.numberNode(value));
    }

    public static Envelope bytesEnvelope(byte[] value) {
        return new Envelope(JsonSchema.BYTES_SCHEMA, JsonNodeFactory.instance.binaryNode(value));
    }

    public static Envelope bytesEnvelope(ByteBuffer value) {
        return new Envelope(JsonSchema.BYTES_SCHEMA, JsonNodeFactory.instance.binaryNode(value.array()));
    }

    public static Envelope stringEnvelope(CharSequence value) {
        return new Envelope(JsonSchema.STRING_SCHEMA, JsonNodeFactory.instance.textNode(value.toString()));
    }
}
