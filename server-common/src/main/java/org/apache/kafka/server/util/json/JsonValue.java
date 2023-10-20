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
package org.apache.kafka.server.util.json;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Optional;

/**
 * A simple wrapper over Jackson's JsonNode that enables type safe parsing via the `DecodeJson` type
 * class.
 * <br>
 * Typical usage would be something like:
 * <pre><code>
 * // Given a jsonNode containing a parsed JSON:
 * JsonObject jsonObject = JsonValue.apply(jsonNode).asJsonObject();
 * Integer intField = jsonObject.apply("int_field").to(new DecodeJson.DecodeInteger());
 * Optional<Integer> optionLongField = jsonObject.apply("option_long_field").to(DecodeJson.decodeOptional(new DecodeJson.DecodeInteger()));
 * Map<String, Integer> mapStringIntField = jsonObject.apply("map_string_int_field").to(DecodeJson.decodeMap(new DecodeJson.DecodeInteger()));
 * List<String> seqStringField = jsonObject.apply("seq_string_field").to(DecodeJson.decodeList(new DecodeJson.DecodeString()));
 * </code></pre>
 * The `to` method throws an exception if the value cannot be converted to the requested type.
 */

public interface JsonValue {
    JsonNode node();

    default <T> T to(DecodeJson<T> decodeJson) throws JsonMappingException {
        return decodeJson.decode(node());
    }

    /**
     * If this is a JSON object, return an instance of JsonObject. Otherwise, throw a JsonMappingException.
     */
    default JsonObject asJsonObject() throws JsonMappingException {
        return asJsonObjectOptional()
                .orElseThrow(() -> new JsonMappingException(null, String.format("Expected JSON object, received %s", node())));
    }

    /**
     * If this is a JSON object, return a JsonObject wrapped by a `Some`. Otherwise, return None.
     */
    default Optional<JsonObject> asJsonObjectOptional() {
        if (this instanceof JsonObject) {
            return Optional.of((JsonObject) this);
        } else if (node() instanceof ObjectNode) {
            return Optional.of(new JsonObject((ObjectNode) node()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * If this is a JSON array, return an instance of JsonArray. Otherwise, throw a JsonMappingException.
     */
    default JsonArray asJsonArray() throws JsonMappingException {
        return asJsonArrayOptional()
                .orElseThrow(() -> new JsonMappingException(null, String.format("Expected JSON array, received %s", node())));

    }

    /**
     * If this is a JSON array, return a JsonArray wrapped by a `Some`. Otherwise, return None.
     */
    default Optional<JsonArray> asJsonArrayOptional() {
        if (this instanceof JsonArray) {
            return Optional.of((JsonArray) this);
        } else if (node() instanceof ArrayNode) {
            return Optional.of(new JsonArray((ArrayNode) node()));
        } else {
            return Optional.empty();
        }
    }

    static JsonValue apply(JsonNode node) {
        if (node instanceof ObjectNode) {
            return new JsonObject((ObjectNode) node);
        } else if (node instanceof ArrayNode) {
            return new JsonArray((ArrayNode) node);
        } else {
            return new BasicJsonValue(node);
        }
    }

    class BasicJsonValue implements JsonValue {
        protected JsonNode node;
        BasicJsonValue(JsonNode node) {
            this.node = node;
        }

        @Override
        public JsonNode node() {
            return node;
        }
        @Override
        public int hashCode() {
            return node().hashCode();
        }
        @Override
        public boolean equals(Object a) {
            if (a instanceof BasicJsonValue) {
                return node().equals(((BasicJsonValue) a).node());
            }
            return false;
        }
        @Override
        public String toString() {
            return node().toString();
        }
    }
}
