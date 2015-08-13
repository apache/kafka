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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * JSON deserializer for Jackson's JsonNode tree model. Using the tree model allows it to work with arbitrarily
 * structured data without having associated Java classes. This deserializer also supports Copycat schemas.
 */
public class JsonDeserializer implements Deserializer<JsonNode> {
    private static final ObjectNode CATCH_ALL_OBJECT_SCHEMA = JsonNodeFactory.instance.objectNode();
    private static final ObjectNode CATCH_ALL_ARRAY_SCHEMA = JsonNodeFactory.instance.objectNode();
    private static final ArrayNode ALL_SCHEMAS_LIST = JsonNodeFactory.instance.arrayNode();
    private static final ObjectNode CATCH_ALL_SCHEMA = JsonNodeFactory.instance.objectNode();
    static {
        CATCH_ALL_OBJECT_SCHEMA.put("type", "object")
                .putArray("field").add(JsonNodeFactory.instance.objectNode().put("*", "all"));

        CATCH_ALL_ARRAY_SCHEMA.put("type", "array").put("items", "all");

        ALL_SCHEMAS_LIST.add("boolean").add("int").add("long").add("float").add("double").add("bytes").add("string")
                .add(CATCH_ALL_ARRAY_SCHEMA).add(CATCH_ALL_OBJECT_SCHEMA);

        CATCH_ALL_SCHEMA.put("name", "all").set("type", ALL_SCHEMAS_LIST);
    }

    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        JsonNode data;
        try {
            data = objectMapper.readTree(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        // The deserialized data should either be an envelope object containing the schema and the payload or the schema
        // was stripped during serialization and we need to fill in an all-encompassing schema.
        if (!data.isObject() || data.size() != 2 || !data.has("schema") || !data.has("payload")) {
            ObjectNode envelope = JsonNodeFactory.instance.objectNode();
            envelope.set("schema", CATCH_ALL_SCHEMA);
            envelope.set("payload", data);
            data = envelope;
        }

        return data;
    }

    @Override
    public void close() {

    }
}
