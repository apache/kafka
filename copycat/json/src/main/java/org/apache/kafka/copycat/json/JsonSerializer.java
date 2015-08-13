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
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serialize Jackson JsonNode tree model objects to UTF-8 JSON. Using the tree model allows handling arbitrarily
 * structured data without corresponding Java classes. This serializer also supports Copycat schemas.
 */
public class JsonSerializer implements Serializer<JsonNode> {

    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    private static final boolean SCHEMAS_ENABLE_DEFAULT = true;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean enableSchemas = SCHEMAS_ENABLE_DEFAULT;

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializer() {

    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        Object enableConfigsVal = config.get(SCHEMAS_ENABLE_CONFIG);
        if (enableConfigsVal != null)
            enableSchemas = enableConfigsVal.toString().equals("true");
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        // This serializer works for Copycat data that requires a schema to be included, so we expect it to have a
        // specific format: { "schema": {...}, "payload": ... }.
        if (!data.isObject() || data.size() != 2 || !data.has("schema") || !data.has("payload"))
            throw new SerializationException("JsonSerializer requires \"schema\" and \"payload\" fields and may not contain additional fields");

        try {
            if (!enableSchemas)
                data = data.get("payload");
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
