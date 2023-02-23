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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.Set;

/**
 * Serialize Jackson {@link JsonNode} tree model objects to UTF-8 JSON. Using the tree model allows handling arbitrarily
 * structured data without corresponding Java classes. This serializer also supports Connect schemas.
 */
public class JsonSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    /**
     * A constructor that additionally specifies some {@link SerializationFeature}s
     * for the serializer
     *
     * @param serializationFeatures the specified serialization features
     * @param jsonNodeFactory the json node factory to use.
     */
    JsonSerializer(
        final Set<SerializationFeature> serializationFeatures,
        final JsonNodeFactory jsonNodeFactory
    ) {
        serializationFeatures.forEach(objectMapper::enable);
        objectMapper.setNodeFactory(jsonNodeFactory);
    }

    @Override
    public byte[] serialize(String topic, JsonNode data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
}
