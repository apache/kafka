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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * JSON deserializer for Jackson's JsonNode tree model. Using the tree model allows it to work with arbitrarily
 * structured data without having associated Java classes. This deserializer also supports Connect schemas.
 */
public class JsonDeserializer implements Deserializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonDeserializer() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    /**
     * A constructor that additionally specifies some {@link DeserializationFeature}
     * for the deserializer
     *
     * @param deserializationFeatures the specified deserialization features
     * @param jsonNodeFactory the json node factory to use.
     */
    JsonDeserializer(
        final Set<DeserializationFeature> deserializationFeatures,
        final JsonNodeFactory jsonNodeFactory
    ) {
        deserializationFeatures.forEach(objectMapper::enable);
        objectMapper.setNodeFactory(jsonNodeFactory);
    }

    @Override
    public JsonNode deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        JsonNode data;
        try {
            data = objectMapper.readTree(bytes);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
}
