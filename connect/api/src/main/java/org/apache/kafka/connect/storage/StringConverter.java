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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link Converter} implementation that only supports serializing to strings. When converting Kafka Connect data to bytes,
 * the schema will be ignored and {@link Object#toString()} will always be invoked to convert the data to a String.
 * When converting from bytes to Kafka Connect format, the converter will only ever return an optional string schema and
 * a string or null.
 *
 * Encoding configuration is identical to {@link StringSerializer} and {@link StringDeserializer}, but for convenience
 * this class can also be configured to use the same encoding for both encoding and decoding with the converter.encoding
 * setting.
 */
public class StringConverter implements Converter {
    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    public StringConverter() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.putAll(configs);
        Map<String, Object> deserializerConfigs = new HashMap<>();
        deserializerConfigs.putAll(configs);

        Object encodingValue = configs.get("converter.encoding");
        if (encodingValue != null) {
            serializerConfigs.put("serializer.encoding", encodingValue);
            deserializerConfigs.put("deserializer.encoding", encodingValue);
        }

        serializer.configure(serializerConfigs, isKey);
        deserializer.configure(deserializerConfigs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return serializer.serialize(topic, value == null ? null : value.toString());
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, deserializer.deserialize(topic, value));
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize string: ", e);
        }
    }
}
