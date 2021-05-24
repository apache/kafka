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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProducerConfigTest {

    private final Serializer<byte[]> keySerializer = new ByteArraySerializer();
    private final Serializer<String> valueSerializer = new StringSerializer();
    private final Object keySerializerClass = keySerializer.getClass();
    private final Object valueSerializerClass = valueSerializer.getClass();

    @Test
    public void testAppendSerializerToConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        Map<String, Object> newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, null);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, null);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, null, valueSerializer);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);

        configs.clear();
        newConfigs = ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer);
        assertEquals(newConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClass);
        assertEquals(newConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClass);
    }
}
