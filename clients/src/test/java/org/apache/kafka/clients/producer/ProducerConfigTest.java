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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ProducerConfigTest {

    private final Serializer<byte[]> keySerializer = new ByteArraySerializer();
    private final Serializer<String> valueSerializer = new StringSerializer();
    private final String keySerializerClassName = keySerializer.getClass().getName();
    private final String valueSerializerClassName = valueSerializer.getClass().getName();
    private final Object keySerializerClass = keySerializer.getClass();
    private final Object valueSerializerClass = valueSerializer.getClass();

    @SuppressWarnings("deprecation")
    @Test
    public void testSerializerToPropertyConfig() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        Properties newProperties = ProducerConfig.addSerializerToConfig(properties, null, null);
        assertEquals(newProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClassName);
        assertEquals(newProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClassName);

        properties.clear();
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        newProperties = ProducerConfig.addSerializerToConfig(properties, keySerializer, null);
        assertEquals(newProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClassName);
        assertEquals(newProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClassName);

        properties.clear();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        newProperties = ProducerConfig.addSerializerToConfig(properties, null, valueSerializer);
        assertEquals(newProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClassName);
        assertEquals(newProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClassName);

        properties.clear();
        newProperties = ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer);
        assertEquals(newProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), keySerializerClassName);
        assertEquals(newProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), valueSerializerClassName);
    }
    
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
