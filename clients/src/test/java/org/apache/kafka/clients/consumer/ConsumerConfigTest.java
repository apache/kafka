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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ConsumerConfigTest {

    private final Deserializer keyDeserializer = new ByteArrayDeserializer();
    private final Deserializer valueDeserializer = new StringDeserializer();
    private final String keyDeserializerClassName = keyDeserializer.getClass().getName();
    private final String valueDeserializerClassName = valueDeserializer.getClass().getName();
    private final Object keyDeserializerClass = keyDeserializer.getClass();
    private final Object valueDeserializerClass = valueDeserializer.getClass();

    @Test
    public void testDeserializerToPropertyConfig() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        Properties newProperties = ConsumerConfig.addDeserializerToConfig(properties, null, null);
        assertEquals(newProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClassName);
        assertEquals(newProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClassName);

        properties.clear();
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        newProperties = ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, null);
        assertEquals(newProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClassName);
        assertEquals(newProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClassName);

        properties.clear();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        newProperties = ConsumerConfig.addDeserializerToConfig(properties, null, valueDeserializer);
        assertEquals(newProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClassName);
        assertEquals(newProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClassName);

        properties.clear();
        newProperties = ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer);
        assertEquals(newProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClassName);
        assertEquals(newProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClassName);
    }

    @Test
    public void testDeserializerToMapConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        Map<String, Object> newConfigs = ConsumerConfig.addDeserializerToConfig(configs, null, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        newConfigs = ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        newConfigs = ConsumerConfig.addDeserializerToConfig(configs, null, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        newConfigs = ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);
    }
}
