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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.errors.InvalidTopicException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfiguredInternalTopicTest {

    @Test
    public void testConstructorAndGetters() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", "1000");
        topicConfigs.put("message.timestamp.type", "LogAppendTime");

        ConfiguredInternalTopic config = new ConfiguredInternalTopic("test-topic", topicConfigs, Optional.of(3), Optional.of((short) 2));

        assertEquals("test-topic", config.name());
        assertEquals(topicConfigs, config.topicConfigs());
        assertEquals(Optional.of(3), config.numberOfPartitions());
        assertEquals(Optional.of((short) 2), config.replicationFactor());
    }

    @Test
    public void testConstructorWithNullName() {
        assertThrows(NullPointerException.class, () -> new ConfiguredInternalTopic(null, Collections.emptyMap()));
    }

    @Test
    public void testConstructorWithInvalidName() {
        assertThrows(InvalidTopicException.class, () -> new ConfiguredInternalTopic("invalid topic name", Collections.emptyMap()));
    }

    @Test
    public void testConstructorWithNullTopicConfigs() {
        assertThrows(NullPointerException.class, () -> new ConfiguredInternalTopic("test-topic", null));
    }

    @Test
    public void testSetNumberOfPartitions() {
        ConfiguredInternalTopic config = new ConfiguredInternalTopic("test-topic", Collections.emptyMap());
        config.setNumberOfPartitions(3);
        assertEquals(Optional.of(3), config.numberOfPartitions());
    }

    @Test
    public void testSetNumberOfPartitionsInvalid() {
        ConfiguredInternalTopic config = new ConfiguredInternalTopic("test-topic", Collections.emptyMap());
        assertThrows(IllegalArgumentException.class, () -> config.setNumberOfPartitions(0));
    }

    @Test
    public void testSetNumberOfPartitionsUnsupportedOperation() {
        ConfiguredInternalTopic config = new ConfiguredInternalTopic("test-topic", Collections.emptyMap(), Optional.of(3), Optional.empty());
        assertThrows(UnsupportedOperationException.class, () -> config.setNumberOfPartitions(4));
    }

    @Test
    public void testEqualsAndHashCode() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", "1000");

        ConfiguredInternalTopic config1 = new ConfiguredInternalTopic("test-topic", topicConfigs, Optional.of(3), Optional.of((short) 2));
        ConfiguredInternalTopic config2 = new ConfiguredInternalTopic("test-topic", topicConfigs, Optional.of(3), Optional.of((short) 2));

        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testToString() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", "1000");

        ConfiguredInternalTopic config = new ConfiguredInternalTopic("test-topic", topicConfigs, Optional.of(3), Optional.of((short) 2));
        String expectedString = "ConfiguredInternalTopic(name=test-topic, topicConfigs={retention.ms=1000}, numberOfPartitions=Optional[3], replicationFactor=Optional[2], enforceNumberOfPartitions=true)";

        assertEquals(expectedString, config.toString());
    }
}