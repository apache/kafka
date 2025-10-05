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
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfiguredInternalTopicTest {

    @Test
    public void testConstructorWithNullName() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredInternalTopic(null, 1, Optional.empty(), Map.of()));
    }

    @Test
    public void testConstructorWithInvalidName() {
        assertThrows(InvalidTopicException.class,
            () -> new ConfiguredInternalTopic("invalid topic name", 1, Optional.empty(), Map.of()));
    }

    @Test
    public void testConstructorWithNullTopicConfigs() {
        assertThrows(NullPointerException.class,
            () -> new ConfiguredInternalTopic("test-topic", 1, Optional.empty(), null));
    }

    @Test
    public void testConstructorWithZeroPartitions() {
        assertThrows(IllegalArgumentException.class,
            () -> new ConfiguredInternalTopic("test-topic", 0, Optional.empty(), Map.of()));
    }

    @Test
    public void testAsStreamsGroupDescribeTopicInfo() {
        String topicName = "test-topic";
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put("retention.ms", "1000");
        int numberOfPartitions = 3;
        Optional<Short> replicationFactor = Optional.of((short) 2);
        ConfiguredInternalTopic configuredInternalTopic = new ConfiguredInternalTopic(
            topicName, numberOfPartitions, replicationFactor, topicConfigs);

        StreamsGroupDescribeResponseData.TopicInfo topicInfo = configuredInternalTopic.asStreamsGroupDescribeTopicInfo();

        assertEquals(topicName, topicInfo.name());
        assertEquals(numberOfPartitions, topicInfo.partitions());
        assertEquals(replicationFactor.orElse((short) 0).shortValue(), topicInfo.replicationFactor());
        assertEquals(1, topicInfo.topicConfigs().size());
        assertEquals("1000", topicInfo.topicConfigs().get(0).value());
    }
}