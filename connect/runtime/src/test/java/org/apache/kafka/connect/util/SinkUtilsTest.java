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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SinkUtilsTest {

    @Test
    public void testConsumerGroupOffsetsToConnectorOffsets() {
        Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = new HashMap<>();
        ConnectorOffsets connectorOffsets = SinkUtils.consumerGroupOffsetsToConnectorOffsets(consumerGroupOffsets);
        assertEquals(0, connectorOffsets.offsets().size());

        consumerGroupOffsets.put(new TopicPartition("test-topic", 0), new OffsetAndMetadata(100));

        connectorOffsets = SinkUtils.consumerGroupOffsetsToConnectorOffsets(consumerGroupOffsets);
        assertEquals(1, connectorOffsets.offsets().size());
        assertEquals(Collections.singletonMap(SinkUtils.KAFKA_OFFSET_KEY, 100L), connectorOffsets.offsets().get(0).offset());

        Map<String, Object> expectedPartition = new HashMap<>();
        expectedPartition.put(SinkUtils.KAFKA_TOPIC_KEY, "test-topic");
        expectedPartition.put(SinkUtils.KAFKA_PARTITION_KEY, 0);
        assertEquals(expectedPartition, connectorOffsets.offsets().get(0).partition());
    }

    @Test
    public void testValidateAndParseEmptyPartitionOffsetMap() {
        // expect no exception to be thrown
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(new HashMap<>());
        assertTrue(parsedOffsets.isEmpty());
    }

    @Test
    public void testValidateAndParseInvalidPartition() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, "topic");
        Map<String, Object> offset = new HashMap<>();
        offset.put(SinkUtils.KAFKA_OFFSET_KEY, 100);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, offset);

        // missing partition key
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("The partition for a sink connector offset must contain the keys 'kafka_topic' and 'kafka_partition'"));

        partition.put(SinkUtils.KAFKA_PARTITION_KEY, "not a number");
        // bad partition key
        e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("Failed to parse the following Kafka partition value in the provided offsets: 'not a number'"));

        partition.remove(SinkUtils.KAFKA_TOPIC_KEY);
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, "5");
        // missing topic key
        e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("The partition for a sink connector offset must contain the keys 'kafka_topic' and 'kafka_partition'"));
    }

    @Test
    public void testValidateAndParseInvalidOffset() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(SinkUtils.KAFKA_TOPIC_KEY, "topic");
        partition.put(SinkUtils.KAFKA_PARTITION_KEY, 10);
        Map<String, Object> offset = new HashMap<>();
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partition, offset);

        // missing offset key
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("The offset for a sink connector should either be null or contain the key 'kafka_offset'"));

        // bad offset key
        offset.put(SinkUtils.KAFKA_OFFSET_KEY, "not a number");
        e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("Failed to parse the following Kafka offset value in the provided offsets: 'not a number'"));
    }

    @Test
    public void testValidateAndParseStringPartitionValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        TopicPartition tp = parsedOffsets.keySet().iterator().next();
        assertEquals(10, tp.partition());
    }

    @Test
    public void testValidateAndParseIntegerPartitionValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", 10, "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        TopicPartition tp = parsedOffsets.keySet().iterator().next();
        assertEquals(10, tp.partition());
    }

    @Test
    public void testValidateAndParseStringOffsetValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", "100");
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        Long offsetValue = parsedOffsets.values().iterator().next();
        assertEquals(100L, offsetValue.longValue());
    }

    @Test
    public void testValidateAndParseIntegerOffsetValue() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap("topic", "10", 100);
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        Long offsetValue = parsedOffsets.values().iterator().next();
        assertEquals(100L, offsetValue.longValue());
    }

    @Test
    public void testNullOffset() {
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(SinkUtils.KAFKA_TOPIC_KEY, "topic");
        partitionMap.put(SinkUtils.KAFKA_PARTITION_KEY, 10);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partitionMap, null);
        Map<TopicPartition, Long> parsedOffsets = SinkUtils.parseSinkConnectorOffsets(partitionOffsets);
        assertEquals(1, parsedOffsets.size());
        assertNull(parsedOffsets.values().iterator().next());
    }

    @Test
    public void testNullPartition() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(SinkUtils.KAFKA_OFFSET_KEY, 100);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(null, offset);
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("The partition for a sink connector offset cannot be null or missing"));

        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(SinkUtils.KAFKA_TOPIC_KEY, "topic");
        partitionMap.put(SinkUtils.KAFKA_PARTITION_KEY, null);
        partitionOffsets.clear();
        partitionOffsets.put(partitionMap, offset);

        e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("Kafka partitions must be valid numbers and may not be null"));
    }

    @Test
    public void testNullTopic() {
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = createPartitionOffsetMap(null, "10", 100);
        ConnectException e = assertThrows(ConnectException.class, () -> SinkUtils.parseSinkConnectorOffsets(partitionOffsets));
        assertTrue(e.getMessage().contains("Kafka topic names must be valid strings and may not be null"));
    }

    private Map<Map<String, ?>, Map<String, ?>> createPartitionOffsetMap(String topic, Object partition, Object offset) {
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put(SinkUtils.KAFKA_TOPIC_KEY, topic);
        partitionMap.put(SinkUtils.KAFKA_PARTITION_KEY, partition);
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(SinkUtils.KAFKA_OFFSET_KEY, offset);
        Map<Map<String, ?>, Map<String, ?>> partitionOffsets = new HashMap<>();
        partitionOffsets.put(partitionMap, offsetMap);
        return partitionOffsets;
    }
}
