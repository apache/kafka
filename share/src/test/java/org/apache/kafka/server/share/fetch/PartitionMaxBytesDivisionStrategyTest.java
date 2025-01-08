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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.share.fetch.PartitionMaxBytesDivisionStrategy.StrategyType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionMaxBytesDivisionStrategyTest {

    @Test
    public void testConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionMaxBytesDivisionStrategy(null));
        assertEquals(StrategyType.EQUAL_DIVISION, new PartitionMaxBytesDivisionStrategy(StrategyType.EQUAL_DIVISION).type());
    }

    @Test
    public void testPartitionMaxBytesThrowsException() {
        PartitionMaxBytesDivisionStrategy partitionMaxBytesDivisionStrategy = new PartitionMaxBytesDivisionStrategy(StrategyType.EQUAL_DIVISION);
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 0));
        TopicIdPartition topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 1));
        TopicIdPartition topicIdPartition3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic2", 0));
        Set<TopicIdPartition> partitions = new LinkedHashSet<>();
        partitions.add(topicIdPartition1);
        partitions.add(topicIdPartition2);
        partitions.add(topicIdPartition3);

        // acquired partitions size is 0.
        assertThrows(IllegalArgumentException.class, () -> partitionMaxBytesDivisionStrategy.partitionMaxBytes(
            100, partitions, 0));
        // empty partitions set.
        assertThrows(IllegalArgumentException.class, () -> partitionMaxBytesDivisionStrategy.partitionMaxBytes(
            100, Collections.EMPTY_SET, 20));
        // request max bytes is 0.
        assertThrows(IllegalArgumentException.class, () -> partitionMaxBytesDivisionStrategy.partitionMaxBytes(
            0, partitions, 20));
    }

    @Test
    public void testPartitionMaxBytesEqualDivision() {
        PartitionMaxBytesDivisionStrategy partitionMaxBytesDivisionStrategy = new PartitionMaxBytesDivisionStrategy(StrategyType.EQUAL_DIVISION);
        TopicIdPartition topicIdPartition1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 0));
        TopicIdPartition topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic1", 1));
        TopicIdPartition topicIdPartition3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic2", 0));
        Set<TopicIdPartition> partitions = new LinkedHashSet<>();
        partitions.add(topicIdPartition1);
        partitions.add(topicIdPartition2);
        partitions.add(topicIdPartition3);

        LinkedHashMap<TopicIdPartition, Integer> result = partitionMaxBytesDivisionStrategy.partitionMaxBytes(
            100, partitions, 3);
        assertEquals(result.values().stream().toList(), List.of(33, 33, 33));

        result = partitionMaxBytesDivisionStrategy.partitionMaxBytes(
            100, partitions, 5);
        assertEquals(result.values().stream().toList(), List.of(20, 20, 20));
    }
}
