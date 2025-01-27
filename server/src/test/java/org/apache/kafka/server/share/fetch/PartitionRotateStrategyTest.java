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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.server.share.fetch.PartitionRotateStrategy.PartitionRotateMetadata;
import org.apache.kafka.server.share.fetch.PartitionRotateStrategy.StrategyType;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.validateRotatedMapEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionRotateStrategyTest {

    @Test
    public void testRoundRobinStrategy() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);
        LinkedHashMap<TopicIdPartition, Integer> partitions = createPartitions(3);

        LinkedHashMap<TopicIdPartition, Integer> result = strategy.rotate(partitions, new PartitionRotateMetadata(1));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 1);

        // Session epoch is greater than the number of partitions.
        result = strategy.rotate(partitions, new PartitionRotateMetadata(5));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 2);

        // Session epoch is at Integer.MAX_VALUE.
        result = strategy.rotate(partitions, new PartitionRotateMetadata(Integer.MAX_VALUE));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 1);

        // No rotation at same size as epoch.
        result = strategy.rotate(partitions, new PartitionRotateMetadata(3));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 0);
    }

    @Test
    public void testRoundRobinStrategyWithSpecialSessionEpochs() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);

        LinkedHashMap<TopicIdPartition, Integer> partitions = createPartitions(3);
        LinkedHashMap<TopicIdPartition, Integer> result = strategy.rotate(
            partitions,
            new PartitionRotateMetadata(ShareRequestMetadata.INITIAL_EPOCH));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 0);

        result = strategy.rotate(
            partitions,
            new PartitionRotateMetadata(ShareRequestMetadata.FINAL_EPOCH));
        assertEquals(3, result.size());
        validateRotatedMapEquals(partitions, result, 0);
    }

    @Test
    public void testRoundRobinStrategyWithEmptyPartitions() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);
        // Empty partitions.
        LinkedHashMap<TopicIdPartition, Integer> result = strategy.rotate(new LinkedHashMap<>(), new PartitionRotateMetadata(5));
        // The result should be empty.
        assertTrue(result.isEmpty());
    }

    /**
     * Create an ordered map of TopicIdPartition to partition max bytes.
     * @param size The number of topic-partitions to create.
     * @return The ordered map of TopicIdPartition to partition max bytes.
     */
    private LinkedHashMap<TopicIdPartition, Integer> createPartitions(int size) {
        LinkedHashMap<TopicIdPartition, Integer> partitions = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            partitions.put(new TopicIdPartition(Uuid.randomUuid(), i, "foo" + i), 1 /* partition max bytes*/);
        }
        return partitions;
    }
}
