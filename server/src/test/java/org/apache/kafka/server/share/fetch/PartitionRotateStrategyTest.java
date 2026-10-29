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
import org.apache.kafka.server.share.fetch.PartitionRotateStrategy.StrategyType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.server.share.fetch.ShareFetchTestUtils.validateRotatedListEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionRotateStrategyTest {

    @Test
    public void testRoundRobinStrategy() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);
        List<TopicIdPartition> partitions = createPartitions(3);

        // The partition read first moves to the end, the others keep their order.
        List<TopicIdPartition> result = strategy.rotate(partitions);
        assertEquals(3, result.size());
        validateRotatedListEquals(partitions, result, 1);

        // Rotating the result again moves the next partition to the end.
        result = strategy.rotate(result);
        validateRotatedListEquals(partitions, result, 2);
    }

    @Test
    public void testRoundRobinStrategyWithSinglePartition() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);
        List<TopicIdPartition> partitions = createPartitions(1);
        assertEquals(partitions, strategy.rotate(partitions));
    }

    @Test
    public void testRoundRobinStrategyWithEmptyPartitions() {
        PartitionRotateStrategy strategy = PartitionRotateStrategy.type(StrategyType.ROUND_ROBIN);
        // Empty partitions.
        List<TopicIdPartition> result = strategy.rotate(new ArrayList<>());
        // The result should be empty.
        assertTrue(result.isEmpty());
    }

    /**
     * Create a list of topic partitions.
     * @param size The number of topic-partitions to create.
     * @return The list of topic partitions.
     */
    private List<TopicIdPartition> createPartitions(int size) {
        List<TopicIdPartition> partitions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            partitions.add(new TopicIdPartition(Uuid.randomUuid(), i, "foo" + i));
        }
        return partitions;
    }
}
