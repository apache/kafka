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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.util.ServerTestUtils;

import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelayedProduceTest {

    @AfterEach
    public void tearDown() {
        ServerTestUtils.clearYammerMetrics();
    }

    @Test
    public void testRemovePartitionMetrics() {
        TopicPartition partition = new TopicPartition("test-topic", 0);

        // Record an expiration so the partition metric is created
        DelayedProduce.recordExpiration(partition);

        // Verify the partition metric exists in the registry
        Predicate<MetricName> isPartitionMetric = name ->
                name.getMBeanName().contains("topic=test-topic")
                        && name.getMBeanName().contains("partition=0")
                        && name.getMBeanName().contains("name=ExpiresPerSec");
        assertTrue(
                KafkaYammerMetrics.defaultRegistry()
                        .allMetrics()
                        .keySet()
                        .stream()
                        .anyMatch(isPartitionMetric),
                "Partition metric should exist after recordExpiration"
        );

        Predicate<MetricName> isAggregateMetric = name ->
                name.getMBeanName().contains("name=ExpiresPerSec")
                        && name.getMBeanName().contains("DelayedProduceMetrics")
                        && !name.getMBeanName().contains("topic=");
        long aggregateCountBefore = KafkaYammerMetrics.defaultRegistry()
                .allMetrics()
                .keySet()
                .stream()
                .filter(isAggregateMetric).count();

        // Remove the partition metric
        DelayedProduce.removePartitionMetrics(partition);

        // Verify the partition metric is removed from the registry
        assertFalse(
                KafkaYammerMetrics.defaultRegistry()
                        .allMetrics()
                        .keySet()
                        .stream()
                        .anyMatch(isPartitionMetric),
                "Partition metric should be removed after removePartitionMetrics"
        );

        // Verify the aggregate metric is unaffected
        long aggregateCountAfter = KafkaYammerMetrics.defaultRegistry()
                .allMetrics()
                .keySet()
                .stream()
                .filter(isAggregateMetric)
                .count();
        assertEquals(
                aggregateCountBefore,
                aggregateCountAfter,
                "Aggregate metric should be unaffected by removePartitionMetrics"
        );
    }

    @Test
    public void testRemovePartitionMetricsForNonExistentPartition() {
        TopicPartition partition = new TopicPartition("nonexistent-topic", 0);

        // Should not throw when removing a partition that was never recorded
        DelayedProduce.removePartitionMetrics(partition);
    }
}
