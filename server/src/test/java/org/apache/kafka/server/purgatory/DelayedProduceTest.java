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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.purgatory.DelayedProduce.PartitionStatusValidator;
import org.apache.kafka.server.purgatory.DelayedProduce.ProducePartitionStatus;
import org.apache.kafka.server.util.ServerTestUtils;

import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelayedProduceTest {

    private static final long DELAY_MS = 1000L;
    private static final long REQUIRED_OFFSET = 100L;
    private static final String TOPIC = "test-topic";
    private static final int PARTITION0 = 0;
    private static final int PARTITION1 = 1;

    @AfterEach
    public void tearDown() {
        ServerTestUtils.clearYammerMetrics();
    }

    private static TopicIdPartition topicIdPartition(int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), partition, TOPIC);
    }

    @Test
    public void testConstructorMarksPendingPartitionsAsTimedOut() {
        var tp = topicIdPartition(PARTITION0);
        var response = new PartitionResponse(Errors.NONE);
        var statusMap = Map.of(tp, new ProducePartitionStatus(REQUIRED_OFFSET, response));

        new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> new PartitionStatusValidator.Result(false, Errors.NONE),
            ignored -> { }
        );

        // A partition that starts with NONE still has acks pending, so it is marked with a timeout error
        // that is cleared once the required acks are received.
        assertEquals(Errors.REQUEST_TIMED_OUT, response.error);
    }

    @Test
    public void testTryCompleteWhenAllPartitionsHaveEnoughReplicas() {
        var tp = topicIdPartition(PARTITION0);
        var response = new PartitionResponse(Errors.NONE);
        var statusMap = Map.of(tp, new ProducePartitionStatus(REQUIRED_OFFSET, response));

        var callbackResult = new AtomicReference<Map<TopicIdPartition, PartitionResponse>>();
        var delayedProduce = new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> new PartitionStatusValidator.Result(true, Errors.NONE),
            callbackResult::set
        );

        assertTrue(delayedProduce.tryComplete());
        assertTrue(delayedProduce.isCompleted());
        // Enough replicas caught up, so the timeout error is cleared.
        assertEquals(Errors.NONE, response.error);
        assertNotNull(callbackResult.get());
        assertEquals(Errors.NONE, callbackResult.get().get(tp).error);
    }

    @Test
    public void testTryCompleteWhenReplicasNotCaughtUp() {
        var tp = topicIdPartition(PARTITION0);
        var response = new PartitionResponse(Errors.NONE);
        var statusMap = Map.of(tp, new ProducePartitionStatus(REQUIRED_OFFSET, response));

        var callbackInvoked = new AtomicBoolean(false);
        var delayedProduce = new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> new PartitionStatusValidator.Result(false, Errors.NONE),
            ignored -> callbackInvoked.set(true)
        );

        assertFalse(delayedProduce.tryComplete());
        assertFalse(delayedProduce.isCompleted());
        // Still waiting for acks, so the timeout error stays and the callback is not fired.
        assertEquals(Errors.REQUEST_TIMED_OUT, response.error);
        assertFalse(callbackInvoked.get());
    }

    @Test
    public void testTryCompleteWhenValidationReturnsError() {
        var tp = topicIdPartition(PARTITION0);
        var response = new PartitionResponse(Errors.NONE);
        var statusMap = Map.of(tp, new ProducePartitionStatus(REQUIRED_OFFSET, response));

        var delayedProduce = new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> new PartitionStatusValidator.Result(false, Errors.NOT_LEADER_OR_FOLLOWER),
            ignored -> { }
        );

        assertTrue(delayedProduce.tryComplete());
        assertTrue(delayedProduce.isCompleted());
        // A validation error satisfies the partition and is propagated to the response.
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
    }

    @Test
    public void testTryCompleteSkipsValidationForNonPendingPartitions() {
        var tp = topicIdPartition(PARTITION0);
        var response = new PartitionResponse(Errors.NOT_ENOUGH_REPLICAS);
        var statusMap = Map.of(tp, new ProducePartitionStatus(REQUIRED_OFFSET, response));

        var validatorInvoked = new AtomicBoolean(false);
        var delayedProduce = new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> {
                validatorInvoked.set(true);
                return new PartitionStatusValidator.Result(true, Errors.NONE);
            },
            ignored -> { }
        );

        // The only partition already has an error (no acks pending), so it completes without validation.
        assertTrue(delayedProduce.tryComplete());
        assertTrue(delayedProduce.isCompleted());
        assertFalse(validatorInvoked.get());
        assertEquals(Errors.NOT_ENOUGH_REPLICAS, response.error);
    }

    @Test
    public void testTryCompleteWaitsWhileAnyPartitionPending() {
        var satisfied = topicIdPartition(PARTITION0);
        var pending = topicIdPartition(PARTITION1);
        var satisfiedResponse = new PartitionResponse(Errors.NONE);
        var pendingResponse = new PartitionResponse(Errors.NONE);
        var statusMap = Map.of(
            satisfied, new ProducePartitionStatus(REQUIRED_OFFSET, satisfiedResponse),
            pending, new ProducePartitionStatus(REQUIRED_OFFSET, pendingResponse)
        );

        var delayedProduce = new DelayedProduce(
            DELAY_MS, 
            statusMap,
            (topicPartition, requiredOffset) -> topicPartition.partition() == PARTITION0
                ? new PartitionStatusValidator.Result(true, Errors.NONE)
                : new PartitionStatusValidator.Result(false, Errors.NONE),
            ignored -> { }
        );

        // One partition is satisfied but the other still has acks pending, so the operation waits.
        assertFalse(delayedProduce.tryComplete());
        assertFalse(delayedProduce.isCompleted());
        assertEquals(Errors.NONE, satisfiedResponse.error);
        assertEquals(Errors.REQUEST_TIMED_OUT, pendingResponse.error);
    }

    @Test
    public void testRemovePartitionMetrics() {
        var partition = new TopicPartition(TOPIC, PARTITION0);

        // Record an expiration so the partition metric is created
        DelayedProduce.recordExpiration(partition);

        // Verify the partition metric exists in the registry
        Predicate<MetricName> isPartitionMetric = name ->
            name.getMBeanName().contains("topic=" + TOPIC)
                && name.getMBeanName().contains("partition=" + PARTITION0)
                && name.getMBeanName().contains("name=ExpiresPerSec");
        assertNotEquals(
            List.of(), 
            KafkaYammerMetrics.defaultRegistry()
                .allMetrics()
                .keySet()
                .stream()
                .filter(isPartitionMetric).toList(), 
            "Partition metric should exist after recordExpiration"
        );

        Predicate<MetricName> isAggregateMetric = name ->
            name.getMBeanName().contains("name=ExpiresPerSec")
                && name.getMBeanName().contains("DelayedProduceMetrics")
                && !name.getMBeanName().contains("topic=");
        var aggregateCountBefore = KafkaYammerMetrics.defaultRegistry()
            .allMetrics()
            .keySet()
            .stream()
            .filter(isAggregateMetric).count();

        // Remove the partition metric
        DelayedProduce.removePartitionMetrics(partition);

        // Verify the partition metric is removed from the registry
        assertEquals(
            List.of(),
            KafkaYammerMetrics.defaultRegistry()
                .allMetrics()
                .keySet()
                .stream()
                .filter(isPartitionMetric).toList(), 
            "Partition metric should be removed after removePartitionMetrics"
        );

        // Verify the aggregate metric is unaffected
        var aggregateCountAfter = KafkaYammerMetrics.defaultRegistry()
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
        var partition = new TopicPartition("nonexistent-topic", 0);

        // Should not throw when removing a partition that was never recorded
        DelayedProduce.removePartitionMetrics(partition);
    }
}
