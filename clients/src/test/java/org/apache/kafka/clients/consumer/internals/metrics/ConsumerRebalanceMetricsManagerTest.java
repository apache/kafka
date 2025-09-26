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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;


class ConsumerRebalanceMetricsManagerTest {

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testAssignedPartitionCountMetric() {
        SubscriptionState subscriptionState = new SubscriptionState(mock(LogContext.class), AutoOffsetResetStrategy.EARLIEST);
        ConsumerRebalanceMetricsManager consumerRebalanceMetricsManager = new ConsumerRebalanceMetricsManager(metrics, subscriptionState);

        assertNotNull(metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount), "Metric assigned-partitions has not been registered as expected");

        // Check for manually assigned partitions
        subscriptionState.assignFromUser(Set.of(new TopicPartition("topic", 0), new TopicPartition("topic", 1)));
        assertEquals(2.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());
        subscriptionState.assignFromUser(Set.of());
        assertEquals(0.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());

        subscriptionState.unsubscribe();
        assertEquals(0.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());

        // Check for automatically assigned partitions
        subscriptionState.subscribe(Set.of("topic"), Optional.empty());
        subscriptionState.assignFromSubscribed(Set.of(new TopicPartition("topic", 0)));
        assertEquals(1.0d, metrics.metric(consumerRebalanceMetricsManager.assignedPartitionsCount).metricValue());
    }
}
