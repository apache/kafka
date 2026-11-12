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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests that the invoker skips the dispatch entirely when no rebalance listener is registered,
 * which is the case after {@link org.apache.kafka.clients.consumer.Consumer#setRebalanceListener(
 * org.apache.kafka.clients.consumer.RebalanceListener)} is passed null. The subscription state is
 * mocked rather than real on purpose: a real one silently drops the callbacks itself, so it would
 * hide whether the invoker checked at all.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ConsumerRebalanceListenerInvokerTest {

    private final SortedSet<TopicPartition> partitions = new TreeSet<>(new Utils.TopicPartitionComparator());

    @Mock
    private ConsumerSubscriptionState subscriptions;

    @Mock
    private RebalanceCallbackMetricsManager metricsManager;

    private ConsumerRebalanceListenerInvoker invoker;

    @BeforeEach
    public void setup() {
        partitions.add(new TopicPartition("topic", 0));
        invoker = new ConsumerRebalanceListenerInvoker(
            new LogContext(), subscriptions, new MockTime(), metricsManager);
    }

    @Test
    public void testInvokePartitionsAssignedWithoutRebalanceListenerSkipsDispatch() {
        when(subscriptions.hasRebalanceListener()).thenReturn(false);

        assertNull(invoker.invokePartitionsAssigned(partitions));

        verify(subscriptions, never()).onPartitionsAssigned(any());
        verifyNoInteractions(metricsManager);
    }

    @Test
    public void testInvokePartitionsRevokedWithoutRebalanceListenerSkipsDispatch() {
        when(subscriptions.hasRebalanceListener()).thenReturn(false);
        when(subscriptions.pausedPartitions()).thenReturn(new HashSet<>());

        assertNull(invoker.invokePartitionsRevoked(partitions));

        verify(subscriptions, never()).onPartitionsRevoked(any());
        verifyNoInteractions(metricsManager);
    }

    @Test
    public void testInvokePartitionsLostWithoutRebalanceListenerSkipsDispatch() {
        when(subscriptions.hasRebalanceListener()).thenReturn(false);
        when(subscriptions.pausedPartitions()).thenReturn(new HashSet<>());

        assertNull(invoker.invokePartitionsLost(partitions));

        verify(subscriptions, never()).onPartitionsLost(any());
        verifyNoInteractions(metricsManager);
    }

    @Test
    public void testInvokeCallbacksWithRebalanceListenerDispatches() {
        when(subscriptions.hasRebalanceListener()).thenReturn(true);
        when(subscriptions.pausedPartitions()).thenReturn(new HashSet<>());

        assertNull(invoker.invokePartitionsAssigned(partitions));
        assertNull(invoker.invokePartitionsRevoked(partitions));
        assertNull(invoker.invokePartitionsLost(partitions));

        verify(subscriptions).onPartitionsAssigned(partitions);
        verify(subscriptions).onPartitionsRevoked(partitions);
        verify(subscriptions).onPartitionsLost(partitions);
        verify(metricsManager).recordPartitionsAssignedLatency(anyLong());
        verify(metricsManager).recordPartitionsRevokedLatency(anyLong());
        verify(metricsManager).recordPartitionsLostLatency(anyLong());
    }
}
