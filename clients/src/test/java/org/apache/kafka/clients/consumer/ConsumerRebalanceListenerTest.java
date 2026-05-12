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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ConsumerRebalanceListenerTest {

    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);
    private static final Collection<TopicPartition> PARTITIONS = List.of(TP0, TP1);

    @Test
    public void testOnPartitionsAssignedTwoArgDelegatesToOneArg() {
        List<Collection<TopicPartition>> oneArgCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                oneArgCalls.add(partitions);
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
        };

        listener.onPartitionsAssigned(PARTITIONS, mock(RebalanceConsumer.class));
        assertEquals(1, oneArgCalls.size());
        assertEquals(PARTITIONS, oneArgCalls.get(0));
    }

    @Test
    public void testOnPartitionsRevokedTwoArgDelegatesToOneArg() {
        List<Collection<TopicPartition>> oneArgCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                oneArgCalls.add(partitions);
            }
        };

        listener.onPartitionsRevoked(PARTITIONS, mock(RebalanceConsumer.class));
        assertEquals(1, oneArgCalls.size());
        assertEquals(PARTITIONS, oneArgCalls.get(0));
    }

    @Test
    public void testOnPartitionsLostTwoArgDelegatesToLostOneArgThenRevokedOneArg() {
        List<Collection<TopicPartition>> revokedCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                revokedCalls.add(partitions);
            }
        };

        RebalanceConsumer rc = mock(RebalanceConsumer.class);
        listener.onPartitionsLost(PARTITIONS, rc);
        assertEquals(1, revokedCalls.size());
        assertEquals(PARTITIONS, revokedCalls.get(0));
    }

    @Test
    public void testOnPartitionsLostOneArgDelegatesToRevokedOneArg() {
        List<Collection<TopicPartition>> revokedCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                revokedCalls.add(partitions);
            }
        };

        listener.onPartitionsLost(PARTITIONS);
        assertEquals(1, revokedCalls.size());
        assertEquals(PARTITIONS, revokedCalls.get(0));
    }

    @Test
    public void testOverridingTwoArgAssignedBypassesOneArg() {
        List<Collection<TopicPartition>> oneArgCalls = new ArrayList<>();
        List<Collection<TopicPartition>> twoArgCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                oneArgCalls.add(partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                twoArgCalls.add(partitions);
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
        };

        listener.onPartitionsAssigned(PARTITIONS, mock(RebalanceConsumer.class));
        assertTrue(oneArgCalls.isEmpty());
        assertEquals(1, twoArgCalls.size());
    }

    @Test
    public void testOverridingTwoArgRevokedBypassesOneArg() {
        List<Collection<TopicPartition>> oneArgCalls = new ArrayList<>();
        List<Collection<TopicPartition>> twoArgCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                oneArgCalls.add(partitions);
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                twoArgCalls.add(partitions);
            }
        };

        listener.onPartitionsRevoked(PARTITIONS, mock(RebalanceConsumer.class));
        assertTrue(oneArgCalls.isEmpty());
        assertEquals(1, twoArgCalls.size());
    }

    @Test
    public void testOverridingTwoArgLostBypassesDefaultDelegation() {
        List<Collection<TopicPartition>> revokedCalls = new ArrayList<>();
        List<Collection<TopicPartition>> lostCalls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                revokedCalls.add(partitions);
            }
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                lostCalls.add(partitions);
            }
        };

        listener.onPartitionsLost(PARTITIONS, mock(RebalanceConsumer.class));
        assertTrue(revokedCalls.isEmpty());
        assertEquals(1, lostCalls.size());
    }


    @Test
    public void testFullCallbackLifecycle() {
        List<String> callOrder = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                callOrder.add("assigned");
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                callOrder.add("revoked");
            }
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                callOrder.add("lost");
            }
        };

        RebalanceConsumer rc = mock(RebalanceConsumer.class);

        // Simulate typical rebalance lifecycle: revoke -> assign
        listener.onPartitionsRevoked(PARTITIONS, rc);
        listener.onPartitionsAssigned(PARTITIONS, rc);

        assertEquals(List.of("revoked", "assigned"), callOrder);

        // Simulate lost scenario
        callOrder.clear();
        listener.onPartitionsLost(PARTITIONS, rc);
        assertEquals(List.of("lost"), callOrder);
    }

    @Test
    public void testRebalanceConsumerIsPassedThroughToCallback() {
        List<RebalanceConsumer> capturedConsumers = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                capturedConsumers.add(consumer);
            }
        };

        RebalanceConsumer rc = mock(RebalanceConsumer.class);
        listener.onPartitionsAssigned(PARTITIONS, rc);
        assertEquals(1, capturedConsumers.size());
        assertEquals(rc, capturedConsumers.get(0));
    }

    @Test
    public void testExceptionInOneArgPropagatesThroughTwoArg() {
        RuntimeException expected = new RuntimeException("boom");

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                throw expected;
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
        };

        Exception e = assertThrows(
                RuntimeException.class, () -> listener.onPartitionsAssigned(PARTITIONS, mock(RebalanceConsumer.class)));
        assertSame(expected, e);
    }

    @Test
    public void testMinimalListenerUsesDefaults() {
        List<String> calls = new ArrayList<>();

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                calls.add("assigned-1arg");
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                calls.add("revoked-1arg");
            }
        };

        RebalanceConsumer rc = mock(RebalanceConsumer.class);

        // 2-arg assigned defaults to 1-arg
        listener.onPartitionsAssigned(PARTITIONS, rc);
        assertEquals(List.of("assigned-1arg"), calls);

        calls.clear();
        // 2-arg revoked defaults to 1-arg
        listener.onPartitionsRevoked(PARTITIONS, rc);
        assertEquals(List.of("revoked-1arg"), calls);

        calls.clear();
        // 2-arg lost defaults to 1-arg lost which defaults to 1-arg revoked
        listener.onPartitionsLost(PARTITIONS, rc);
        assertEquals(List.of("revoked-1arg"), calls);

        calls.clear();
        // 1-arg lost defaults to 1-arg revoked
        listener.onPartitionsLost(PARTITIONS);
        assertEquals(List.of("revoked-1arg"), calls);
    }
}
