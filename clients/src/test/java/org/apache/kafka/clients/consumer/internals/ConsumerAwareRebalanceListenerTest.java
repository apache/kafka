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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.RebalanceConsumer;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class ConsumerAwareRebalanceListenerTest {

    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);
    private static final Collection<TopicPartition> PARTITIONS = List.of(TP0, TP1);

    private Consumer<?, ?> consumer;

    @BeforeEach
    public void setup() {
        consumer = mock(Consumer.class);
    }

    @Test
    public void testOnPartitionsAssignedCreatesViewAndDelegates() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsAssigned(PARTITIONS);

        assertEquals(1, captured.size());
        assertNotNull(captured.get(0));
    }

    @Test
    public void testOnPartitionsRevokedCreatesViewAndDelegates() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsRevoked(PARTITIONS);

        assertEquals(1, captured.size());
        assertNotNull(captured.get(0));
    }

    @Test
    public void testOnPartitionsLostCreatesViewAndDelegates() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsLost(PARTITIONS);

        assertEquals(1, captured.size());
        assertNotNull(captured.get(0));
    }

    @Test
    public void testViewIsClosedAfterAssignedCallback() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsAssigned(PARTITIONS);

        assertThrows(IllegalStateException.class, () -> captured.get(0).assignment());
    }

    @Test
    public void testViewIsClosedAfterRevokedCallback() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsRevoked(PARTITIONS);

        assertThrows(IllegalStateException.class, () -> captured.get(0).commitSync());
    }

    @Test
    public void testViewIsClosedAfterLostCallback() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsLost(PARTITIONS);

        assertThrows(IllegalStateException.class, () -> captured.get(0).paused());
    }

    @Test
    public void testViewIsClosedEvenWhenCallbackThrows() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
                throw new RuntimeException("callback failed");
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        //noinspection ExcessiveLambdaUsage
        assertThrows(RuntimeException.class, () -> wrapper.onPartitionsAssigned(PARTITIONS), () -> "callback failed");

        assertEquals(1, captured.size());
        assertThrows(IllegalStateException.class, () -> captured.get(0).assignment());
    }

    @Test
    public void testEachCallbackGetsFreshView() {
        List<RebalanceConsumer> captured = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer rc) {
                captured.add(rc);
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsAssigned(PARTITIONS);
        wrapper.onPartitionsAssigned(PARTITIONS);

        assertEquals(2, captured.size());
        assertNotSame(captured.get(0), captured.get(1));
    }

    @Test
    public void testExceptionFromCallbackPropagates() {
        RuntimeException expected = new RuntimeException("boom");
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                throw expected;
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> wrapper.onPartitionsAssigned(PARTITIONS));
        assertEquals(expected, thrown);
    }

    @Test
    public void testDefaultDelegationFromTwoArgToOneArg() {
        List<String> calls = new ArrayList<>();
        ConsumerRebalanceListener userListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                calls.add("assigned-1arg");
            }
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                calls.add("revoked-1arg");
            }
        };

        ConsumerAwareRebalanceListener wrapper = new ConsumerAwareRebalanceListener(userListener, consumer);
        wrapper.onPartitionsAssigned(PARTITIONS);
        wrapper.onPartitionsRevoked(PARTITIONS);
        wrapper.onPartitionsLost(PARTITIONS);

        assertEquals(List.of("assigned-1arg", "revoked-1arg", "revoked-1arg"), calls);
    }
}
