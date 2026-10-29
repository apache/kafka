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

public class RebalanceListenerTest {

    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);
    private static final Collection<TopicPartition> PARTITIONS = List.of(TP0, TP1);

    @Test
    public void testRebalanceListenerOnPartitionsLostDefaultsToOnPartitionsRevoked() {
        List<Collection<TopicPartition>> revokedCalls = new ArrayList<>();

        RebalanceListener listener = new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                revokedCalls.add(partitions);
            }
        };

        listener.onPartitionsLost(PARTITIONS, mock(RebalanceConsumer.class));
        assertEquals(1, revokedCalls.size());
        assertEquals(PARTITIONS, revokedCalls.get(0));
    }

    @Test
    public void testRebalanceListenerOverridingOnPartitionsLostBypassesDefault() {
        List<Collection<TopicPartition>> revokedCalls = new ArrayList<>();
        List<Collection<TopicPartition>> lostCalls = new ArrayList<>();

        RebalanceListener listener = new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {}
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
        assertEquals(PARTITIONS, lostCalls.get(0));
    }

    @Test
    public void testRebalanceListenerOnPartitionsLostPassesConsumerThroughToOnPartitionsRevoked() {
        List<RebalanceConsumer> capturedConsumers = new ArrayList<>();

        RebalanceListener listener = new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                capturedConsumers.add(consumer);
            }
        };

        RebalanceConsumer rc = mock(RebalanceConsumer.class);
        listener.onPartitionsLost(PARTITIONS, rc);
        assertEquals(1, capturedConsumers.size());
        assertSame(rc, capturedConsumers.get(0));
    }

    @Test
    public void testRebalanceListenerExceptionInOnPartitionsRevokedPropagatesThroughOnPartitionsLost() {
        RuntimeException expected = new RuntimeException("boom");

        RebalanceListener listener = new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                throw expected;
            }
        };

        Exception e = assertThrows(
                RuntimeException.class, () -> listener.onPartitionsLost(PARTITIONS, mock(RebalanceConsumer.class)));
        assertSame(expected, e);
    }

    @Test
    public void testRebalanceListenerOverriddenOnPartitionsLostExceptionWinsOverRevokedException() {
        RuntimeException revokedException = new RuntimeException("from revoked");
        RuntimeException lostException = new RuntimeException("from lost");

        RebalanceListener listener = new RebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {}
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                throw revokedException;
            }
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
                throw lostException;
            }
        };

        Exception e = assertThrows(
                RuntimeException.class, () -> listener.onPartitionsLost(PARTITIONS, mock(RebalanceConsumer.class)));
        assertSame(lostException, e);
    }
}
