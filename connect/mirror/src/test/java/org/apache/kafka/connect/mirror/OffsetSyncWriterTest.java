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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OffsetSyncWriterTest {
    String topicName = "topic";
    @SuppressWarnings("unchecked")
    KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
    TopicPartition topicPartition = new TopicPartition(topicName, 0);

    @Test
    public void testMaybeQueueOffsetSyncs() {
        int maxOffsetLag = 2;

        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        Semaphore outstandingOffsetSyncs = new Semaphore(1);

        OffsetSyncWriter offsetSyncWriter = new OffsetSyncWriter(producer, topicName, outstandingOffsetSyncs, maxOffsetLag);

        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 0, 1);
        assertFalse(offsetSyncWriter.getDelayedOffsetSyncs().containsKey(topicPartition));
        assertTrue(offsetSyncWriter.getPendingOffsetSyncs().containsKey(topicPartition));
        assertEquals(1, offsetSyncWriter.partitionStates().get(topicPartition).lastSyncDownstreamOffset);

        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 1, 2);
        assertTrue(offsetSyncWriter.getDelayedOffsetSyncs().containsKey(topicPartition));
        assertEquals(1, offsetSyncWriter.partitionStates().get(topicPartition).lastSyncDownstreamOffset);
    }
    
    @Test
    public void testFirePendingOffsetSyncs() {
        int maxOffsetLag = 1;

        Semaphore outstandingOffsetSyncs = new Semaphore(1);

        OffsetSyncWriter offsetSyncWriter = new OffsetSyncWriter(producer, topicName, outstandingOffsetSyncs, maxOffsetLag);

        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 0, 100);
        assertEquals(100, offsetSyncWriter.partitionStates().get(topicPartition).lastSyncDownstreamOffset);

        offsetSyncWriter.firePendingOffsetSyncs();

        ArgumentCaptor<Callback> producerCallback = ArgumentCaptor.forClass(Callback.class);
        when(producer.send(any(), producerCallback.capture())).thenAnswer(mockInvocation -> {
            producerCallback.getValue().onCompletion(null, null);
            return null;
        });

        // We should have dispatched this sync to the producer
        verify(producer, times(1)).send(any(), any());

        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 2, 102);
        assertEquals(102, offsetSyncWriter.partitionStates().get(topicPartition).lastSyncDownstreamOffset);
        offsetSyncWriter.firePendingOffsetSyncs();

        // in-flight offset syncs; will not try to send remaining offset syncs immediately
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void testPromoteDelayedOffsetSyncs() {
        int maxOffsetLag = 50;
        @SuppressWarnings("unchecked")
        KafkaProducer<byte[], byte[]> producer = mock(KafkaProducer.class);
        Semaphore outstandingOffsetSyncs = new Semaphore(1);

        OffsetSyncWriter offsetSyncWriter = new OffsetSyncWriter(producer, topicName, outstandingOffsetSyncs, maxOffsetLag);
        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 0, 100);
        offsetSyncWriter.maybeQueueOffsetSyncs(topicPartition, 1, 101);
        offsetSyncWriter.promoteDelayedOffsetSyncs();

        assertTrue(offsetSyncWriter.getDelayedOffsetSyncs().isEmpty());
        Map<TopicPartition, OffsetSync> pendingOffsetSyncs = offsetSyncWriter.getPendingOffsetSyncs();
        assertEquals(1, pendingOffsetSyncs.size());
        assertEquals(1, pendingOffsetSyncs.get(topicPartition).upstreamOffset());
        assertEquals(101, pendingOffsetSyncs.get(topicPartition).downstreamOffset());
    }
}
