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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingRebalanceConsumerTest {

    private static final TopicPartition TP0 = new TopicPartition("topic", 0);
    private static final TopicPartition TP1 = new TopicPartition("topic", 1);
    private static final Duration TIMEOUT = Duration.ofMillis(1000);

    private Consumer<?, ?> delegate;
    private DelegatingRebalanceConsumer rebalanceConsumer;

    @BeforeEach
    public void setup() {
        delegate = mock(Consumer.class);
        rebalanceConsumer = new DelegatingRebalanceConsumer(delegate);
    }

    @Test
    public void testPermittedOperationsDelegateToUnderlyingConsumer() {
        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(TP0, new OffsetAndMetadata(10));
        OffsetCommitCallback commitCallback = mock(OffsetCommitCallback.class);
        Map<TopicPartition, Long> timestamps = Map.of(TP0, 1000L);

        when(delegate.committed(Set.of(TP0))).thenReturn(Map.of(TP0, new OffsetAndMetadata(5)));
        when(delegate.committed(Set.of(TP0), TIMEOUT)).thenReturn(Map.of(TP0, new OffsetAndMetadata(5)));
        when(delegate.position(TP0)).thenReturn(42L);
        when(delegate.position(TP0, TIMEOUT)).thenReturn(42L);
        when(delegate.assignment()).thenReturn(Set.of(TP0, TP1));
        when(delegate.paused()).thenReturn(Set.of(TP0));
        when(delegate.clientInstanceId(TIMEOUT)).thenReturn(Uuid.randomUuid());
        when(delegate.beginningOffsets(List.of(TP0))).thenReturn(Map.of(TP0, 0L));
        when(delegate.beginningOffsets(List.of(TP0), TIMEOUT)).thenReturn(Map.of(TP0, 0L));
        when(delegate.endOffsets(List.of(TP0))).thenReturn(Map.of(TP0, 100L));
        when(delegate.endOffsets(List.of(TP0), TIMEOUT)).thenReturn(Map.of(TP0, 100L));
        when(delegate.offsetsForTimes(timestamps)).thenReturn(Map.of(TP0, new OffsetAndTimestamp(50, 1000)));
        when(delegate.offsetsForTimes(timestamps, TIMEOUT)).thenReturn(Map.of(TP0, new OffsetAndTimestamp(50, 1000)));
        when(delegate.partitionsFor("topic")).thenReturn(Collections.emptyList());
        when(delegate.partitionsFor("topic", TIMEOUT)).thenReturn(Collections.emptyList());
        when(delegate.listTopics()).thenReturn(Collections.emptyMap());
        when(delegate.listTopics(TIMEOUT)).thenReturn(Collections.emptyMap());
        when(delegate.currentLag(TP0)).thenReturn(OptionalLong.of(10));
        when(delegate.groupMetadata()).thenReturn(mock(ConsumerGroupMetadata.class));

        // Offset management
        rebalanceConsumer.commitSync();
        rebalanceConsumer.commitSync(TIMEOUT);
        rebalanceConsumer.commitSync(offsets);
        rebalanceConsumer.commitSync(offsets, TIMEOUT);
        rebalanceConsumer.commitAsync();
        rebalanceConsumer.commitAsync(commitCallback);
        rebalanceConsumer.commitAsync(offsets, commitCallback);
        rebalanceConsumer.committed(Set.of(TP0));
        rebalanceConsumer.committed(Set.of(TP0), TIMEOUT);
        assertEquals(42L, rebalanceConsumer.position(TP0));
        assertEquals(42L, rebalanceConsumer.position(TP0, TIMEOUT));

        // Seek
        rebalanceConsumer.seek(TP0, 100L);
        rebalanceConsumer.seek(TP0, new OffsetAndMetadata(100));
        rebalanceConsumer.seekToBeginning(List.of(TP0, TP1));
        rebalanceConsumer.seekToEnd(List.of(TP0, TP1));

        // Partition state
        assertEquals(Set.of(TP0, TP1), rebalanceConsumer.assignment());
        rebalanceConsumer.pause(List.of(TP0));
        rebalanceConsumer.resume(List.of(TP0));
        assertEquals(Set.of(TP0), rebalanceConsumer.paused());

        // Read-only queries
        rebalanceConsumer.clientInstanceId(TIMEOUT);
        rebalanceConsumer.beginningOffsets(List.of(TP0));
        rebalanceConsumer.beginningOffsets(List.of(TP0), TIMEOUT);
        rebalanceConsumer.endOffsets(List.of(TP0));
        rebalanceConsumer.endOffsets(List.of(TP0), TIMEOUT);
        rebalanceConsumer.offsetsForTimes(timestamps);
        rebalanceConsumer.offsetsForTimes(timestamps, TIMEOUT);
        rebalanceConsumer.partitionsFor("topic");
        rebalanceConsumer.partitionsFor("topic", TIMEOUT);
        rebalanceConsumer.listTopics();
        rebalanceConsumer.listTopics(TIMEOUT);
        assertEquals(OptionalLong.of(10), rebalanceConsumer.currentLag(TP0));
        rebalanceConsumer.groupMetadata();

        // Verify every permitted method was forwarded
        verify(delegate).commitSync();
        verify(delegate).commitSync(TIMEOUT);
        verify(delegate).commitSync(offsets);
        verify(delegate).commitSync(offsets, TIMEOUT);
        verify(delegate).commitAsync();
        verify(delegate).commitAsync(commitCallback);
        verify(delegate).commitAsync(offsets, commitCallback);
        verify(delegate).committed(Set.of(TP0));
        verify(delegate).committed(Set.of(TP0), TIMEOUT);
        verify(delegate).position(TP0);
        verify(delegate).position(TP0, TIMEOUT);
        verify(delegate).seek(TP0, 100L);
        verify(delegate).seek(TP0, new OffsetAndMetadata(100));
        verify(delegate).seekToBeginning(List.of(TP0, TP1));
        verify(delegate).seekToEnd(List.of(TP0, TP1));
        verify(delegate).assignment();
        verify(delegate).pause(List.of(TP0));
        verify(delegate).resume(List.of(TP0));
        verify(delegate).paused();
        verify(delegate).clientInstanceId(TIMEOUT);
        verify(delegate).beginningOffsets(List.of(TP0));
        verify(delegate).beginningOffsets(List.of(TP0), TIMEOUT);
        verify(delegate).endOffsets(List.of(TP0));
        verify(delegate).endOffsets(List.of(TP0), TIMEOUT);
        verify(delegate).offsetsForTimes(timestamps);
        verify(delegate).offsetsForTimes(timestamps, TIMEOUT);
        verify(delegate).partitionsFor("topic");
        verify(delegate).partitionsFor("topic", TIMEOUT);
        verify(delegate).listTopics();
        verify(delegate).listTopics(TIMEOUT);
        verify(delegate).currentLag(TP0);
        verify(delegate).groupMetadata();
    }

    @Test
    public void testUnsupportedConsumerOperationsNeverInvokedOnDelegate() {
        when(delegate.assignment()).thenReturn(Set.of(TP0));
        when(delegate.position(TP0)).thenReturn(42L);

        rebalanceConsumer.assignment();
        rebalanceConsumer.position(TP0);
        rebalanceConsumer.commitSync();
        rebalanceConsumer.seek(TP0, 100L);

        verify(delegate, never()).poll(any(Duration.class));
        verify(delegate, never()).close();
        //noinspection deprecation
        verify(delegate, never()).close(any(Duration.class));
        verify(delegate, never()).close(any(CloseOptions.class));
        verify(delegate, never()).subscribe(anyCollection());
        verify(delegate, never()).subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
        verify(delegate, never()).subscribe(any(java.util.regex.Pattern.class));
        verify(delegate, never()).subscribe(any(java.util.regex.Pattern.class), any(ConsumerRebalanceListener.class));
        verify(delegate, never()).unsubscribe();
        verify(delegate, never()).assign(anyCollection());
        verify(delegate, never()).wakeup();
        verify(delegate, never()).enforceRebalance();
        verify(delegate, never()).enforceRebalance(any());
    }

    @Test
    public void testAllMethodsThrowAfterClose() {
        rebalanceConsumer.close();

        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(TP0, new OffsetAndMetadata(10));

        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitSync());
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitSync(TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitSync(offsets));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitSync(offsets, TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitAsync());
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitAsync(mock(OffsetCommitCallback.class)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.commitAsync(Map.of(), mock(OffsetCommitCallback.class)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.committed(Set.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.committed(Set.of(TP0), TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.position(TP0));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.position(TP0, TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.seek(TP0, 0L));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.seek(TP0, new OffsetAndMetadata(0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.seekToBeginning(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.seekToEnd(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.assignment());
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.pause(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.resume(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.paused());
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.clientInstanceId(TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.beginningOffsets(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.beginningOffsets(List.of(TP0), TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.endOffsets(List.of(TP0)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.endOffsets(List.of(TP0), TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.offsetsForTimes(Map.of(TP0, 0L)));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.offsetsForTimes(Map.of(TP0, 0L), TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.partitionsFor("topic"));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.partitionsFor("topic", TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.listTopics());
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.listTopics(TIMEOUT));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.currentLag(TP0));
        assertThrows(IllegalStateException.class, () -> rebalanceConsumer.groupMetadata());

        // close itself is idempotent
        assertDoesNotThrow(() -> rebalanceConsumer.close());
    }
}
