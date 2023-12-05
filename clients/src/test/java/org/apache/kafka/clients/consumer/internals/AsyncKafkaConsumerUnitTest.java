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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class AsyncKafkaConsumerUnitTest {

    private final LogContext logContext = new LogContext();
    private AsyncKafkaConsumer<String, String> consumer;
    private final Deserializers<String, String> deserializers = mock(Deserializers.class);
    private final FetchBuffer fetchBuffer = mock(FetchBuffer.class);
    private final FetchCollector<String, String> fetchCollector = mock(FetchCollector.class);
    private final ConsumerInterceptors<String, String> interceptors = mock(ConsumerInterceptors.class);
    private final Time time = new MockTime();
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final  BlockingQueue<BackgroundEvent> backgroundEventQueue = mock(BlockingQueue.class);
    private final Metrics metrics = new Metrics();
    private final SubscriptionState subscriptions = mock(SubscriptionState.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final List<ConsumerPartitionAssignor> assignors = new LinkedList<>();

    @BeforeEach
    public void setup() {
        backgroundEventQueue.clear();
        assignors.clear();
        String clientId = "";
        long retryBackoffMs = 100;
        int defaultApiTimeoutMs = 100;
        String groupId = "group-id";
        consumer = new AsyncKafkaConsumer<>(
            logContext,
            clientId,
            deserializers,
            fetchBuffer,
            fetchCollector,
            interceptors,
            time,
            applicationEventHandler,
            backgroundEventQueue,
            metrics,
            subscriptions,
            metadata,
            retryBackoffMs,
            defaultApiTimeoutMs,
            assignors,
            groupId
        );
    }

    @AfterEach
    public void cleanup() {
        consumer.close(Duration.ZERO);
    }

    @Test
    public void testCommitSyncLeaderEpochUpdate() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));

        consumer.assign(Arrays.asList(t0, t1));

        Mockito.doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof CommitApplicationEvent);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(any());

        assertDoesNotThrow(() -> consumer.commitSync(topicPartitionOffsets));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
    }

    @Test
    public void testCommitAsyncLeaderEpochUpdate() {
        OffsetCommitCallback callback = mock(OffsetCommitCallback.class);
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));
        consumer.assign(Arrays.asList(t0, t1));

        consumer.commitAsync(topicPartitionOffsets, callback);

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
    }

    @Test
    public void testCommittedLeaderEpochUpdate() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        final TopicPartition t2 = new TopicPartition("t0", 4);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, null);
        topicPartitionOffsets.put(t2, new OffsetAndMetadata(20L, Optional.of(3), ""));

        when(applicationEventHandler.addAndGet(any(), any())).thenAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertTrue(event instanceof FetchCommittedOffsetsApplicationEvent);
            return topicPartitionOffsets;
        });

        assertDoesNotThrow(() -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t2, 3);
    }

}

