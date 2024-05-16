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

import org.apache.kafka.clients.Metadata.LeaderAndEpoch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.clients.consumer.internals.MembershipManagerImpl.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class AsyncKafkaConsumerTest {

    private AsyncKafkaConsumer<String, String> consumer = null;
    private Time time = new MockTime(0);
    private final FetchCollector<String, String> fetchCollector = mock(FetchCollector.class);
    private final ApplicationEventHandler applicationEventHandler = mock(ApplicationEventHandler.class);
    private final ConsumerMetadata metadata = mock(ConsumerMetadata.class);
    private final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();

    @AfterEach
    public void resetAll() {
        backgroundEventQueue.clear();
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
        consumer = null;
        Mockito.framework().clearInlineMocks();
        MockConsumerInterceptor.resetCounters();
    }

    private AsyncKafkaConsumer<String, String> newConsumer() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        return newConsumer(props);
    }

    private AsyncKafkaConsumer<String, String> newConsumerWithoutGroupId() {
        final Properties props = requiredConsumerConfig();
        return newConsumer(props);
    }

    @SuppressWarnings("UnusedReturnValue")
    private AsyncKafkaConsumer<String, String> newConsumerWithEmptyGroupId() {
        final Properties props = requiredConsumerConfigAndGroupId("");
        return newConsumer(props);
    }

    private AsyncKafkaConsumer<String, String> newConsumer(Properties props) {
        final ConsumerConfig config = new ConsumerConfig(props);
        return newConsumer(config);
    }

    private AsyncKafkaConsumer<String, String> newConsumer(ConsumerConfig config) {
        return new AsyncKafkaConsumer<>(
            config,
            new StringDeserializer(),
            new StringDeserializer(),
            time,
            (a, b, c, d, e, f) -> applicationEventHandler,
            (a, b, c, d, e, f, g) -> fetchCollector,
            (a, b, c, d) -> metadata,
            backgroundEventQueue
        );
    }

    private AsyncKafkaConsumer<String, String> newConsumer(
        FetchBuffer fetchBuffer,
        ConsumerInterceptors<String, String> interceptors,
        ConsumerRebalanceListenerInvoker rebalanceListenerInvoker,
        SubscriptionState subscriptions,
        List<ConsumerPartitionAssignor> assignors,
        String groupId,
        String clientId) {
        long retryBackoffMs = 100L;
        int defaultApiTimeoutMs = 1000;
        boolean autoCommitEnabled = true;
        return new AsyncKafkaConsumer<>(
            new LogContext(),
            clientId,
            new Deserializers<>(new StringDeserializer(), new StringDeserializer()),
            fetchBuffer,
            fetchCollector,
            interceptors,
            time,
            applicationEventHandler,
            backgroundEventQueue,
            rebalanceListenerInvoker,
            new Metrics(),
            subscriptions,
            metadata,
            retryBackoffMs,
            defaultApiTimeoutMs,
            assignors,
            groupId,
            autoCommitEnabled);
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer();
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testInvalidGroupId() {
        KafkaException e = assertThrows(KafkaException.class, this::newConsumerWithEmptyGroupId);
        assertInstanceOf(InvalidGroupIdException.class, e.getCause());
    }

    @Test
    public void testFailOnClosedConsumer() {
        consumer = newConsumer();
        consumer.close();
        final IllegalStateException res = assertThrows(IllegalStateException.class, consumer::assignment);
        assertEquals("This consumer has already been closed.", res.getMessage());
    }

    @Test
    public void testCommitAsyncWithNullCallback() {
        consumer = newConsumer();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(t0, new OffsetAndMetadata(10L));
        offsets.put(t1, new OffsetAndMetadata(20L));

        consumer.commitAsync(offsets, null);

        final ArgumentCaptor<AsyncCommitEvent> commitEventCaptor = ArgumentCaptor.forClass(AsyncCommitEvent.class);
        verify(applicationEventHandler).add(commitEventCaptor.capture());
        final AsyncCommitEvent commitEvent = commitEventCaptor.getValue();
        assertEquals(offsets, commitEvent.offsets());
        assertDoesNotThrow(() -> commitEvent.future().complete(null));
        assertDoesNotThrow(() -> consumer.commitAsync(offsets, null));

        // Clean-up. Close the consumer here as we know it will cause a TimeoutException to be thrown.
        // If we get an error *other* than the TimeoutException, we'll fail the test.
        try {
            Exception e = assertThrows(KafkaException.class, () -> consumer.close(Duration.ZERO));
            assertInstanceOf(TimeoutException.class, e.getCause());
        } finally {
            consumer = null;
        }
    }

    @Test
    public void testCommitAsyncUserSuppliedCallbackNoException() {
        consumer = newConsumer();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));
        completeCommitAsyncApplicationEventSuccessfully();

        MockCommitCallback callback = new MockCommitCallback();
        assertDoesNotThrow(() -> consumer.commitAsync(offsets, callback));
        forceCommitCallbackInvocation();

        assertEquals(callback.invoked, 1);
        assertNull(callback.exception);
    }

    @ParameterizedTest
    @MethodSource("commitExceptionSupplier")
    public void testCommitAsyncUserSuppliedCallbackWithException(Exception exception) {
        consumer = newConsumer();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));
        completeCommitAsyncApplicationEventExceptionally(exception);

        MockCommitCallback callback = new MockCommitCallback();
        assertDoesNotThrow(() -> consumer.commitAsync(offsets, callback));
        forceCommitCallbackInvocation();

        assertSame(exception.getClass(), callback.exception.getClass());
    }

    private static Stream<Exception> commitExceptionSupplier() {
        return Stream.of(
                new KafkaException("Test exception"),
                new GroupAuthorizationException("Group authorization exception"));
    }

    @Test
    public void testCommitAsyncWithFencedException() {
        consumer = newConsumer();
        final Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        MockCommitCallback callback = new MockCommitCallback();

        assertDoesNotThrow(() -> consumer.commitAsync(offsets, callback));

        final ArgumentCaptor<AsyncCommitEvent> commitEventCaptor = ArgumentCaptor.forClass(AsyncCommitEvent.class);
        verify(applicationEventHandler).add(commitEventCaptor.capture());
        final AsyncCommitEvent commitEvent = commitEventCaptor.getValue();
        commitEvent.future().completeExceptionally(Errors.FENCED_INSTANCE_ID.exception());

        assertThrows(Errors.FENCED_INSTANCE_ID.exception().getClass(), () -> consumer.commitAsync());
    }

    @Test
    public void testCommitted() {
        time = new MockTime(1);
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = mockTopicPartitionOffset();
        completeFetchedCommittedOffsetApplicationEventSuccessfully(topicPartitionOffsets);

        assertEquals(topicPartitionOffsets, consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class), any());
        final Metric metric = consumer.metrics()
            .get(consumer.metricsRegistry().metricName("committed-time-ns-total", "consumer-metrics"));
        assertTrue((double) metric.metricValue() > 0);
    }

    @Test
    public void testCommittedLeaderEpochUpdate() {
        consumer = newConsumer();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        final TopicPartition t2 = new TopicPartition("t0", 4);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, null);
        topicPartitionOffsets.put(t2, new OffsetAndMetadata(20L, Optional.of(3), ""));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(topicPartitionOffsets);

        assertDoesNotThrow(() -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t2, 3);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class), any());
    }

    @Test
    public void testCommittedExceptionThrown() {
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        when(applicationEventHandler.addAndGet(
            any(FetchCommittedOffsetsEvent.class), any())).thenAnswer(invocation -> {
                CompletableApplicationEvent<?> event = invocation.getArgument(0);
                assertInstanceOf(FetchCommittedOffsetsEvent.class, event);
                throw new KafkaException("Test exception");
            });

        assertThrows(KafkaException.class, () -> consumer.committed(offsets.keySet(), Duration.ofMillis(1000)));
    }

    @Test
    public void testWakeupBeforeCallingPoll() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(offsets);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterEmptyFetch() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doAnswer(invocation -> {
            consumer.wakeup();
            return Fetch.empty();
        }).doAnswer(invocation -> Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(offsets);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofMinutes(1)));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterNonEmptyFetch() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );
        doAnswer(invocation -> {
            consumer.wakeup();
            return Fetch.forPartition(tp, records, true);
        }).when(fetchCollector).collectFetch(Mockito.any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(offsets);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        // since wakeup() is called when the non-empty fetch is returned the wakeup should be ignored
        assertDoesNotThrow(() -> consumer.poll(Duration.ofMinutes(1)));
        // the previously ignored wake-up should not be ignored in the next call
        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testCommitInRebalanceCallback() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doAnswer(invocation -> Fetch.empty()).when(fetchCollector).collectFetch(Mockito.any(FetchBuffer.class));
        SortedSet<TopicPartition> sortedPartitions = new TreeSet<>(TOPIC_PARTITION_COMPARATOR);
        sortedPartitions.add(tp);
        CompletableBackgroundEvent<Void> e = new ConsumerRebalanceListenerCallbackNeededEvent(ON_PARTITIONS_REVOKED, sortedPartitions);
        backgroundEventQueue.add(e);
        completeCommitSyncApplicationEventSuccessfully();
        final AtomicBoolean callbackExecuted = new AtomicBoolean(false);

        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                assertDoesNotThrow(() -> consumer.commitSync(mkMap(mkEntry(tp, new OffsetAndMetadata(0)))));
                callbackExecuted.set(true);
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                // no-op
            }
        };

        consumer.subscribe(Collections.singletonList(topicName), listener);
        consumer.poll(Duration.ZERO);
        assertTrue(callbackExecuted.get());
    }

    @Test
    public void testClearWakeupTriggerAfterPoll() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );
        doReturn(Fetch.forPartition(tp, records, true))
            .when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(offsets);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        consumer.poll(Duration.ZERO);

        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testEnsureCallbackExecutedByApplicationThread() {
        consumer = newConsumer();
        final String currentThread = Thread.currentThread().getName();
        MockCommitCallback callback = new MockCommitCallback();
        completeCommitAsyncApplicationEventSuccessfully();

        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        forceCommitCallbackInvocation();
        assertEquals(1, callback.invoked);
        assertEquals(currentThread, callback.completionThread);
    }

    @Test
    public void testEnsureCommitSyncExecutedCommitAsyncCallbacks() {
        consumer = newConsumer();
        KafkaException callbackException = new KafkaException("Async commit callback failed");
        OffsetCommitCallback callback = (offsets, exception) -> {
            throw callbackException;
        };

        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        assertThrows(callbackException.getClass(), () -> consumer.commitSync());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPollLongThrowsException() {
        consumer = newConsumer();
        Exception e = assertThrows(UnsupportedOperationException.class, () -> consumer.poll(0L));
        assertEquals("Consumer.poll(long) is not supported when \"group.protocol\" is \"consumer\". " +
            "This method is deprecated and will be removed in the next major release.", e.getMessage());
    }

    @Test
    public void testCommitSyncLeaderEpochUpdate() {
        consumer = newConsumer();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));
        completeCommitSyncApplicationEventSuccessfully();

        consumer.assign(Arrays.asList(t0, t1));

        assertDoesNotThrow(() -> consumer.commitSync(topicPartitionOffsets));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
        verify(applicationEventHandler).add(ArgumentMatchers.isA(SyncCommitEvent.class));
    }

    @Test
    public void testCommitAsyncLeaderEpochUpdate() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            new ConsumerInterceptors<>(Collections.emptyList()),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            singletonList(new RoundRobinAssignor()),
            "group-id",
            "client-id");
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));
        when(metadata.currentLeader(t0)).thenReturn(
            new LeaderAndEpoch(Optional.of(
                new Node(1, "host", 9000)), Optional.of(1)));
        when(metadata.currentLeader(t1)).thenReturn(
            new LeaderAndEpoch(Optional.of(
                new Node(1, "host", 9000)), Optional.of(1)));
        consumer.assign(Arrays.asList(t0, t1));
        consumer.seek(t0, 10);
        consumer.seek(t1, 20);

        MockCommitCallback callback = new MockCommitCallback();
        assertDoesNotThrow(() -> consumer.commitAsync(topicPartitionOffsets, callback));

        verify(metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(metadata).updateLastSeenEpochIfNewer(t1, 1);
        verify(applicationEventHandler).add(ArgumentMatchers.isA(AsyncCommitEvent.class));

        // Clean-Up. Close the consumer here as we know it will cause a TimeoutException to be thrown.
        // If we get an error *other* than the TimeoutException, we'll fail the test.
        try {
            Exception e = assertThrows(KafkaException.class, () -> consumer.close(Duration.ZERO));
            assertInstanceOf(TimeoutException.class, e.getCause());
        } finally {
            consumer = null;
        }
    }

    @Test
    public void testCommitAsyncTriggersFencedExceptionFromCommitAsync() {
        final String groupId = "consumerGroupA";
        final String groupInstanceId = "groupInstanceId1";
        final Properties props = requiredConsumerConfigAndGroupId(groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);
        completeCommitAsyncApplicationEventExceptionally(Errors.FENCED_INSTANCE_ID.exception());
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(mkMap());
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        final TopicPartition tp = new TopicPartition("foo", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);

        assertDoesNotThrow(() -> consumer.commitAsync());

        Exception e = assertThrows(FencedInstanceIdException.class, () -> consumer.commitAsync());
        assertEquals("Get fenced exception for group.instance.id groupInstanceId1", e.getMessage());
    }

    @Test
    public void testCommitSyncTriggersFencedExceptionFromCommitAsync() {
        final String groupId = "consumerGroupA";
        final String groupInstanceId = "groupInstanceId1";
        final Properties props = requiredConsumerConfigAndGroupId(groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);
        completeCommitAsyncApplicationEventExceptionally(Errors.FENCED_INSTANCE_ID.exception());
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(mkMap());
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        final TopicPartition tp = new TopicPartition("foo", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);

        assertDoesNotThrow(() -> consumer.commitAsync());

        Exception e =  assertThrows(FencedInstanceIdException.class, () -> consumer.commitSync());
        assertEquals("Get fenced exception for group.instance.id groupInstanceId1", e.getMessage());
    }

    @Test
    public void testCommitSyncAwaitsCommitAsyncCompletionWithEmptyOffsets() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final CompletableFuture<Void> asyncCommitFuture = setUpConsumerWithIncompleteAsyncCommit(tp);

        // Commit async is not completed yet, so commit sync should wait for it to complete (time out)
        assertThrows(TimeoutException.class, () -> consumer.commitSync(Collections.emptyMap(), Duration.ofMillis(100)));

        // Complete exceptionally async commit event
        asyncCommitFuture.completeExceptionally(new KafkaException("Test exception"));

        // Commit async is completed, so commit sync completes immediately (since offsets are empty)
        assertDoesNotThrow(() -> consumer.commitSync(Collections.emptyMap(), Duration.ofMillis(100)));
    }

    @Test
    public void testCommitSyncAwaitsCommitAsyncCompletionWithNonEmptyOffsets() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final CompletableFuture<Void> asyncCommitFuture = setUpConsumerWithIncompleteAsyncCommit(tp);

        // Mock to complete sync event
        completeCommitSyncApplicationEventSuccessfully();

        // Commit async is not completed yet, so commit sync should wait for it to complete (time out)
        assertThrows(TimeoutException.class, () -> consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(20)), Duration.ofMillis(100)));

        // Complete async commit event
        asyncCommitFuture.complete(null);

        // Commit async is completed, so commit sync does not need to wait before committing its offsets
        assertDoesNotThrow(() -> consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(20)), Duration.ofMillis(100)));
    }

    @Test
    public void testCommitSyncAwaitsCommitAsyncButDoesNotFail() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final CompletableFuture<Void> asyncCommitFuture = setUpConsumerWithIncompleteAsyncCommit(tp);

        // Mock to complete sync event
        completeCommitSyncApplicationEventSuccessfully();

        // Commit async is not completed yet, so commit sync should wait for it to complete (time out)
        assertThrows(TimeoutException.class, () -> consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(20)), Duration.ofMillis(100)));

        // Complete exceptionally async commit event
        asyncCommitFuture.completeExceptionally(new KafkaException("Test exception"));

        // Commit async is completed exceptionally, but this will be handled by commit callback - commit sync should not fail.
        assertDoesNotThrow(() -> consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(20)), Duration.ofMillis(100)));
    }

    private CompletableFuture<Void> setUpConsumerWithIncompleteAsyncCommit(TopicPartition tp) {
        time = new MockTime(1);
        consumer = newConsumer();

        // Commit async (incomplete)
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);
        consumer.commitAsync();

        return getLastEnqueuedEventFuture();
    }

    // ArgumentCaptor's type-matching does not work reliably with Java 8, so we cannot directly capture the AsyncCommitEvent
    // Instead, we capture the super-class CompletableApplicationEvent and fetch the last captured event.
    private CompletableFuture<Void> getLastEnqueuedEventFuture() {
        final ArgumentCaptor<CompletableApplicationEvent<Void>> eventArgumentCaptor = ArgumentCaptor.forClass(CompletableApplicationEvent.class);
        verify(applicationEventHandler, atLeast(1)).add(eventArgumentCaptor.capture());
        final List<CompletableApplicationEvent<Void>> allValues = eventArgumentCaptor.getAllValues();
        final CompletableApplicationEvent<Void> lastEvent = allValues.get(allValues.size() - 1);
        return lastEvent.future();
    }

    @Test
    public void testPollTriggersFencedExceptionFromCommitAsync() {
        final String groupId = "consumerGroupA";
        final String groupInstanceId = "groupInstanceId1";
        final Properties props = requiredConsumerConfigAndGroupId(groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);
        completeCommitAsyncApplicationEventExceptionally(Errors.FENCED_INSTANCE_ID.exception());
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(mkMap());
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        final TopicPartition tp = new TopicPartition("foo", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);

        assertDoesNotThrow(() -> consumer.commitAsync());

        Exception e = assertThrows(FencedInstanceIdException.class, () -> consumer.poll(Duration.ZERO));
        assertEquals("Get fenced exception for group.instance.id groupInstanceId1", e.getMessage());
    }

    @Test
    public void testEnsurePollExecutedCommitAsyncCallbacks() {
        consumer = newConsumer();
        MockCommitCallback callback = new MockCommitCallback();
        completeCommitAsyncApplicationEventSuccessfully();
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(mkMap());

        consumer.assign(Collections.singleton(new TopicPartition("foo", 0)));
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        assertMockCommitCallbackInvoked(() -> consumer.poll(Duration.ZERO),
            callback,
            null);
    }

    @Test
    public void testEnsureShutdownExecutedCommitAsyncCallbacks() {
        consumer = newConsumer();
        MockCommitCallback callback = new MockCommitCallback();
        completeCommitAsyncApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        assertMockCommitCallbackInvoked(() -> consumer.close(),
            callback,
            null);
    }

    @Test
    public void testVerifyApplicationEventOnShutdown() {
        consumer = newConsumer();
        doReturn(null).when(applicationEventHandler).addAndGet(any(), any());
        consumer.close();
        verify(applicationEventHandler).addAndGet(any(LeaveOnCloseEvent.class), any());
        verify(applicationEventHandler).add(any(CommitOnCloseEvent.class));
    }

    @Test
    public void testPartitionRevocationOnClose() {
        MockRebalanceListener listener = new MockRebalanceListener();
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            singletonList(new RoundRobinAssignor()),
            "group-id",
            "client-id");

        consumer.subscribe(singleton("topic"), listener);
        subscriptions.assignFromSubscribed(singleton(new TopicPartition("topic", 0)));
        consumer.close(Duration.ZERO);
        assertTrue(subscriptions.assignedPartitions().isEmpty());
        assertEquals(1, listener.revokedCount);
    }

    @Test
    public void testFailedPartitionRevocationOnClose() {
        // If rebalance listener failed to execute during close, we will skip sending leave group and proceed with
        // closing the consumer.
        ConsumerRebalanceListener listener = mock(ConsumerRebalanceListener.class);
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            new ConsumerInterceptors<>(Collections.emptyList()),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            singletonList(new RoundRobinAssignor()),
            "group-id",
            "client-id");
        subscriptions.subscribe(singleton("topic"), Optional.of(listener));
        TopicPartition tp = new TopicPartition("topic", 0);
        subscriptions.assignFromSubscribed(singleton(tp));
        doThrow(new KafkaException()).when(listener).onPartitionsRevoked(eq(singleton(tp)));
        assertThrows(KafkaException.class, () -> consumer.close(Duration.ZERO));
        verify(applicationEventHandler, never()).addAndGet(any(LeaveOnCloseEvent.class), any());
        verify(listener).onPartitionsRevoked(eq(singleton(tp)));
        assertEquals(emptySet(), subscriptions.assignedPartitions());
    }

    @Test
    public void testCompleteQuietly() {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        CompletableFuture<Object> future = CompletableFuture.completedFuture(null);
        consumer = newConsumer();
        assertDoesNotThrow(() -> consumer.completeQuietly(() -> {
            future.get(0, TimeUnit.MILLISECONDS);
        }, "test", exception));
        assertNull(exception.get());

        assertDoesNotThrow(() -> consumer.completeQuietly(() -> {
            throw new KafkaException("Test exception");
        }, "test", exception));
        assertInstanceOf(KafkaException.class, exception.get());
    }

    @Test
    public void testAutoCommitSyncEnabled() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            singletonList(new RoundRobinAssignor()),
            "group-id",
            "client-id");
        consumer.subscribe(singleton("topic"), mock(ConsumerRebalanceListener.class));
        subscriptions.assignFromSubscribed(singleton(new TopicPartition("topic", 0)));
        subscriptions.seek(new TopicPartition("topic", 0), 100);
        consumer.maybeAutoCommitSync(true, time.timer(100));
        verify(applicationEventHandler).add(any(SyncCommitEvent.class));
    }

    @Test
    public void testAutoCommitSyncDisabled() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            singletonList(new RoundRobinAssignor()),
            "group-id",
            "client-id");
        consumer.subscribe(singleton("topic"), mock(ConsumerRebalanceListener.class));
        subscriptions.assignFromSubscribed(singleton(new TopicPartition("topic", 0)));
        subscriptions.seek(new TopicPartition("topic", 0), 100);
        consumer.maybeAutoCommitSync(false, time.timer(100));
        verify(applicationEventHandler, never()).add(any(SyncCommitEvent.class));
    }

    private void assertMockCommitCallbackInvoked(final Executable task,
                                                 final MockCommitCallback callback,
                                                 final Errors errors) {
        assertDoesNotThrow(task);
        assertEquals(1, callback.invoked);
        if (errors == null)
            assertNull(callback.exception);
        else if (errors.exception() instanceof RetriableException)
            assertInstanceOf(RetriableCommitFailedException.class, callback.exception);
    }

    private static class MockCommitCallback implements OffsetCommitCallback {
        public int invoked = 0;
        public Exception exception = null;
        public String completionThread;

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            invoked++;
            this.completionThread = Thread.currentThread().getName();
            this.exception = exception;
        }
    }

    @Test
    public void testAssign() {
        consumer = newConsumer();
        final TopicPartition tp = new TopicPartition("foo", 3);
        consumer.assign(singleton(tp));
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().contains(tp));
        verify(applicationEventHandler).add(any(AssignmentChangeEvent.class));
        verify(applicationEventHandler).add(any(NewTopicsMetadataUpdateRequestEvent.class));
    }

    @Test
    public void testAssignOnNullTopicPartition() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(null));
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.assign(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testAssignOnNullTopicInPartition() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition(null, 0))));
    }

    @Test
    public void testAssignOnEmptyTopicInPartition() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition("  ", 0))));
    }

    @Test
    public void testBeginningOffsetsFailsIfNullPartitions() {
        consumer = newConsumer();
        assertThrows(NullPointerException.class, () -> consumer.beginningOffsets(null,
            Duration.ofMillis(1)));
    }

    @Test
    public void testBeginningOffsets() {
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndTimestampInternal> expectedOffsets = mockOffsetAndTimestamp();

        when(applicationEventHandler.addAndGet(any(ListOffsetsEvent.class), any())).thenAnswer(invocation -> {
            Timer timer = invocation.getArgument(1);
            if (timer.remainingMs() == 0) {
                fail("Timer duration should not be zero.");
            }
            return expectedOffsets;
        });

        Map<TopicPartition, Long> result = assertDoesNotThrow(() -> consumer.beginningOffsets(expectedOffsets.keySet(), Duration.ofMillis(1)));

        expectedOffsets.forEach((key, value) -> {
            assertTrue(result.containsKey(key));
            assertEquals(value.offset(), result.get(key));
        });
        verify(applicationEventHandler).addAndGet(any(ListOffsetsEvent.class), any(Timer.class));
    }

    @Test
    public void testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure() {
        consumer = newConsumer();
        Set<TopicPartition> partitions = mockTopicPartitionOffset().keySet();
        Throwable eventProcessingFailure = new KafkaException("Unexpected failure " +
            "processing List Offsets event");
        doThrow(eventProcessingFailure).when(applicationEventHandler).addAndGet(
            any(ListOffsetsEvent.class),
            any());
        Throwable consumerError = assertThrows(KafkaException.class,
            () -> consumer.beginningOffsets(partitions,
                Duration.ofMillis(1)));
        assertEquals(eventProcessingFailure, consumerError);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsTimeoutOnEventProcessingTimeout() {
        consumer = newConsumer();
        doThrow(new TimeoutException()).when(applicationEventHandler).addAndGet(any(), any());
        assertThrows(TimeoutException.class,
            () -> consumer.beginningOffsets(
                Collections.singletonList(new TopicPartition("t1", 0)),
                Duration.ofMillis(1)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testOffsetsForTimesOnNullPartitions() {
        consumer = newConsumer();
        assertThrows(NullPointerException.class, () -> consumer.offsetsForTimes(null,
            Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimesFailsOnNegativeTargetTimes() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.EARLIEST_TIMESTAMP),
                        Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.LATEST_TIMESTAMP),
                        Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.MAX_TIMESTAMP),
                        Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimes() {
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndTimestampInternal> expectedResult = mockOffsetAndTimestamp();
        Map<TopicPartition, Long> timestampToSearch = mockTimestampToSearch();

        doReturn(expectedResult).when(applicationEventHandler).addAndGet(any(), any());
        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ofMillis(1)));
        expectedResult.forEach((key, value) -> {
            OffsetAndTimestamp expected = value.buildOffsetAndTimestamp();
            assertEquals(expected, result.get(key));
        });
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    // This test ensures same behaviour as the current consumer when offsetsForTimes is called
    // with 0 timeout. It should return map with all requested partitions as keys, with null
    // OffsetAndTimestamp as value.
    @Test
    public void testBeginningOffsetsWithZeroTimeout() {
        consumer = newConsumer();
        TopicPartition tp = new TopicPartition("topic1", 0);
        Map<TopicPartition, Long> result =
                assertDoesNotThrow(() -> consumer.beginningOffsets(Collections.singletonList(tp), Duration.ZERO));
        // The result should be {tp=null}
        assertTrue(result.containsKey(tp));
        assertNull(result.get(tp));
        verify(applicationEventHandler).add(ArgumentMatchers.isA(ListOffsetsEvent.class));
    }

    @Test
    public void testOffsetsForTimesWithZeroTimeout() {
        consumer = newConsumer();
        TopicPartition tp = new TopicPartition("topic1", 0);
        Map<TopicPartition, OffsetAndTimestamp> expectedResult = Collections.singletonMap(tp, null);
        Map<TopicPartition, Long> timestampToSearch = Collections.singletonMap(tp, 5L);
        Map<TopicPartition, OffsetAndTimestamp> result =
            assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ZERO));
        assertEquals(expectedResult, result);
        verify(applicationEventHandler, never()).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class),
            ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testWakeupCommitted() {
        consumer = newConsumer();
        final Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            Timer timer = invocation.getArgument(1);
            assertInstanceOf(FetchCommittedOffsetsEvent.class, event);
            assertTrue(event.future().isCompletedExceptionally());
            return ConsumerUtils.getResult(event.future(), timer);
        })
            .when(applicationEventHandler)
            .addAndGet(any(FetchCommittedOffsetsEvent.class), any(Timer.class));

        consumer.wakeup();
        assertThrows(WakeupException.class, () -> consumer.committed(offsets.keySet()));
        assertNull(consumer.wakeupTrigger().getPendingTask());
    }

    @Test
    public void testNoWakeupInCloseCommit() {
        TopicPartition tp = new TopicPartition("topic1", 0);
        consumer = newConsumer();
        consumer.assign(Collections.singleton(tp));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.seek(tp, 10);
        consumer.wakeup();

        AtomicReference<SyncCommitEvent> capturedEvent = new AtomicReference<>();
        doAnswer(invocation -> {
            ApplicationEvent event = invocation.getArgument(0);
            if (event instanceof SyncCommitEvent) {
                capturedEvent.set((SyncCommitEvent) event);
            }
            return null;
        }).when(applicationEventHandler).add(any());

        consumer.close(Duration.ZERO);

        // A commit was triggered and not completed exceptionally by the wakeup
        assertNotNull(capturedEvent.get());
        assertFalse(capturedEvent.get().future().isCompletedExceptionally());
    }

    @Test
    public void testCloseAwaitPendingAsyncCommitIncomplete() {
        time = new MockTime(1);
        consumer = newConsumer();

        // Commit async (incomplete)
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        final TopicPartition tp = new TopicPartition("foo", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);

        consumer.commitAsync();
        Exception e = assertThrows(KafkaException.class, () -> consumer.close(Duration.ofMillis(10)));
        assertInstanceOf(TimeoutException.class, e.getCause());
    }

    @Test
    public void testCloseAwaitPendingAsyncCommitComplete() {
        time = new MockTime(1);
        consumer = newConsumer();
        MockCommitCallback cb = new MockCommitCallback();

        // Commit async (complete)
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        final TopicPartition tp = new TopicPartition("foo", 0);
        consumer.assign(Collections.singleton(tp));
        consumer.seek(tp, 20);
        completeCommitAsyncApplicationEventSuccessfully();
        consumer.commitAsync(cb);

        assertDoesNotThrow(() -> consumer.close(Duration.ofMillis(10)));
        assertEquals(1, cb.invoked);
    }


    @Test
    public void testInterceptorAutoCommitOnClose() {
        Properties props = requiredConsumerConfigAndGroupId("test-id");
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = newConsumer(props);
        assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
        completeCommitSyncApplicationEventSuccessfully();

        consumer.close(Duration.ZERO);

        assertEquals(1, MockConsumerInterceptor.ON_COMMIT_COUNT.get());
        assertEquals(1, MockConsumerInterceptor.CLOSE_COUNT.get());
    }

    @Test
    public void testInterceptorCommitSync() {
        Properties props = requiredConsumerConfigAndGroupId("test-id");
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = newConsumer(props);
        assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
        completeCommitSyncApplicationEventSuccessfully();

        consumer.commitSync(mockTopicPartitionOffset());

        assertEquals(1, MockConsumerInterceptor.ON_COMMIT_COUNT.get());
    }

    @Test
    public void testNoInterceptorCommitSyncFailed() {
        Properties props = requiredConsumerConfigAndGroupId("test-id");
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = newConsumer(props);
        assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
        KafkaException expected = new KafkaException("Test exception");
        completeCommitSyncApplicationEventExceptionally(expected);

        KafkaException actual = assertThrows(KafkaException.class, () -> consumer.commitSync(mockTopicPartitionOffset()));
        assertEquals(expected, actual);
        assertEquals(0, MockConsumerInterceptor.ON_COMMIT_COUNT.get());
    }

    @Test
    public void testInterceptorCommitAsync() {
        Properties props = requiredConsumerConfigAndGroupId("test-id");
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = newConsumer(props);
        assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());

        completeCommitAsyncApplicationEventSuccessfully();
        consumer.commitAsync(mockTopicPartitionOffset(), new MockCommitCallback());
        assertEquals(0, MockConsumerInterceptor.ON_COMMIT_COUNT.get());

        forceCommitCallbackInvocation();
        assertEquals(1, MockConsumerInterceptor.ON_COMMIT_COUNT.get());
    }

    @Test
    public void testNoInterceptorCommitAsyncFailed() {
        Properties props = requiredConsumerConfigAndGroupId("test-id");
        props.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumer = newConsumer(props);
        assertEquals(1, MockConsumerInterceptor.INIT_COUNT.get());
        completeCommitAsyncApplicationEventExceptionally(new KafkaException("Test exception"));

        consumer.commitAsync(mockTopicPartitionOffset(), new MockCommitCallback());
        assertEquals(0, MockConsumerInterceptor.ON_COMMIT_COUNT.get());

        forceCommitCallbackInvocation();
        assertEquals(0, MockConsumerInterceptor.ON_COMMIT_COUNT.get());
    }

    @Test
    public void testRefreshCommittedOffsetsSuccess() {
        consumer = newConsumer();
        TopicPartition partition = new TopicPartition("t1", 1);
        Set<TopicPartition> partitions = Collections.singleton(partition);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = Collections.singletonMap(partition, new OffsetAndMetadata(10L));
        testRefreshCommittedOffsetsSuccess(partitions, committedOffsets);
    }

    @Test
    public void testRefreshCommittedOffsetsSuccessButNoCommittedOffsetsFound() {
        consumer = newConsumer();
        TopicPartition partition = new TopicPartition("t1", 1);
        Set<TopicPartition> partitions = Collections.singleton(partition);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = Collections.emptyMap();
        testRefreshCommittedOffsetsSuccess(partitions, committedOffsets);
    }

    @Test
    public void testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout() {
        consumer = newConsumer();
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(true);
    }

    @Test
    public void testRefreshCommittedOffsetsNotCalledIfNoGroupId() {
        // Create consumer without group id so committed offsets are not used for updating positions
        consumer = newConsumerWithoutGroupId();
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(false);
    }

    @Test
    public void testSubscribeGeneratesEvent() {
        consumer = newConsumer();
        String topic = "topic1";
        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(SubscriptionChangeEvent.class));
    }

    @Test
    public void testUnsubscribeGeneratesUnsubscribeEvent() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.unsubscribe();

        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeEvent.class));
    }

    @Test
    public void testSubscribeToEmptyListActsAsUnsubscribe() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.subscribe(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeEvent.class));
    }

    @Test
    public void testSubscribeToNullTopicCollection() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((List<String>) null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        consumer = newConsumer();
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        consumer = newConsumer();
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(emptyTopic)));
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNull() {
        final Properties props = requiredConsumerConfig();
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertFalse(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertFalse(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
        final Throwable exception = assertThrows(InvalidGroupIdException.class, consumer::groupMetadata);
        assertEquals(
            "To use the group management or offset commit APIs, you must " +
                "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.",
            exception.getMessage()
        );
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNotNull() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerConfigAndGroupId(groupId));
        consumer = newConsumer(config);

        final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

        assertEquals(groupId, groupMetadata.groupId());
        assertEquals(Optional.empty(), groupMetadata.groupInstanceId());
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNotNullAndGroupInstanceIdSet() {
        final String groupId = "consumerGroupA";
        final String groupInstanceId = "groupInstanceId1";
        final Properties props = requiredConsumerConfigAndGroupId(groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

        assertEquals(groupId, groupMetadata.groupId());
        assertEquals(Optional.of(groupInstanceId), groupMetadata.groupInstanceId());
        assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
        assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
    }

    private MemberStateListener captureGroupMetadataUpdateListener(final MockedStatic<RequestManagers> requestManagers) {
        ArgumentCaptor<MemberStateListener> applicationThreadMemberStateListener = ArgumentCaptor.forClass(MemberStateListener.class);
        requestManagers.verify(() -> RequestManagers.supplier(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            applicationThreadMemberStateListener.capture()
        ));
        return applicationThreadMemberStateListener.getValue();
    }

    @Test
    public void testGroupMetadataUpdate() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerConfigAndGroupId(groupId));
        try (final MockedStatic<RequestManagers> requestManagers = mockStatic(RequestManagers.class)) {
            consumer = newConsumer(config);
            final ConsumerGroupMetadata oldGroupMetadata = consumer.groupMetadata();
            final MemberStateListener groupMetadataUpdateListener = captureGroupMetadataUpdateListener(requestManagers);
            final int expectedMemberEpoch = 42;
            final String expectedMemberId = "memberId";
            groupMetadataUpdateListener.onMemberEpochUpdated(
                Optional.of(expectedMemberEpoch),
                Optional.of(expectedMemberId)
            );
            final ConsumerGroupMetadata newGroupMetadata = consumer.groupMetadata();
            assertEquals(oldGroupMetadata.groupId(), newGroupMetadata.groupId());
            assertEquals(expectedMemberId, newGroupMetadata.memberId());
            assertEquals(expectedMemberEpoch, newGroupMetadata.generationId());
            assertEquals(oldGroupMetadata.groupInstanceId(), newGroupMetadata.groupInstanceId());
        }
    }

    @Test
    public void testGroupMetadataIsResetAfterUnsubscribe() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerConfigAndGroupId(groupId));
        try (final MockedStatic<RequestManagers> requestManagers = mockStatic(RequestManagers.class)) {
            consumer = newConsumer(config);
            final MemberStateListener groupMetadataUpdateListener = captureGroupMetadataUpdateListener(requestManagers);
            consumer.subscribe(singletonList("topic"));
            final int memberEpoch = 42;
            final String memberId = "memberId";
            groupMetadataUpdateListener.onMemberEpochUpdated(Optional.of(memberEpoch), Optional.of(memberId));
            final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            assertNotEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
            assertNotEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        }
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.unsubscribe();

        final ConsumerGroupMetadata groupMetadataAfterUnsubscription = new ConsumerGroupMetadata(
            groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            Optional.empty()
        );
        assertEquals(groupMetadataAfterUnsubscription, consumer.groupMetadata());
    }

    /**
     * Tests that the consumer correctly invokes the callbacks for {@link ConsumerRebalanceListener} that was
     * specified. We don't go through the full effort to emulate heartbeats and correct group management here. We're
     * simply exercising the background {@link EventProcessor} does the correct thing when
     * {@link AsyncKafkaConsumer#poll(Duration)} is called.
     *
     * Note that we test {@link ConsumerRebalanceListener} that throws errors in its different callbacks. Failed
     * callback execution does <em>not</em> immediately errors. Instead, those errors are forwarded to the
     * application event thread for the {@link MembershipManagerImpl} to handle.
     */
    @ParameterizedTest
    @MethodSource("listenerCallbacksInvokeSource")
    public void testListenerCallbacksInvoke(List<ConsumerRebalanceListenerMethodName> methodNames,
                                            Optional<RuntimeException> revokedError,
                                            Optional<RuntimeException> assignedError,
                                            Optional<RuntimeException> lostError,
                                            int expectedRevokedCount,
                                            int expectedAssignedCount,
                                            int expectedLostCount,
                                            Optional<RuntimeException> expectedException
                                            ) {
        consumer = newConsumer();
        CounterConsumerRebalanceListener consumerRebalanceListener = new CounterConsumerRebalanceListener(
                revokedError,
                assignedError,
                lostError
        );
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        consumer.subscribe(Collections.singletonList("topic"), consumerRebalanceListener);
        SortedSet<TopicPartition> partitions = Collections.emptySortedSet();

        for (ConsumerRebalanceListenerMethodName methodName : methodNames) {
            CompletableBackgroundEvent<Void> e = new ConsumerRebalanceListenerCallbackNeededEvent(methodName, partitions);
            backgroundEventQueue.add(e);
        }

        // This will trigger the background event queue to process our background event message.
        // If any error is happening inside the rebalance callbacks, we expect the first exception to be thrown from poll.
        if (expectedException.isPresent()) {
            Exception exception = assertThrows(expectedException.get().getClass(), () -> consumer.poll(Duration.ZERO));
            assertEquals(expectedException.get().getMessage(), exception.getMessage());
            assertEquals(expectedException.get().getCause(), exception.getCause());
        } else {
            assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
        }

        assertEquals(expectedRevokedCount, consumerRebalanceListener.revokedCount());
        assertEquals(expectedAssignedCount, consumerRebalanceListener.assignedCount());
        assertEquals(expectedLostCount, consumerRebalanceListener.lostCount());
    }

    private static Stream<Arguments> listenerCallbacksInvokeSource() {
        Optional<RuntimeException> empty = Optional.empty();
        Optional<RuntimeException> error = Optional.of(new RuntimeException("Intentional error"));
        Optional<RuntimeException> kafkaException = Optional.of(new KafkaException("Intentional error"));
        Optional<RuntimeException> wrappedException = Optional.of(new KafkaException("User rebalance callback throws an error", error.get()));

        return Stream.of(
            // Tests if we don't have an event, the listener doesn't get called.
            Arguments.of(Collections.emptyList(), empty, empty, empty, 0, 0, 0, empty),

            // Tests if we get an event for a revocation, that we invoke our listener.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_REVOKED), empty, empty, empty, 1, 0, 0, empty),

            // Tests if we get an event for an assignment, that we invoke our listener.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_ASSIGNED), empty, empty, empty, 0, 1, 0, empty),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_LOST), empty, empty, empty, 0, 0, 1, empty),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_REVOKED), error, empty, empty, 1, 0, 0, wrappedException),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_ASSIGNED), empty, error, empty, 0, 1, 0, wrappedException),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_LOST), empty, empty, error, 0, 0, 1, wrappedException),

            // Tests that we invoke our listener even if it encounters an exception. Special case to test that a kafka exception is not wrapped.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_REVOKED), kafkaException, empty, empty, 1, 0, 0, kafkaException),
            Arguments.of(Collections.singletonList(ON_PARTITIONS_ASSIGNED), empty, kafkaException, empty, 0, 1, 0, kafkaException),
            Arguments.of(Collections.singletonList(ON_PARTITIONS_LOST), empty, empty, kafkaException, 0, 0, 1, kafkaException),

            // Tests if we get separate events for revocation and then assignment--AND our revocation throws an error--
            // we still invoke the listeners correctly and throw the error.
            Arguments.of(Arrays.asList(ON_PARTITIONS_REVOKED, ON_PARTITIONS_ASSIGNED), error, empty, empty, 1, 1, 0, wrappedException),

            // Tests if we get separate events for revocation and then assignment--AND both throws an error--
            // we still invoke the listeners correctly and throw the first error.
            Arguments.of(Arrays.asList(ON_PARTITIONS_REVOKED, ON_PARTITIONS_ASSIGNED), kafkaException, error, empty, 1, 1, 0, kafkaException)
        );
    }

    @Test
    public void testBackgroundError() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerConfigAndGroupId(groupId));
        consumer = newConsumer(config);

        final KafkaException expectedException = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorEvent = new ErrorEvent(expectedException);
        backgroundEventQueue.add(errorEvent);
        consumer.assign(singletonList(new TopicPartition("topic", 0)));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException.getMessage(), exception.getMessage());
    }

    @Test
    public void testMultipleBackgroundErrors() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerConfigAndGroupId(groupId));
        consumer = newConsumer(config);

        final KafkaException expectedException1 = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorEvent1 = new ErrorEvent(expectedException1);
        backgroundEventQueue.add(errorEvent1);
        final KafkaException expectedException2 = new KafkaException("Spam, Spam, Spam");
        final ErrorEvent errorEvent2 = new ErrorEvent(expectedException2);
        backgroundEventQueue.add(errorEvent2);
        consumer.assign(singletonList(new TopicPartition("topic", 0)));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException1.getMessage(), exception.getMessage());
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testGroupRemoteAssignorUnusedIfGroupIdUndefined() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertTrue(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @Test
    public void testGroupRemoteAssignorUnusedInGenericProtocol() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT));
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertTrue(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @Test
    public void testGroupRemoteAssignorUsedInConsumerProtocol() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertFalse(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @Test
    public void testGroupIdNull() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertFalse(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertFalse(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testGroupIdNotNullAndValid() {
        final Properties props = requiredConsumerConfigAndGroupId("consumerGroupA");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertTrue(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
        assertTrue(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testGroupIdEmpty() {
        testInvalidGroupId("");
    }

    @Test
    public void testGroupIdOnlyWhitespaces() {
        testInvalidGroupId("       ");
    }

    @Test
    public void testEnsurePollEventSentOnConsumerPoll() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        consumer = newConsumer(
                mock(FetchBuffer.class),
                new ConsumerInterceptors<>(Collections.emptyList()),
                mock(ConsumerRebalanceListenerInvoker.class),
                subscriptions,
                singletonList(new RoundRobinAssignor()),
                "group-id",
                "client-id");
        final TopicPartition tp = new TopicPartition("topic", 0);
        final List<ConsumerRecord<String, String>> records = singletonList(
                new ConsumerRecord<>("topic", 0, 2, "key1", "value1"));
        doAnswer(invocation -> Fetch.forPartition(tp, records, true))
                .when(fetchCollector)
                .collectFetch(Mockito.any(FetchBuffer.class));

        consumer.subscribe(singletonList("topic1"));
        consumer.poll(Duration.ofMillis(100));
        verify(applicationEventHandler).add(any(PollEvent.class));
    }

    private void testInvalidGroupId(final String groupId) {
        final Properties props = requiredConsumerConfigAndGroupId(groupId);
        final ConsumerConfig config = new ConsumerConfig(props);

        final Exception exception = assertThrows(
            KafkaException.class,
            () -> consumer = newConsumer(config)
        );

        assertEquals("Failed to construct kafka consumer", exception.getMessage());
    }

    private Properties requiredConsumerConfigAndGroupId(final String groupId) {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    private void testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(boolean committedOffsetsEnabled) {
        completeFetchedCommittedOffsetApplicationEventExceptionally(new TimeoutException());
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));

        consumer.assign(singleton(new TopicPartition("t1", 1)));

        consumer.poll(Duration.ZERO);

        verify(applicationEventHandler, atLeast(1))
            .addAndGet(ArgumentMatchers.isA(ValidatePositionsEvent.class), ArgumentMatchers.isA(Timer.class));

        if (committedOffsetsEnabled) {
            // Verify there was an FetchCommittedOffsets event and no ResetPositions event
            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class), ArgumentMatchers.isA(Timer.class));
            verify(applicationEventHandler, never())
                .addAndGet(ArgumentMatchers.isA(ResetPositionsEvent.class), ArgumentMatchers.isA(Timer.class));
        } else {
            // Verify there was not any FetchCommittedOffsets event but there should be a ResetPositions
            verify(applicationEventHandler, never())
                .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class), ArgumentMatchers.isA(Timer.class));
            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(ResetPositionsEvent.class), ArgumentMatchers.isA(Timer.class));
        }
    }

    private void testRefreshCommittedOffsetsSuccess(Set<TopicPartition> partitions,
                                                    Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        completeFetchedCommittedOffsetApplicationEventSuccessfully(committedOffsets);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());

        consumer.assign(partitions);

        consumer.poll(Duration.ZERO);

        verify(applicationEventHandler, atLeast(1))
            .addAndGet(ArgumentMatchers.isA(ValidatePositionsEvent.class), ArgumentMatchers.isA(Timer.class));
        verify(applicationEventHandler, atLeast(1))
            .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class), ArgumentMatchers.isA(Timer.class));
        verify(applicationEventHandler, atLeast(1))
            .addAndGet(ArgumentMatchers.isA(ResetPositionsEvent.class), ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testLongPollWaitIsLimited() {
        consumer = newConsumer();
        String topicName = "topic1";
        consumer.subscribe(singletonList(topicName));

        assertEquals(singleton(topicName), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());

        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );

        // On the first iteration, return no data; on the second, return two records
        doAnswer(invocation -> {
            // Mock the subscription being assigned as the first fetch is collected
            consumer.subscriptions().assignFromSubscribed(Collections.singleton(tp));
            return Fetch.empty();
        }).doAnswer(invocation -> {
            return Fetch.forPartition(tp, records, true);
        }).when(fetchCollector).collectFetch(any(FetchBuffer.class));

        // And then poll for up to 10000ms, which should return 2 records without timing out
        ConsumerRecords<?, ?> returnedRecords = consumer.poll(Duration.ofMillis(10000));
        assertEquals(2, returnedRecords.count());

        assertEquals(singleton(topicName), consumer.subscription());
        assertEquals(singleton(tp), consumer.assignment());
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(EventProcessor, Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} takes a bit of time to complete, but does within the timeout.
     */
    @Test
    public void testProcessBackgroundEventsWithInitialDelay() throws Exception {
        consumer = newConsumer();
        Timer timer = time.timer(1000);
        CompletableFuture<?> future = mock(CompletableFuture.class);
        CountDownLatch latch = new CountDownLatch(3);

        // Mock our call to Future.get(timeout) so that it mimics a delay of 200 milliseconds. Keep in mind that
        // the incremental timeout inside processBackgroundEvents is 100 seconds for each pass. Our first two passes
        // will exceed the incremental timeout, but the third will return.
        doAnswer(invocation -> {
            latch.countDown();

            if (latch.getCount() > 0) {
                long timeout = invocation.getArgument(0, Long.class);
                timer.sleep(timeout);
                throw new java.util.concurrent.TimeoutException("Intentional timeout");
            }

            future.complete(null);
            return null;
        }).when(future).get(any(Long.class), any(TimeUnit.class));

        try (EventProcessor<?> processor = mock(EventProcessor.class)) {
            consumer.processBackgroundEvents(processor, future, timer);

            // 800 is the 1000 ms timeout (above) minus the 200 ms delay for the two incremental timeouts/retries.
            assertEquals(800, timer.remainingMs());
        }
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(EventProcessor, Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} is already complete when invoked, so it doesn't have to wait.
     */
    @Test
    public void testProcessBackgroundEventsWithoutDelay() {
        consumer = newConsumer();
        Timer timer = time.timer(1000);

        // Create a future that is already completed.
        CompletableFuture<?> future = CompletableFuture.completedFuture(null);

        try (EventProcessor<?> processor = mock(EventProcessor.class)) {
            consumer.processBackgroundEvents(processor, future, timer);

            // Because we didn't need to perform a timed get, we should still have every last millisecond
            // of our initial timeout.
            assertEquals(1000, timer.remainingMs());
        }
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(EventProcessor, Future, Timer) processBackgroundEvents}
     * handles the case where the {@link Future} does not complete within the timeout.
     */
    @Test
    public void testProcessBackgroundEventsTimesOut() throws Exception {
        consumer = newConsumer();
        Timer timer = time.timer(1000);
        CompletableFuture<?> future = mock(CompletableFuture.class);

        doAnswer(invocation -> {
            long timeout = invocation.getArgument(0, Long.class);
            timer.sleep(timeout);
            throw new java.util.concurrent.TimeoutException("Intentional timeout");
        }).when(future).get(any(Long.class), any(TimeUnit.class));

        try (EventProcessor<?> processor = mock(EventProcessor.class)) {
            assertThrows(TimeoutException.class, () -> consumer.processBackgroundEvents(processor, future, timer));

            // Because we forced our mocked future to continuously time out, we should have no time remaining.
            assertEquals(0, timer.remainingMs());
        }
    }

    /**
     * Tests that calling {@link Thread#interrupt()} before {@link KafkaConsumer#poll(Duration)}
     * causes {@link InterruptException} to be thrown.
     */
    @Test
    public void testPollThrowsInterruptExceptionIfInterrupted() {
        consumer = newConsumer();
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        completeFetchedCommittedOffsetApplicationEventSuccessfully(offsets);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        consumer.assign(singleton(tp));

        // interrupt the thread and call poll
        try {
            Thread.currentThread().interrupt();
            assertThrows(InterruptException.class, () -> consumer.poll(Duration.ZERO));
        } finally {
            // clear interrupted state again since this thread may be reused by JUnit
            Thread.interrupted();
        }
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }
    
    private Map<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private Map<TopicPartition, OffsetAndTimestampInternal> mockOffsetAndTimestamp() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        Map<TopicPartition, OffsetAndTimestampInternal> offsetAndTimestamp = new HashMap<>();
        offsetAndTimestamp.put(t0, new OffsetAndTimestampInternal(5L, 1L, Optional.empty()));
        offsetAndTimestamp.put(t1, new OffsetAndTimestampInternal(6L, 3L, Optional.empty()));
        return offsetAndTimestamp;
    }

    private Map<TopicPartition, Long> mockTimestampToSearch() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t0, 1L);
        timestampToSearch.put(t1, 2L);
        return timestampToSearch;
    }

    private void completeCommitAsyncApplicationEventExceptionally(Exception ex) {
        doAnswer(invocation -> {
            AsyncCommitEvent event = invocation.getArgument(0);
            event.future().completeExceptionally(ex);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(AsyncCommitEvent.class));
    }

    private void completeCommitSyncApplicationEventExceptionally(Exception ex) {
        doAnswer(invocation -> {
            SyncCommitEvent event = invocation.getArgument(0);
            event.future().completeExceptionally(ex);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(SyncCommitEvent.class));
    }

    private void completeCommitAsyncApplicationEventSuccessfully() {
        doAnswer(invocation -> {
            AsyncCommitEvent event = invocation.getArgument(0);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(AsyncCommitEvent.class));
    }

    private void completeCommitSyncApplicationEventSuccessfully() {
        doAnswer(invocation -> {
            SyncCommitEvent event = invocation.getArgument(0);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(SyncCommitEvent.class));
    }

    private void completeFetchedCommittedOffsetApplicationEventSuccessfully(final Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        doReturn(committedOffsets)
            .when(applicationEventHandler)
            .addAndGet(any(FetchCommittedOffsetsEvent.class), any(Timer.class));
    }

    private void completeFetchedCommittedOffsetApplicationEventExceptionally(Exception ex) {
        doThrow(ex)
            .when(applicationEventHandler)
            .addAndGet(any(FetchCommittedOffsetsEvent.class), any(Timer.class));
    }

    private void completeUnsubscribeApplicationEventSuccessfully() {
        doAnswer(invocation -> {
            UnsubscribeEvent event = invocation.getArgument(0);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeEvent.class));
    }

    private void forceCommitCallbackInvocation() {
        // Invokes callback
        consumer.commitAsync();
    }
}

