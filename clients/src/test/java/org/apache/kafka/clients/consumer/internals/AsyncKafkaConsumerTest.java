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
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CheckAndUpdatePositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetOffsetEvent;
import org.apache.kafka.clients.consumer.internals.events.SeekUnvalidatedEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicRe2JPatternSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.events.UpdatePatternSubscriptionEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.internals.AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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
    private final CompletableEventReaper backgroundEventReaper = mock(CompletableEventReaper.class);

    @AfterEach
    public void resetAll() {
        backgroundEventQueue.clear();
        if (consumer != null) {
            try {
                consumer.close(Duration.ZERO);
            } catch (Exception e) {
                // best effort to clean up after each test, but may throw (ex. if callbacks where
                // throwing errors)
            }
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

    private AsyncKafkaConsumer<String, String> newConsumer(Properties props) {
        // disable auto-commit by default, so we don't need to handle SyncCommitEvent for each case
        if (!props.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }
        final ConsumerConfig config = new ConsumerConfig(props);
        return new AsyncKafkaConsumer<>(
            config,
            new StringDeserializer(),
            new StringDeserializer(),
            time,
            (a, b, c, d, e, f, g) -> applicationEventHandler,
            a -> backgroundEventReaper,
            (a, b, c, d, e, f, g) -> fetchCollector,
            (a, b, c, d) -> metadata,
            backgroundEventQueue
        );
    }

    private AsyncKafkaConsumer<String, String> newConsumer(ConsumerConfig config) {
        return new AsyncKafkaConsumer<>(
            config,
            new StringDeserializer(),
            new StringDeserializer(),
            time,
            (a, b, c, d, e, f, g) -> applicationEventHandler,
            a -> backgroundEventReaper,
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
        String groupId,
        String clientId,
        boolean autoCommitEnabled) {
        long retryBackoffMs = 100L;
        int defaultApiTimeoutMs = 1000;
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
            backgroundEventReaper,
            rebalanceListenerInvoker,
            new Metrics(),
            subscriptions,
            metadata,
            retryBackoffMs,
            defaultApiTimeoutMs,
            groupId,
            autoCommitEnabled);
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testFailOnClosedConsumer() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();
        consumer.close();
        final IllegalStateException res = assertThrows(IllegalStateException.class, consumer::assignment);
        assertEquals("This consumer has already been closed.", res.getMessage());
    }

    @Test
    public void testUnsubscribeWithInvalidTopicException() {
        consumer = newConsumer();
        backgroundEventQueue.add(new ErrorEvent(new InvalidTopicException("Invalid topic name")));
        completeUnsubscribeApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.unsubscribe());
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testCloseWithInvalidTopicException() {
        consumer = newConsumer();
        backgroundEventQueue.add(new ErrorEvent(new InvalidTopicException("Invalid topic name")));
        completeUnsubscribeApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testUnsubscribeWithTopicAuthorizationException() {
        consumer = newConsumer();
        backgroundEventQueue.add(new ErrorEvent(new TopicAuthorizationException(Set.of("test-topic"))));
        completeUnsubscribeApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.unsubscribe());
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testCloseWithTopicAuthorizationException() {
        consumer = newConsumer();
        backgroundEventQueue.add(new ErrorEvent(new TopicAuthorizationException(Set.of("test-topic"))));
        completeUnsubscribeApplicationEventSuccessfully();
        assertDoesNotThrow(() -> consumer.close());
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
        assertTrue(commitEvent.offsets().isPresent());
        assertEquals(offsets, commitEvent.offsets().get());

        commitEvent.future().complete(offsets);
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
    public void testCommitted() {
        time = new MockTime(1);
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = mockTopicPartitionOffset();
        completeFetchedCommittedOffsetApplicationEventSuccessfully(topicPartitionOffsets);

        assertEquals(topicPartitionOffsets, consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class));
        final Metric metric = consumer.metrics()
            .get(consumer.metricsRegistry().metricName("committed-time-ns-total", "consumer-metrics"));
        assertTrue((double) metric.metricValue() > 0);
    }

    @Test
    public void testCommittedExceptionThrown() {
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        when(applicationEventHandler.addAndGet(
            any(FetchCommittedOffsetsEvent.class))).thenAnswer(invocation -> {
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
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
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
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
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
            return Fetch.forPartition(tp, records, true, new OffsetAndMetadata(4, Optional.of(0), ""));
        }).when(fetchCollector).collectFetch(Mockito.any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
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
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
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

        completeTopicSubscriptionChangeEventSuccessfully();
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
        doReturn(Fetch.forPartition(tp, records, true, new OffsetAndMetadata(4, Optional.of(0), "")))
            .when(fetchCollector).collectFetch(any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
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
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(Collections.singleton(tp));
        completeSeekUnvalidatedEventSuccessfully();
        consumer.seek(tp, 20);
        consumer.commitAsync();

        CompletableApplicationEvent<Void> event = getLastEnqueuedEvent();
        return event.future();
    }

    // ArgumentCaptor's type-matching does not work reliably with Java 8, so we cannot directly capture the AsyncCommitEvent
    // Instead, we capture the super-class CompletableApplicationEvent and fetch the last captured event.
    private <T> CompletableApplicationEvent<T> getLastEnqueuedEvent() {
        final ArgumentCaptor<CompletableApplicationEvent<T>> eventArgumentCaptor = ArgumentCaptor.forClass(CompletableApplicationEvent.class);
        verify(applicationEventHandler, atLeast(1)).add(eventArgumentCaptor.capture());
        final List<CompletableApplicationEvent<T>> allValues = eventArgumentCaptor.getAllValues();
        return allValues.get(allValues.size() - 1);
    }

    private <T> CompletableApplicationEvent<T> addAndGetLastEnqueuedEvent() {
        final ArgumentCaptor<CompletableApplicationEvent<T>> eventArgumentCaptor = ArgumentCaptor.forClass(CompletableApplicationEvent.class);
        verify(applicationEventHandler, atLeast(1)).addAndGet(eventArgumentCaptor.capture());
        final List<CompletableApplicationEvent<T>> allValues = eventArgumentCaptor.getAllValues();
        return allValues.get(allValues.size() - 1);
    }

    @Test
    public void testEnsurePollExecutedCommitAsyncCallbacks() {
        consumer = newConsumer();
        MockCommitCallback callback = new MockCommitCallback();
        completeCommitAsyncApplicationEventSuccessfully();
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);

        completeAssignmentChangeEventSuccessfully();
        consumer.assign(Collections.singleton(new TopicPartition("foo", 0)));
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        assertMockCommitCallbackInvoked(() -> consumer.poll(Duration.ZERO),
            callback,
            null);
    }

    @Test
    public void testEnsureShutdownExecutedCommitAsyncCallbacks() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();
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
        completeUnsubscribeApplicationEventSuccessfully();
        doReturn(null).when(applicationEventHandler).addAndGet(any());
        consumer.close();
        verify(applicationEventHandler).add(any(CommitOnCloseEvent.class));
        verify(applicationEventHandler).addAndGet(any(LeaveGroupOnCloseEvent.class));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS})
    public void testCloseLeavesGroup(long timeoutMs) {
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        consumer = spy(newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            "group-id",
            "client-id",
            false));
        consumer.close(Duration.ofMillis(timeoutMs));
        verify(applicationEventHandler).addAndGet(any(LeaveGroupOnCloseEvent.class));
    }

    @Test
    public void testCloseLeavesGroupDespiteOnPartitionsLostError() {
        // If rebalance listener failed to execute during close, we still send the leave group,
        // and proceed with closing the consumer.
        Throwable rootError = new KafkaException("Intentional error");
        Set<TopicPartition> partitions = singleton(new TopicPartition("topic1", 0));
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        when(subscriptions.assignedPartitions()).thenReturn(partitions);
        ConsumerRebalanceListenerInvoker invoker = mock(ConsumerRebalanceListenerInvoker.class);
        doAnswer(invocation -> rootError).when(invoker).invokePartitionsLost(any(SortedSet.class));

        consumer = spy(newConsumer(
            mock(FetchBuffer.class),
            new ConsumerInterceptors<>(Collections.emptyList()),
            invoker,
            subscriptions,
            "group-id",
            "client-id",
            false));
        consumer.setGroupAssignmentSnapshot(partitions);

        Throwable t = assertThrows(KafkaException.class, () -> consumer.close(Duration.ZERO));
        assertNotNull(t.getCause());
        assertEquals(rootError, t.getCause());

        verify(applicationEventHandler).addAndGet(any(LeaveGroupOnCloseEvent.class));
    }

    @ParameterizedTest
    @ValueSource(longs = {0, ConsumerUtils.DEFAULT_CLOSE_TIMEOUT_MS})
    public void testCloseLeavesGroupDespiteInterrupt(long timeoutMs) {
        Set<TopicPartition> partitions = singleton(new TopicPartition("topic1", 0));
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        when(subscriptions.assignedPartitions()).thenReturn(partitions);
        when(applicationEventHandler.addAndGet(any(CompletableApplicationEvent.class))).thenThrow(InterruptException.class);
        consumer = spy(newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            "group-id",
            "client-id",
            false));

        Duration timeout = Duration.ofMillis(timeoutMs);

        try {
            assertThrows(InterruptException.class, () -> consumer.close(timeout));
        } finally {
            Thread.interrupted();
        }

        verify(applicationEventHandler).add(any(CommitOnCloseEvent.class));
        verify(applicationEventHandler).addAndGet(any(LeaveGroupOnCloseEvent.class));
    }

    @Test
    public void testCommitSyncAllConsumed() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            "group-id",
            "client-id",
            false);
        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(singleton("topic"), mock(ConsumerRebalanceListener.class));
        subscriptions.assignFromSubscribed(singleton(new TopicPartition("topic", 0)));
        completeSeekUnvalidatedEventSuccessfully();
        subscriptions.seek(new TopicPartition("topic", 0), 100);
        consumer.commitSyncAllConsumed(time.timer(100));

        ArgumentCaptor<SyncCommitEvent> eventCaptor = ArgumentCaptor.forClass(SyncCommitEvent.class);
        verify(applicationEventHandler).add(eventCaptor.capture());
        SyncCommitEvent capturedEvent = eventCaptor.getValue();
        assertFalse(capturedEvent.offsets().isPresent(), "Expected empty optional offsets");
    }

    @Test
    public void testAutoCommitSyncDisabled() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(
            mock(FetchBuffer.class),
            mock(ConsumerInterceptors.class),
            mock(ConsumerRebalanceListenerInvoker.class),
            subscriptions,
            "group-id",
            "client-id",
            false);
        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(singleton("topic"), mock(ConsumerRebalanceListener.class));
        subscriptions.assignFromSubscribed(singleton(new TopicPartition("topic", 0)));
        completeSeekUnvalidatedEventSuccessfully();
        subscriptions.seek(new TopicPartition("topic", 0), 100);
        completeUnsubscribeApplicationEventSuccessfully();
        consumer.close();
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
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(singleton(tp));
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().contains(tp));
        verify(applicationEventHandler).addAndGet(any(AssignmentChangeEvent.class));
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

        when(applicationEventHandler.addAndGet(any(ListOffsetsEvent.class))).thenAnswer(invocation -> {
            ListOffsetsEvent event = invocation.getArgument(0);
            Timer timer = time.timer(event.deadlineMs() - time.milliseconds());
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
        verify(applicationEventHandler).addAndGet(any(ListOffsetsEvent.class));
    }

    @Test
    public void testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure() {
        consumer = newConsumer();
        Set<TopicPartition> partitions = mockTopicPartitionOffset().keySet();
        Throwable eventProcessingFailure = new KafkaException("Unexpected failure " +
            "processing List Offsets event");
        doThrow(eventProcessingFailure).when(applicationEventHandler).addAndGet(
            any(ListOffsetsEvent.class));
        Throwable consumerError = assertThrows(KafkaException.class,
            () -> consumer.beginningOffsets(partitions,
                Duration.ofMillis(1)));
        assertEquals(eventProcessingFailure, consumerError);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class));
    }

    @Test
    public void testBeginningOffsetsTimeoutOnEventProcessingTimeout() {
        consumer = newConsumer();
        doThrow(new TimeoutException()).when(applicationEventHandler).addAndGet(any());
        assertThrows(TimeoutException.class,
            () -> consumer.beginningOffsets(
                Collections.singletonList(new TopicPartition("t1", 0)),
                Duration.ofMillis(1)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class));
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

        doReturn(expectedResult).when(applicationEventHandler).addAndGet(any());
        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ofMillis(1)));
        expectedResult.forEach((key, value) -> {
            OffsetAndTimestamp expected = value.buildOffsetAndTimestamp();
            assertEquals(expected, result.get(key));
        });
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class));
    }

    @Test
    public void testOffsetsForTimesTimeoutException() {
        consumer = newConsumer();
        long timeout = 100;
        doThrow(new TimeoutException("Event did not complete in time and was expired by the reaper"))
            .when(applicationEventHandler).addAndGet(any());

        Throwable t = assertThrows(
            TimeoutException.class,
            () -> consumer.offsetsForTimes(mockTimestampToSearch(), Duration.ofMillis(timeout)));
        assertEquals("Failed to get offsets by times in " + timeout + "ms", t.getMessage());
    }

    @Test
    public void testBeginningOffsetsTimeoutException() {
        consumer = newConsumer();
        long timeout = 100;
        doThrow(new TimeoutException("Event did not complete in time and was expired by the reaper"))
            .when(applicationEventHandler).addAndGet(any());

        Throwable t = assertThrows(
            TimeoutException.class,
            () -> consumer.beginningOffsets(Collections.singleton(new TopicPartition("topic", 5)),
                Duration.ofMillis(timeout)));
        assertEquals("Failed to get offsets by times in " + timeout + "ms", t.getMessage());
    }

    @Test
    public void testEndOffsetsTimeoutException() {
        consumer = newConsumer();
        long timeout = 100;
        doThrow(new TimeoutException("Event did not complete in time and was expired by the reaper"))
            .when(applicationEventHandler).addAndGet(any());

        Throwable t = assertThrows(
            TimeoutException.class,
            () -> consumer.endOffsets(Collections.singleton(new TopicPartition("topic", 5)),
                Duration.ofMillis(timeout)));
        assertEquals("Failed to get offsets by times in " + timeout + "ms", t.getMessage());
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
        verify(applicationEventHandler, never()).addAndGet(ArgumentMatchers.isA(ListOffsetsEvent.class));
    }

    @Test
    public void testWakeupCommitted() {
        consumer = newConsumer();
        final Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        doAnswer(invocation -> {
            CompletableApplicationEvent<?> event = invocation.getArgument(0);
            assertInstanceOf(FetchCommittedOffsetsEvent.class, event);
            assertTrue(event.future().isCompletedExceptionally());
            return ConsumerUtils.getResult(event.future());
        })
            .when(applicationEventHandler)
            .addAndGet(any(FetchCommittedOffsetsEvent.class));

        consumer.wakeup();
        assertThrows(WakeupException.class, () -> consumer.committed(offsets.keySet()));
        assertNull(consumer.wakeupTrigger().getPendingTask());
    }

    @Test
    public void testNoWakeupInCloseCommit() {
        TopicPartition tp = new TopicPartition("topic1", 0);
        Properties props = requiredConsumerConfigAndGroupId("consumer-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumer = newConsumer(props);
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(Collections.singleton(tp));
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeSeekUnvalidatedEventSuccessfully();
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
        completeUnsubscribeApplicationEventSuccessfully();
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
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(Collections.singleton(tp));
        completeSeekUnvalidatedEventSuccessfully();
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
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(Collections.singleton(tp));
        completeSeekUnvalidatedEventSuccessfully();
        consumer.seek(tp, 20);
        completeCommitAsyncApplicationEventSuccessfully();
        consumer.commitAsync(cb);

        completeUnsubscribeApplicationEventSuccessfully();
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
        completeUnsubscribeApplicationEventSuccessfully();

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
        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicSubscriptionChangeEvent.class));
    }

    @Test
    public void testSubscribePatternGeneratesEvent() {
        consumer = newConsumer();
        Pattern pattern = Pattern.compile("topic.*");
        completeTopicPatternSubscriptionChangeEventSuccessfully();
        consumer.subscribe(pattern);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicPatternSubscriptionChangeEvent.class));
    }

    @Test
    public void testUnsubscribeGeneratesUnsubscribeEvent() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.unsubscribe();

        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
        ArgumentCaptor<UnsubscribeEvent> eventCaptor = ArgumentCaptor.forClass(UnsubscribeEvent.class);
        verify(applicationEventHandler).add(eventCaptor.capture());

        // check the deadline is set to the default API timeout
        long deadline = time.milliseconds() + (int) ConsumerConfig.configDef().defaultValues().get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        assertTrue(eventCaptor.getValue().deadlineMs() <= deadline);
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
        consumer = newConsumer(requiredConsumerConfigAndGroupId(groupId));

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
        consumer = newConsumer(props);

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
        try (final MockedStatic<RequestManagers> requestManagers = mockStatic(RequestManagers.class)) {
            consumer = newConsumer(requiredConsumerConfigAndGroupId(groupId));
            final ConsumerGroupMetadata oldGroupMetadata = consumer.groupMetadata();
            final MemberStateListener groupMetadataUpdateListener = captureGroupMetadataUpdateListener(requestManagers);
            final int expectedMemberEpoch = 42;
            final String expectedMemberId = "memberId";
            groupMetadataUpdateListener.onMemberEpochUpdated(
                Optional.of(expectedMemberEpoch),
                expectedMemberId
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
        try (final MockedStatic<RequestManagers> requestManagers = mockStatic(RequestManagers.class)) {
            consumer = newConsumer(requiredConsumerConfigAndGroupId(groupId));
            final MemberStateListener groupMetadataUpdateListener = captureGroupMetadataUpdateListener(requestManagers);
            consumer.subscribe(singletonList("topic"));
            final int memberEpoch = 42;
            final String memberId = "memberId";
            groupMetadataUpdateListener.onMemberEpochUpdated(Optional.of(memberEpoch), memberId);
            final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
            assertNotEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
            assertNotEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        }
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.unsubscribe();

        final ConsumerGroupMetadata groupMetadataAfterUnsubscribe = new ConsumerGroupMetadata(
            groupId,
            JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID,
            Optional.empty()
        );
        assertEquals(groupMetadataAfterUnsubscribe, consumer.groupMetadata());
    }

    /**
     * Tests that the consumer correctly invokes the callbacks for {@link ConsumerRebalanceListener} that was
     * specified. We don't go through the full effort to emulate heartbeats and correct group management here. We're
     * simply exercising the background {@link EventProcessor} does the correct thing when
     * {@link AsyncKafkaConsumer#poll(Duration)} is called.
     *
     * Note that we test {@link ConsumerRebalanceListener} that throws errors in its different callbacks. Failed
     * callback execution does <em>not</em> immediately errors. Instead, those errors are forwarded to the
     * application event thread for the {@link ConsumerMembershipManager} to handle.
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
        completeTopicSubscriptionChangeEventSuccessfully();
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
            when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
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
        consumer = newConsumer(requiredConsumerConfigAndGroupId(groupId));

        final KafkaException expectedException = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorEvent = new ErrorEvent(expectedException);
        backgroundEventQueue.add(errorEvent);
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(singletonList(new TopicPartition("topic", 0)));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException.getMessage(), exception.getMessage());
    }

    @Test
    public void testMultipleBackgroundErrors() {
        final String groupId = "consumerGroupA";
        consumer = newConsumer(requiredConsumerConfigAndGroupId(groupId));

        final KafkaException expectedException1 = new KafkaException("Nobody expects the Spanish Inquisition");
        final ErrorEvent errorEvent1 = new ErrorEvent(expectedException1);
        backgroundEventQueue.add(errorEvent1);
        final KafkaException expectedException2 = new KafkaException("Spam, Spam, Spam");
        final ErrorEvent errorEvent2 = new ErrorEvent(expectedException2);
        backgroundEventQueue.add(errorEvent2);
        completeAssignmentChangeEventSuccessfully();
        consumer.assign(singletonList(new TopicPartition("topic", 0)));
        final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

        assertEquals(expectedException1.getMessage(), exception.getMessage());
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testGroupRemoteAssignorUnusedIfGroupIdUndefined() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        final ConsumerConfig config = new ConsumerConfig(props);
        consumer = newConsumer(config);

        assertTrue(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
    }

    @Test
    public void testGroupRemoteAssignorInClassicProtocol() {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT));
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        assertThrows(ConfigException.class, () -> new ConsumerConfig(props));
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
    public void testEnsurePollEventSentOnConsumerPoll() {
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), AutoOffsetResetStrategy.NONE);
        consumer = newConsumer(
                mock(FetchBuffer.class),
                new ConsumerInterceptors<>(Collections.emptyList()),
                mock(ConsumerRebalanceListenerInvoker.class),
                subscriptions,
                "group-id",
                "client-id",
                false);
        final TopicPartition tp = new TopicPartition("topic", 0);
        final List<ConsumerRecord<String, String>> records = singletonList(
                new ConsumerRecord<>("topic", 0, 2, "key1", "value1"));
        doAnswer(invocation -> Fetch.forPartition(tp, records, true, new OffsetAndMetadata(3, Optional.of(0), "")))
                .when(fetchCollector)
                .collectFetch(Mockito.any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);

        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(singletonList("topic1"));
        consumer.poll(Duration.ofMillis(100));
        verify(applicationEventHandler).add(any(PollEvent.class));
        verify(applicationEventHandler).add(any(CreateFetchRequestsEvent.class));
    }

    private Properties requiredConsumerConfigAndGroupId(final String groupId) {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    private void testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(boolean committedOffsetsEnabled) {
        completeFetchedCommittedOffsetApplicationEventExceptionally(new TimeoutException());
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);

        completeAssignmentChangeEventSuccessfully();
        consumer.assign(singleton(new TopicPartition("t1", 1)));

        consumer.poll(Duration.ZERO);

        verify(applicationEventHandler, atLeast(1))
            .addAndGet(ArgumentMatchers.isA(CheckAndUpdatePositionsEvent.class));
    }

    @Test
    public void testLongPollWaitIsLimited() {
        consumer = newConsumer();
        String topicName = "topic1";
        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(singletonList(topicName));

        assertEquals(singleton(topicName), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());

        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        final List<ConsumerRecord<String, String>> records = asList(
            new ConsumerRecord<>(topicName, partition, 2, "key1", "value1"),
            new ConsumerRecord<>(topicName, partition, 3, "key2", "value2")
        );
        final OffsetAndMetadata nextOffsetAndMetadata = new OffsetAndMetadata(4, Optional.of(0), "");

        // On the first iteration, return no data; on the second, return two records
        Set<TopicPartition> partitions = singleton(tp);
        doAnswer(invocation -> {
            // Mock the subscription being assigned as the first fetch is collected
            consumer.subscriptions().assignFromSubscribed(partitions);
            consumer.setGroupAssignmentSnapshot(partitions);
            return Fetch.empty();
        }).doAnswer(invocation ->
            Fetch.forPartition(tp, records, true, nextOffsetAndMetadata)
        ).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);

        // And then poll for up to 10000ms, which should return 2 records without timing out
        ConsumerRecords<?, ?> returnedRecords = consumer.poll(Duration.ofMillis(10000));
        assertEquals(2, returnedRecords.count());
        assertEquals(4, returnedRecords.nextOffsets().get(tp).offset());
        assertEquals(Optional.of(0), returnedRecords.nextOffsets().get(tp).leaderEpoch());

        assertEquals(singleton(topicName), consumer.subscription());
        assertEquals(partitions, consumer.assignment());
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(Future, Timer, Predicate) processBackgroundEvents}
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

        consumer.processBackgroundEvents(future, timer, e -> false);

        // 800 is the 1000 ms timeout (above) minus the 200 ms delay for the two incremental timeouts/retries.
        assertEquals(800, timer.remainingMs());
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(Future, Timer, Predicate) processBackgroundEvents}
     * handles the case where the {@link Future} is already complete when invoked, so it doesn't have to wait.
     */
    @Test
    public void testProcessBackgroundEventsWithoutDelay() {
        consumer = newConsumer();
        Timer timer = time.timer(1000);

        // Create a future that is already completed.
        CompletableFuture<?> future = CompletableFuture.completedFuture(null);

        consumer.processBackgroundEvents(future, timer, e -> false);

        // Because we didn't need to perform a timed get, we should still have every last millisecond
        // of our initial timeout.
        assertEquals(1000, timer.remainingMs());
    }

    /**
     * Tests {@link AsyncKafkaConsumer#processBackgroundEvents(Future, Timer, Predicate) processBackgroundEvents}
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

        assertThrows(TimeoutException.class, () -> consumer.processBackgroundEvents(future, timer, e -> false));

        // Because we forced our mocked future to continuously time out, we should have no time remaining.
        assertEquals(0, timer.remainingMs());
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
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
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

    @Test
    void testReaperInvokedInClose() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();
        consumer.close();
        verify(backgroundEventReaper).reap(backgroundEventQueue);
    }

    @Test
    void testReaperInvokedInUnsubscribe() {
        consumer = newConsumer();
        completeUnsubscribeApplicationEventSuccessfully();
        consumer.unsubscribe();
        verify(backgroundEventReaper).reap(time.milliseconds());
    }

    @Test
    void testReaperInvokedInPoll() {
        consumer = newConsumer();
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        completeTopicSubscriptionChangeEventSuccessfully();
        consumer.subscribe(Collections.singletonList("topic"));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        consumer.poll(Duration.ZERO);
        verify(backgroundEventReaper).reap(time.milliseconds());
    }

    @Test
    public void testUnsubscribeWithoutGroupId() {
        consumer = newConsumerWithoutGroupId();

        completeUnsubscribeApplicationEventSuccessfully();
        consumer.unsubscribe();
        verify(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeEvent.class));
    }

    @Test
    public void testSeekToBeginning() {
        Collection<TopicPartition> topics = Collections.singleton(new TopicPartition("test", 0));
        consumer = newConsumer();
        consumer.seekToBeginning(topics);
        CompletableApplicationEvent<Void> event = addAndGetLastEnqueuedEvent();
        ResetOffsetEvent resetOffsetEvent = assertInstanceOf(ResetOffsetEvent.class, event);
        assertEquals(topics, new HashSet<>(resetOffsetEvent.topicPartitions()));
        assertEquals(AutoOffsetResetStrategy.EARLIEST, resetOffsetEvent.offsetResetStrategy());
    }

    @Test
    public void testSeekToBeginningWithException() {
        Collection<TopicPartition> topics = Collections.singleton(new TopicPartition("test", 0));
        consumer = newConsumer();
        completeResetOffsetEventExceptionally(new TimeoutException());
        assertThrows(TimeoutException.class, () -> consumer.seekToBeginning(topics));
    }

    @Test
    public void testSeekToEndWithException() {
        Collection<TopicPartition> topics = Collections.singleton(new TopicPartition("test", 0));
        consumer = newConsumer();
        completeResetOffsetEventExceptionally(new TimeoutException());
        assertThrows(TimeoutException.class, () -> consumer.seekToEnd(topics));
    }

    @Test
    public void testSeekToEnd() {
        Collection<TopicPartition> topics = Collections.singleton(new TopicPartition("test", 0));
        consumer = newConsumer();
        consumer.seekToEnd(topics);
        CompletableApplicationEvent<Void> event = addAndGetLastEnqueuedEvent();
        ResetOffsetEvent resetOffsetEvent = assertInstanceOf(ResetOffsetEvent.class, event);
        assertEquals(topics, new HashSet<>(resetOffsetEvent.topicPartitions()));
        assertEquals(AutoOffsetResetStrategy.LATEST, resetOffsetEvent.offsetResetStrategy());
    }

    @Test
    public void testUpdatePatternSubscriptionEventGeneratedOnlyIfPatternUsed() {
        consumer = newConsumer();
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        when(applicationEventHandler.addAndGet(any(CheckAndUpdatePositionsEvent.class))).thenReturn(true);
        doReturn(LeaderAndEpoch.noLeaderOrEpoch()).when(metadata).currentLeader(any());
        completeAssignmentChangeEventSuccessfully();
        completeTopicPatternSubscriptionChangeEventSuccessfully();
        completeUnsubscribeApplicationEventSuccessfully();

        consumer.assign(singleton(new TopicPartition("topic1", 0)));
        consumer.poll(Duration.ZERO);
        verify(applicationEventHandler, never()).addAndGet(any(UpdatePatternSubscriptionEvent.class));

        consumer.unsubscribe();

        consumer.subscribe(Pattern.compile("t*"));
        consumer.poll(Duration.ZERO);
        verify(applicationEventHandler).addAndGet(any(UpdatePatternSubscriptionEvent.class));
    }

    @Test
    public void testSubscribeToRe2JPatternValidation() {
        consumer = newConsumer();

        Throwable t = assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((SubscriptionPattern) null));
        assertEquals("Topic pattern to subscribe to cannot be null", t.getMessage());

        t = assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(new SubscriptionPattern("")));
        assertEquals("Topic pattern to subscribe to cannot be empty", t.getMessage());

        assertDoesNotThrow(() -> consumer.subscribe(new SubscriptionPattern("t*")));

        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(new SubscriptionPattern("t*"), null));
        assertDoesNotThrow(() -> consumer.subscribe(new SubscriptionPattern("t*"), mock(ConsumerRebalanceListener.class)));
    }

    @Test
    public void testSubscribeToRe2JPatternThrowsIfNoGroupId() {
        consumer = newConsumer(requiredConsumerConfig());
        assertThrows(InvalidGroupIdException.class, () -> consumer.subscribe(new SubscriptionPattern("t*")));
        assertThrows(InvalidGroupIdException.class, () -> consumer.subscribe(new SubscriptionPattern("t*"),
            mock(ConsumerRebalanceListener.class)));
    }

    @Test
    public void testSubscribeToRe2JPatternGeneratesEvent() {
        consumer = newConsumer();
        completeTopicRe2JPatternSubscriptionChangeEventSuccessfully();

        consumer.subscribe(new SubscriptionPattern("t*"));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicRe2JPatternSubscriptionChangeEvent.class));

        clearInvocations(applicationEventHandler);
        consumer.subscribe(new SubscriptionPattern("t*"), mock(ConsumerRebalanceListener.class));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicRe2JPatternSubscriptionChangeEvent.class));
    }

    // SubscriptionPattern is supported as of ConsumerGroupHeartbeatRequest v1. Clients using subscribe
    // (SubscribePattern) against older broker versions should get UnsupportedVersionException on poll after subscribe
    @Test
    public void testSubscribePatternAgainstBrokerNotSupportingRegex() throws InterruptedException {
        final Properties props = requiredConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final ConsumerConfig config = new ConsumerConfig(props);

        ConsumerMetadata metadata = new ConsumerMetadata(0, 0, Long.MAX_VALUE, false, false,
            mock(SubscriptionState.class), new LogContext(), new ClusterResourceListeners());
        MockClient client = new MockClient(time, metadata);
        MetadataResponse initialMetadata = RequestTestUtils.metadataUpdateWithIds(1, Map.of("topic1", 2),
            Map.of("topic1", Uuid.randomUuid()));
        client.updateMetadata(initialMetadata);
        // ConsumerGroupHeartbeat v0 does not support broker-side regex resolution
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.CONSUMER_GROUP_HEARTBEAT.id, (short) 0, (short) 0));

        // Mock response to find coordinator
        Node node = metadata.fetch().nodes().get(0);
        client.prepareResponseFrom(FindCoordinatorResponse.prepareResponse(Errors.NONE, "group-id", node), node);

        // Mock HB response (needed so that the MockClient builds the request)
        ConsumerGroupHeartbeatResponse result =
            new ConsumerGroupHeartbeatResponse(new ConsumerGroupHeartbeatResponseData()
                .setMemberId("")
                .setMemberEpoch(0));
        Node coordinator = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        client.prepareResponseFrom(result, coordinator);

        SubscriptionState subscriptionState = mock(SubscriptionState.class);

        consumer = new AsyncKafkaConsumer<>(
            new LogContext(),
            time,
            config,
            new StringDeserializer(),
            new StringDeserializer(),
            client,
            subscriptionState,
            metadata
        );
        completeTopicRe2JPatternSubscriptionChangeEventSuccessfully();

        SubscriptionPattern pattern = new SubscriptionPattern("t*");
        consumer.subscribe(pattern);
        when(subscriptionState.subscriptionPattern()).thenReturn(pattern);
        TestUtils.waitForCondition(() -> {
            try {
                // The request is generated in the background thread so allow for that
                // async operation to happen to detect the failure.
                consumer.poll(Duration.ZERO);
                return false;
            } catch (UnsupportedVersionException e) {
                return true;
            }
        }, "Consumer did not throw the expected UnsupportedVersionException on poll");
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

    private void completeResetOffsetEventExceptionally(Exception ex) {
        doThrow(ex).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ResetOffsetEvent.class));
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
            .addAndGet(any(FetchCommittedOffsetsEvent.class));

        doAnswer(invocation -> {
            FetchCommittedOffsetsEvent event = invocation.getArgument(0);
            event.future().complete(committedOffsets);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(FetchCommittedOffsetsEvent.class));
    }

    private void completeFetchedCommittedOffsetApplicationEventExceptionally(Exception ex) {
        doThrow(ex)
            .when(applicationEventHandler)
            .addAndGet(any(FetchCommittedOffsetsEvent.class));
    }

    private void completeUnsubscribeApplicationEventSuccessfully() {
        doAnswer(invocation -> {
            UnsubscribeEvent event = invocation.getArgument(0);
            consumer.subscriptions().unsubscribe();
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeEvent.class));
    }

    private void completeAssignmentChangeEventSuccessfully() {
        doAnswer(invocation -> {
            AssignmentChangeEvent event = invocation.getArgument(0);
            HashSet<TopicPartition> partitions = new HashSet<>(event.partitions());
            consumer.subscriptions().assignFromUser(partitions);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(AssignmentChangeEvent.class));
    }

    private void completeTopicSubscriptionChangeEventSuccessfully() {
        doAnswer(invocation -> {
            TopicSubscriptionChangeEvent event = invocation.getArgument(0);
            consumer.subscriptions().subscribe(event.topics(), event.listener());
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicSubscriptionChangeEvent.class));
    }

    private void completeTopicPatternSubscriptionChangeEventSuccessfully() {
        doAnswer(invocation -> {
            TopicPatternSubscriptionChangeEvent event = invocation.getArgument(0);
            consumer.subscriptions().subscribe(event.pattern(), event.listener());
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicPatternSubscriptionChangeEvent.class));
    }

    private void completeTopicRe2JPatternSubscriptionChangeEventSuccessfully() {
        doAnswer(invocation -> {
            TopicRe2JPatternSubscriptionChangeEvent event = invocation.getArgument(0);
            consumer.subscriptions().subscribe(event.pattern(), event.listener());
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(TopicRe2JPatternSubscriptionChangeEvent.class));
    }

    private void completeSeekUnvalidatedEventSuccessfully() {
        doAnswer(invocation -> {
            SeekUnvalidatedEvent event = invocation.getArgument(0);
            SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                    event.offset(),
                    event.offsetEpoch(),
                    metadata.currentLeader(event.partition())
            );
            consumer.subscriptions().seekUnvalidated(event.partition(), newPosition);
            event.future().complete(null);
            return null;
        }).when(applicationEventHandler).addAndGet(ArgumentMatchers.isA(SeekUnvalidatedEvent.class));
    }

    private void forceCommitCallbackInvocation() {
        // Invokes callback
        consumer.commitAsync();
    }
}
