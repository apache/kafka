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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.GroupMetadataUpdateEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.SubscriptionChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_LOST;
import static org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncKafkaConsumerTest {

    private AsyncKafkaConsumer<?, ?> consumer;
    private FetchCollector<?, ?> fetchCollector;
    private ConsumerTestBuilder.AsyncKafkaConsumerTestBuilder testBuilder;
    private ApplicationEventHandler applicationEventHandler;
    private SubscriptionState subscriptions;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;

    @BeforeEach
    public void setup() {
        // By default, the consumer is part of a group and autoCommit is enabled.
        setup(ConsumerTestBuilder.createDefaultGroupInformation(), true);
    }

    private void setup(Optional<ConsumerTestBuilder.GroupInformation> groupInfo, boolean enableAutoCommit) {
        testBuilder = new ConsumerTestBuilder.AsyncKafkaConsumerTestBuilder(groupInfo, enableAutoCommit, true);
        applicationEventHandler = testBuilder.applicationEventHandler;
        consumer = testBuilder.consumer;
        fetchCollector = testBuilder.fetchCollector;
        subscriptions = testBuilder.subscriptions;
        backgroundEventQueue = testBuilder.backgroundEventQueue;
    }

    @AfterEach
    public void cleanup() {
        if (testBuilder != null) {
            shutDown();
        }
    }

    private void shutDown() {
        prepAutocommitOnClose();
        testBuilder.close();
    }

    private void resetWithEmptyGroupId() {
        // Create a consumer that is not configured as part of a group.
        cleanup();
        setup(Optional.empty(), false);
    }

    private void resetWithAutoCommitEnabled() {
        cleanup();
        setup(ConsumerTestBuilder.createDefaultGroupInformation(), true);
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testSuccessfulStartupShutdownWithAutoCommit() {
        resetWithAutoCommitEnabled();
        TopicPartition tp = new TopicPartition("topic", 0);
        consumer.assign(singleton(tp));
        consumer.seek(tp, 100);
        prepAutocommitOnClose();
    }

    @Test
    public void testInvalidGroupId() {
        // Create consumer without group id
        resetWithEmptyGroupId();
        assertThrows(InvalidGroupIdException.class, () -> consumer.committed(new HashSet<>()));
    }

    @Test
    public void testFailOnClosedConsumer() {
        consumer.close();
        final IllegalStateException res = assertThrows(IllegalStateException.class, consumer::assignment);
        assertEquals("This consumer has already been closed.", res.getMessage());
    }

    @Test
    public void testCommitAsync_NullCallback() throws InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        doReturn(future).when(consumer).commit(offsets, false);
        consumer.commitAsync(offsets, null);
        future.complete(null);
        TestUtils.waitForCondition(future::isDone,
                2000,
                "commit future should complete");

        assertFalse(future.isCompletedExceptionally());
    }

    @ParameterizedTest
    @MethodSource("commitExceptionSupplier")
    public void testCommitAsync_UserSuppliedCallback(Exception exception) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        doReturn(future).when(consumer).commit(offsets, false);
        MockCommitCallback callback = new MockCommitCallback();
        assertDoesNotThrow(() -> consumer.commitAsync(offsets, callback));

        if (exception == null) {
            future.complete(null);
            consumer.maybeInvokeCommitCallbacks();
            assertNull(callback.exception);
        } else {
            future.completeExceptionally(exception);
            consumer.maybeInvokeCommitCallbacks();
            assertSame(exception.getClass(), callback.exception.getClass());
        }
    }

    private static Stream<Exception> commitExceptionSupplier() {
        return Stream.of(
                null,  // For the successful completion scenario
                new KafkaException("Test exception"),
                new GroupAuthorizationException("Group authorization exception"));
    }

    @Test
    public void testFencedInstanceException() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        doReturn(future).when(consumer).commit(new HashMap<>(), false);
        assertDoesNotThrow(() -> consumer.commitAsync());
        future.completeExceptionally(Errors.FENCED_INSTANCE_ID.exception());
    }

    @Test
    public void testCommitted() {
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.complete(offsets);

        try (MockedConstruction<FetchCommittedOffsetsApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            assertDoesNotThrow(() -> consumer.committed(offsets.keySet(), Duration.ofMillis(1000)));
            verify(applicationEventHandler).add(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class));
        }
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

        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.complete(topicPartitionOffsets);

        try (MockedConstruction<FetchCommittedOffsetsApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            assertDoesNotThrow(() -> consumer.committed(topicPartitionOffsets.keySet(), Duration.ofMillis(1000)));
        }
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t2, 3);
        verify(applicationEventHandler).add(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class));
    }

    @Test
    public void testCommitted_ExceptionThrown() {
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.completeExceptionally(new KafkaException("Test exception"));

        try (MockedConstruction<FetchCommittedOffsetsApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            assertThrows(KafkaException.class, () -> consumer.committed(offsets.keySet(), Duration.ofMillis(1000)));
            verify(applicationEventHandler).add(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class));
        }
    }

    @Test
    public void testWakeupBeforeCallingPoll() {
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doReturn(Fetch.empty()).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        consumer.assign(singleton(tp));

        consumer.wakeup();

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterEmptyFetch() {
        final String topicName = "foo";
        final int partition = 3;
        final TopicPartition tp = new TopicPartition(topicName, partition);
        doAnswer(invocation -> {
            consumer.wakeup();
            return Fetch.empty();
        }).when(fetchCollector).collectFetch(any(FetchBuffer.class));
        Map<TopicPartition, OffsetAndMetadata> offsets = mkMap(mkEntry(tp, new OffsetAndMetadata(1)));
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        consumer.assign(singleton(tp));

        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ofMinutes(1)));
        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testWakeupAfterNonEmptyFetch() {
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
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        consumer.assign(singleton(tp));

        // since wakeup() is called when the non-empty fetch is returned the wakeup should be ignored
        assertDoesNotThrow(() -> consumer.poll(Duration.ofMinutes(1)));
        // the previously ignored wake-up should not be ignored in the next call
        assertThrows(WakeupException.class, () -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testClearWakeupTriggerAfterPoll() {
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
        doReturn(offsets).when(applicationEventHandler).addAndGet(any(FetchCommittedOffsetsApplicationEvent.class), any(Timer.class));
        consumer.assign(singleton(tp));

        consumer.poll(Duration.ZERO);

        assertDoesNotThrow(() -> consumer.poll(Duration.ZERO));
    }

    @Test
    public void testEnsureCallbackExecutedByApplicationThread() {
        final String currentThread = Thread.currentThread().getName();
        ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor();
        MockCommitCallback callback = new MockCommitCallback();
        CountDownLatch latch = new CountDownLatch(1);  // Initialize the latch with a count of 1
        try {
            CompletableFuture<Void> future = new CompletableFuture<>();
            doReturn(future).when(consumer).commit(new HashMap<>(), false);
            assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
            // Simulating some background work
            backgroundExecutor.submit(() -> {
                future.complete(null);
                latch.countDown();
            });
            latch.await();
            assertEquals(1, consumer.callbacks());
            consumer.maybeInvokeCommitCallbacks();
            assertEquals(currentThread, callback.completionThread);
        } catch (Exception e) {
            fail("Not expecting an exception");
        } finally {
            backgroundExecutor.shutdown();
        }
    }

    @Test
    public void testEnsureCommitSyncExecutedCommitAsyncCallbacks() {
        MockCommitCallback callback = new MockCommitCallback();
        CompletableFuture<Void> future = new CompletableFuture<>();
        doReturn(future).when(consumer).commit(new HashMap<>(), false);
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        future.completeExceptionally(new NetworkException("Test exception"));
        assertMockCommitCallbackInvoked(() -> consumer.commitSync(),
            callback,
            Errors.NETWORK_EXCEPTION);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPollLongThrowsException() {
        Exception e = assertThrows(UnsupportedOperationException.class, () -> consumer.poll(0L));
        assertEquals("Consumer.poll(long) is not supported when \"group.protocol\" is \"consumer\". " +
            "This method is deprecated and will be removed in the next major release.", e.getMessage());
    }

    @Test
    public void testCommitSyncLeaderEpochUpdate() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));

        consumer.assign(Arrays.asList(t0, t1));

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        commitFuture.complete(null);

        try (MockedConstruction<CommitApplicationEvent> ignored = commitEventMocker(commitFuture)) {
            assertDoesNotThrow(() -> consumer.commitSync(topicPartitionOffsets));
        }
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t1, 1);
        verify(applicationEventHandler).add(ArgumentMatchers.isA(CommitApplicationEvent.class));
    }

    @Test
    public void testCommitAsyncLeaderEpochUpdate() {
        MockCommitCallback callback = new MockCommitCallback();
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L, Optional.of(2), ""));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L, Optional.of(1), ""));

        consumer.assign(Arrays.asList(t0, t1));

        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        commitFuture.complete(null);

        try (MockedConstruction<CommitApplicationEvent> ignored = commitEventMocker(commitFuture)) {
            assertDoesNotThrow(() -> consumer.commitAsync(topicPartitionOffsets, callback));
        }
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t0, 2);
        verify(testBuilder.metadata).updateLastSeenEpochIfNewer(t1, 1);
        verify(applicationEventHandler).add(ArgumentMatchers.isA(CommitApplicationEvent.class));
    }

    @Test
    public void testEnsurePollExecutedCommitAsyncCallbacks() {
        MockCommitCallback callback = new MockCommitCallback();
        CompletableFuture<Void> future = new CompletableFuture<>();
        consumer.assign(Collections.singleton(new TopicPartition("foo", 0)));
        doReturn(future).when(consumer).commit(new HashMap<>(), false);
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        future.complete(null);
        assertMockCommitCallbackInvoked(() -> consumer.poll(Duration.ZERO),
            callback,
            null);
    }

    @Test
    public void testEnsureShutdownExecutedCommitAsyncCallbacks() {
        MockCommitCallback callback = new MockCommitCallback();
        CompletableFuture<Void> future = new CompletableFuture<>();
        doReturn(future).when(consumer).commit(new HashMap<>(), false);
        assertDoesNotThrow(() -> consumer.commitAsync(new HashMap<>(), callback));
        future.complete(null);
        assertMockCommitCallbackInvoked(() -> consumer.close(),
            callback,
            null);
    }

    private void assertMockCommitCallbackInvoked(final Executable task,
                                                 final MockCommitCallback callback,
                                                 final Errors errors) {
        assertDoesNotThrow(task);
        assertEquals(1, callback.invoked);
        if (errors == null)
            assertNull(callback.exception);
        else if (errors.exception() instanceof RetriableException)
            assertTrue(callback.exception instanceof RetriableCommitFailedException);
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
    /**
     * This is a rather ugly bit of code. Not my choice :(
     *
     * <p/>
     *
     * Inside the {@link org.apache.kafka.clients.consumer.Consumer#committed(Set, Duration)} call we create an
     * instance of {@link FetchCommittedOffsetsApplicationEvent} that holds the partitions and internally holds a
     * {@link CompletableFuture}. We want to test different behaviours of the {@link Future#get()}, such as
     * returning normally, timing out, throwing an error, etc. By mocking the construction of the event object that
     * is created, we can affect that behavior.
     */
    private static MockedConstruction<FetchCommittedOffsetsApplicationEvent> offsetFetchEventMocker(CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        // This "answer" is where we pass the future to be invoked by the ConsumerUtils.getResult() method
        Answer<Map<TopicPartition, OffsetAndMetadata>> getInvocationAnswer = invocation -> {
            // This argument captures the actual argument value that was passed to the event's get() method, so we
            // just "forward" that value to our mocked call
            Timer timer = invocation.getArgument(0);
            return ConsumerUtils.getResult(future, timer);
        };

        MockedConstruction.MockInitializer<FetchCommittedOffsetsApplicationEvent> mockInitializer = (mock, ctx) -> {
            // When the event's get() method is invoked, we call the "answer" method just above
            when(mock.get(any())).thenAnswer(getInvocationAnswer);

            // When the event's type() method is invoked, we have to return the type as it will be null in the mock
            when(mock.type()).thenReturn(ApplicationEvent.Type.FETCH_COMMITTED_OFFSETS);

            // This is needed for the WakeupTrigger code that keeps track of the active task
            when(mock.future()).thenReturn(future);
        };

        return mockConstruction(FetchCommittedOffsetsApplicationEvent.class, mockInitializer);
    }

    private static MockedConstruction<CommitApplicationEvent> commitEventMocker(CompletableFuture<Void> future) {
        Answer<Void> getInvocationAnswer = invocation -> {
            Timer timer = invocation.getArgument(0);
            return ConsumerUtils.getResult(future, timer);
        };

        MockedConstruction.MockInitializer<CommitApplicationEvent> mockInitializer = (mock, ctx) -> {
            when(mock.get(any())).thenAnswer(getInvocationAnswer);
            when(mock.type()).thenReturn(ApplicationEvent.Type.COMMIT);
            when(mock.future()).thenReturn(future);
        };

        return mockConstruction(CommitApplicationEvent.class, mockInitializer);
    }

    @Test
    public void testAssign() {
        final TopicPartition tp = new TopicPartition("foo", 3);
        consumer.assign(singleton(tp));
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().contains(tp));
        verify(applicationEventHandler).add(any(AssignmentChangeApplicationEvent.class));
        verify(applicationEventHandler).add(any(NewTopicsMetadataUpdateRequestEvent.class));
    }

    @Test
    public void testAssignOnNullTopicPartition() {
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(null));
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        consumer.assign(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testAssignOnNullTopicInPartition() {
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition(null, 0))));
    }

    @Test
    public void testAssignOnEmptyTopicInPartition() {
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition("  ", 0))));
    }

    @Test
    public void testBeginningOffsetsFailsIfNullPartitions() {
        assertThrows(NullPointerException.class, () -> consumer.beginningOffsets(null,
                Duration.ofMillis(1)));
    }

    @Test
    public void testBeginningOffsets() {
        Map<TopicPartition, OffsetAndTimestamp> expectedOffsetsAndTimestamp =
                mockOffsetAndTimestamp();
        Set<TopicPartition> partitions = expectedOffsetsAndTimestamp.keySet();
        doReturn(expectedOffsetsAndTimestamp).when(applicationEventHandler).addAndGet(any(), any());
        Map<TopicPartition, Long> result =
                assertDoesNotThrow(() -> consumer.beginningOffsets(partitions,
                        Duration.ofMillis(1)));
        Map<TopicPartition, Long> expectedOffsets = expectedOffsetsAndTimestamp.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        assertEquals(expectedOffsets, result);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure() {
        Set<TopicPartition> partitions = mockTopicPartitionOffset().keySet();
        Throwable eventProcessingFailure = new KafkaException("Unexpected failure " +
                "processing List Offsets event");
        doThrow(eventProcessingFailure).when(applicationEventHandler).addAndGet(any(), any());
        Throwable consumerError = assertThrows(KafkaException.class,
                () -> consumer.beginningOffsets(partitions,
                        Duration.ofMillis(1)));
        assertEquals(eventProcessingFailure, consumerError);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsTimeoutOnEventProcessingTimeout() {
        doThrow(new TimeoutException()).when(applicationEventHandler).addAndGet(any(), any());
        assertThrows(TimeoutException.class,
                () -> consumer.beginningOffsets(
                        Collections.singletonList(new TopicPartition("t1", 0)),
                        Duration.ofMillis(1)));
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testOffsetsForTimesOnNullPartitions() {
        assertThrows(NullPointerException.class, () -> consumer.offsetsForTimes(null,
                Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimesFailsOnNegativeTargetTimes() {
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
        Map<TopicPartition, OffsetAndTimestamp> expectedResult = mockOffsetAndTimestamp();
        Map<TopicPartition, Long> timestampToSearch = mockTimestampToSearch();

        doReturn(expectedResult).when(applicationEventHandler).addAndGet(any(), any());
        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ofMillis(1)));
        assertEquals(expectedResult, result);
        verify(applicationEventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    // This test ensures same behaviour as the current consumer when offsetsForTimes is called
    // with 0 timeout. It should return map with all requested partitions as keys, with null
    // OffsetAndTimestamp as value.
    @Test
    public void testOffsetsForTimesWithZeroTimeout() {
        TopicPartition tp = new TopicPartition("topic1", 0);
        Map<TopicPartition, OffsetAndTimestamp> expectedResult =
                Collections.singletonMap(tp, null);
        Map<TopicPartition, Long> timestampToSearch = Collections.singletonMap(tp, 5L);

        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch,
                        Duration.ofMillis(0)));
        assertEquals(expectedResult, result);
        verify(applicationEventHandler, never()).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testWakeup_committed() {
        consumer.wakeup();
        assertThrows(WakeupException.class, () -> consumer.committed(mockTopicPartitionOffset().keySet()));
        assertNoPendingWakeup(consumer.wakeupTrigger());
    }

    @Test
    public void testRefreshCommittedOffsetsSuccess() {
        TopicPartition partition = new TopicPartition("t1", 1);
        Set<TopicPartition> partitions = Collections.singleton(partition);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = Collections.singletonMap(partition, new OffsetAndMetadata(10L));
        testRefreshCommittedOffsetsSuccess(partitions, committedOffsets);
    }

    @Test
    public void testRefreshCommittedOffsetsSuccessButNoCommittedOffsetsFound() {
        TopicPartition partition = new TopicPartition("t1", 1);
        Set<TopicPartition> partitions = Collections.singleton(partition);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = Collections.emptyMap();
        testRefreshCommittedOffsetsSuccess(partitions, committedOffsets);
    }

    @Test
    public void testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout() {
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(true);
    }

    @Test
    public void testRefreshCommittedOffsetsNotCalledIfNoGroupId() {
        // Create consumer without group id so committed offsets are not used for updating positions
        resetWithEmptyGroupId();
        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(false);
    }

    @Test
    public void testSubscribeGeneratesEvent() {
        String topic = "topic1";
        consumer.subscribe(singletonList(topic));
        assertEquals(singleton(topic), consumer.subscription());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(SubscriptionChangeApplicationEvent.class));
    }

    @Test
    public void testUnsubscribeGeneratesUnsubscribeEvent() {
        consumer.unsubscribe();

        // Verify the unsubscribe event was generated and mock its completion.
        final ArgumentCaptor<UnsubscribeApplicationEvent> captor = ArgumentCaptor.forClass(UnsubscribeApplicationEvent.class);
        verify(applicationEventHandler).add(captor.capture());
        UnsubscribeApplicationEvent unsubscribeApplicationEvent = captor.getValue();
        unsubscribeApplicationEvent.future().complete(null);

        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testSubscribeToEmptyListActsAsUnsubscribe() {
        consumer.subscribe(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
        verify(applicationEventHandler).add(ArgumentMatchers.isA(UnsubscribeApplicationEvent.class));
    }

    @Test
    public void testSubscribeToNullTopicCollection() {
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe((List<String>) null));
    }

    @Test
    public void testSubscriptionOnNullTopic() {
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(null)));
    }

    @Test
    public void testSubscriptionOnEmptyTopic() {
        String emptyTopic = "  ";
        assertThrows(IllegalArgumentException.class, () -> consumer.subscribe(singletonList(emptyTopic)));
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNull() {
        final Properties props = requiredConsumerProperties();
        final ConsumerConfig config = new ConsumerConfig(props);
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {

            assertFalse(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            assertFalse(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
            final Throwable exception = assertThrows(InvalidGroupIdException.class, consumer::groupMetadata);
            assertEquals(
                "To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.",
                exception.getMessage()
            );
        }
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNotNull() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {

            final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

            assertEquals(groupId, groupMetadata.groupId());
            assertEquals(Optional.empty(), groupMetadata.groupInstanceId());
            assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
            assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        }
    }

    @Test
    public void testGroupMetadataAfterCreationWithGroupIdIsNotNullAndGroupInstanceIdSet() {
        final String groupId = "consumerGroupA";
        final String groupInstanceId = "groupInstanceId1";
        final Properties props = requiredConsumerPropertiesAndGroupId(groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        final ConsumerConfig config = new ConsumerConfig(props);
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {

            final ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();

            assertEquals(groupId, groupMetadata.groupId());
            assertEquals(Optional.of(groupInstanceId), groupMetadata.groupInstanceId());
            assertEquals(JoinGroupRequest.UNKNOWN_GENERATION_ID, groupMetadata.generationId());
            assertEquals(JoinGroupRequest.UNKNOWN_MEMBER_ID, groupMetadata.memberId());
        }
    }

    @Test
    public void testGroupMetadataUpdateSingleCall() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer(), backgroundEventQueue)) {
            final int generation = 1;
            final String memberId = "newMemberId";
            final ConsumerGroupMetadata expectedGroupMetadata = new ConsumerGroupMetadata(
                groupId,
                generation,
                memberId,
                Optional.empty()
            );
            final GroupMetadataUpdateEvent groupMetadataUpdateEvent = new GroupMetadataUpdateEvent(
                generation,
                memberId
            );
            backgroundEventQueue.add(groupMetadataUpdateEvent);
            consumer.assign(singletonList(new TopicPartition("topic", 0)));
            consumer.poll(Duration.ZERO);

            final ConsumerGroupMetadata actualGroupMetadata = consumer.groupMetadata();

            assertEquals(expectedGroupMetadata, actualGroupMetadata);

            final ConsumerGroupMetadata secondActualGroupMetadataWithoutUpdate = consumer.groupMetadata();

            assertEquals(expectedGroupMetadata, secondActualGroupMetadataWithoutUpdate);
        }
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
                                            int expectedLostCount) {
        CounterConsumerRebalanceListener consumerRebalanceListener = new CounterConsumerRebalanceListener(
                revokedError,
                assignedError,
                lostError
        );
        consumer.subscribe(Collections.singletonList("topic"), consumerRebalanceListener);
        SortedSet<TopicPartition> partitions = Collections.emptySortedSet();

        for (ConsumerRebalanceListenerMethodName methodName : methodNames) {
            CompletableBackgroundEvent<Void> e = new ConsumerRebalanceListenerCallbackNeededEvent(methodName, partitions);
            backgroundEventQueue.add(e);

            // This will trigger the background event queue to process our background event message.
            consumer.poll(Duration.ZERO);
        }

        assertEquals(expectedRevokedCount, consumerRebalanceListener.revokedCount());
        assertEquals(expectedAssignedCount, consumerRebalanceListener.assignedCount());
        assertEquals(expectedLostCount, consumerRebalanceListener.lostCount());
    }

    private static Stream<Arguments> listenerCallbacksInvokeSource() {
        Optional<RuntimeException> empty = Optional.empty();
        Optional<RuntimeException> error = Optional.of(new RuntimeException("Intentional error"));

        return Stream.of(
            // Tests if we don't have an event, the listener doesn't get called.
            Arguments.of(Collections.emptyList(), empty, empty, empty, 0, 0, 0),

            // Tests if we get an event for a revocation, that we invoke our listener.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_REVOKED), empty, empty, empty, 1, 0, 0),

            // Tests if we get an event for an assignment, that we invoke our listener.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_ASSIGNED), empty, empty, empty, 0, 1, 0),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_LOST), empty, empty, empty, 0, 0, 1),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_REVOKED), error, empty, empty, 1, 0, 0),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_ASSIGNED), empty, error, empty, 0, 1, 0),

            // Tests that we invoke our listener even if it encounters an exception.
            Arguments.of(Collections.singletonList(ON_PARTITIONS_LOST), empty, empty, error, 0, 0, 1),

            // Tests if we get separate events for revocation and then assignment--AND our revocation throws an error--
            // we still invoke the listeners correctly without throwing the error at the user.
            Arguments.of(Arrays.asList(ON_PARTITIONS_REVOKED, ON_PARTITIONS_ASSIGNED), error, empty, empty, 1, 1, 0)
        );
    }

    @Test
    public void testBackgroundError() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer(), backgroundEventQueue)) {
            final KafkaException expectedException = new KafkaException("Nobody expects the Spanish Inquisition");
            final ErrorBackgroundEvent errorBackgroundEvent = new ErrorBackgroundEvent(expectedException);
            backgroundEventQueue.add(errorBackgroundEvent);
            consumer.assign(singletonList(new TopicPartition("topic", 0)));

            final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

            assertEquals(expectedException.getMessage(), exception.getMessage());
        }
    }

    @Test
    public void testMultipleBackgroundErrors() {
        final String groupId = "consumerGroupA";
        final ConsumerConfig config = new ConsumerConfig(requiredConsumerPropertiesAndGroupId(groupId));
        final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        try (final AsyncKafkaConsumer<String, String> consumer =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer(), backgroundEventQueue)) {
            final KafkaException expectedException1 = new KafkaException("Nobody expects the Spanish Inquisition");
            final ErrorBackgroundEvent errorBackgroundEvent1 = new ErrorBackgroundEvent(expectedException1);
            backgroundEventQueue.add(errorBackgroundEvent1);
            final KafkaException expectedException2 = new KafkaException("Spam, Spam, Spam");
            final ErrorBackgroundEvent errorBackgroundEvent2 = new ErrorBackgroundEvent(expectedException2);
            backgroundEventQueue.add(errorBackgroundEvent2);
            consumer.assign(singletonList(new TopicPartition("topic", 0)));

            final KafkaException exception = assertThrows(KafkaException.class, () -> consumer.poll(Duration.ZERO));

            assertEquals(expectedException1.getMessage(), exception.getMessage());
            assertTrue(backgroundEventQueue.isEmpty());
        }
    }

    @Test
    public void testGroupRemoteAssignorUnusedIfGroupIdUndefined() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);

        try (AsyncKafkaConsumer<String, String> ignored =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            assertTrue(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
        }
    }

    @Test
    public void testGroupRemoteAssignorUnusedInGenericProtocol() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.GENERIC.name().toLowerCase(Locale.ROOT));
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);

        try (AsyncKafkaConsumer<String, String> ignored =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            assertTrue(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
        }
    }

    @Test
    public void testGroupRemoteAssignorUsedInConsumerProtocol() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroupA");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT));
        props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, "someAssignor");
        final ConsumerConfig config = new ConsumerConfig(props);

        try (AsyncKafkaConsumer<String, String> ignored =
            new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            assertFalse(config.unused().contains(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
        }
    }
    
    @Test
    public void testGroupIdNull() {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);

        try (final AsyncKafkaConsumer<String, String> consumer =
                 new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            assertFalse(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            assertFalse(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
        } catch (final Exception exception) {
            throw new AssertionFailedError("The following exception was not expected:", exception);
        }
    }

    @Disabled("Flaky test temporarily disabled - in review")
    @Test
    public void testGroupIdNotNullAndValid() {
        final Properties props = requiredConsumerPropertiesAndGroupId("consumerGroupA");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED, true);
        final ConsumerConfig config = new ConsumerConfig(props);

        try (final AsyncKafkaConsumer<String, String> consumer =
                 new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            assertTrue(config.unused().contains(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG));
            assertTrue(config.unused().contains(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
        } catch (final Exception exception) {
            throw new AssertionFailedError("The following exception was not expected:", exception);
        }
    }

    @Test
    public void testGroupIdEmpty() {
        testInvalidGroupId("");
    }

    @Test
    public void testGroupIdOnlyWhitespaces() {
        testInvalidGroupId("       ");
    }

    private void testInvalidGroupId(final String groupId) {
        final Properties props = requiredConsumerPropertiesAndGroupId(groupId);
        final ConsumerConfig config = new ConsumerConfig(props);

        final Exception exception = assertThrows(
            KafkaException.class,
            () -> new AsyncKafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())
        );

        assertEquals("Failed to construct kafka consumer", exception.getMessage());
    }

    private Properties requiredConsumerPropertiesAndGroupId(final String groupId) {
        final Properties props = requiredConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    private Properties requiredConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        return props;
    }

    private void testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(boolean committedOffsetsEnabled) {
        // Uncompleted future that will time out if used
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();

        consumer.assign(singleton(new TopicPartition("t1", 1)));

        try (MockedConstruction<FetchCommittedOffsetsApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            // Poll with 250ms timeout to give the background thread time to process the events without timing out
            consumer.poll(Duration.ofMillis(250));

            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(ValidatePositionsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));

            if (committedOffsetsEnabled) {
                // Verify there was an FetchCommittedOffsets event and no ResetPositions event
                verify(applicationEventHandler, atLeast(1))
                    .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
                verify(applicationEventHandler, never())
                    .addAndGet(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
            } else {
                // Verify there was not any FetchCommittedOffsets event but there should be a ResetPositions
                verify(applicationEventHandler, never())
                    .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
                verify(applicationEventHandler, atLeast(1))
                    .addAndGet(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
            }
        }
    }

    private void testRefreshCommittedOffsetsSuccess(Set<TopicPartition> partitions,
                                                    Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.complete(committedOffsets);
        consumer.assign(partitions);
        try (MockedConstruction<FetchCommittedOffsetsApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            // Poll with 250ms timeout to give the background thread time to process the events without timing out
            consumer.poll(Duration.ofMillis(250));

            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(ValidatePositionsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(FetchCommittedOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
            verify(applicationEventHandler, atLeast(1))
                .addAndGet(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
        }
    }

    @Test
    public void testLongPollWaitIsLimited() {
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
            subscriptions.assignFromSubscribed(Collections.singleton(tp));
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
        Time time = new MockTime();
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
        Time time = new MockTime();
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
        Time time = new MockTime();
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

    private void assertNoPendingWakeup(final WakeupTrigger wakeupTrigger) {
        assertNull(wakeupTrigger.getPendingTask());
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private HashMap<TopicPartition, OffsetAndTimestamp> mockOffsetAndTimestamp() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = new HashMap<>();
        offsetAndTimestamp.put(t0, new OffsetAndTimestamp(5L, 1L));
        offsetAndTimestamp.put(t1, new OffsetAndTimestamp(6L, 3L));
        return offsetAndTimestamp;
    }

    private HashMap<TopicPartition, Long> mockTimestampToSearch() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t0, 1L);
        timestampToSearch.put(t1, 2L);
        return timestampToSearch;
    }

    private void prepAutocommitOnClose() {
        Node node = testBuilder.metadata.fetch().nodes().get(0);
        testBuilder.client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "group-id", node));
        if (!testBuilder.subscriptions.allConsumed().isEmpty()) {
            List<TopicPartition> topicPartitions = new ArrayList<>(testBuilder.subscriptions.assignedPartitionsList());
            testBuilder.client.prepareResponse(mockAutocommitResponse(
                topicPartitions,
                (short) 1,
                Errors.NONE).responseBody());
        }
    }

    private ClientResponse mockAutocommitResponse(final List<TopicPartition> topicPartitions,
                                                  final short apiKeyVersion,
                                                  final Errors error) {
        OffsetCommitResponseData responseData = new OffsetCommitResponseData();
        List<OffsetCommitResponseData.OffsetCommitResponseTopic> responseTopics = new ArrayList<>();
        topicPartitions.forEach(tp -> {
            responseTopics.add(new OffsetCommitResponseData.OffsetCommitResponseTopic()
                .setName(tp.topic())
                .setPartitions(Collections.singletonList(
                    new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setErrorCode(error.code())
                        .setPartitionIndex(tp.partition()))));
        });
        responseData.setTopics(responseTopics);
        OffsetCommitResponse response = mock(OffsetCommitResponse.class);
        when(response.data()).thenReturn(responseData);
        return new ClientResponse(
            new RequestHeader(ApiKeys.OFFSET_COMMIT, apiKeyVersion, "", 1),
            null,
            "-1",
            testBuilder.time.milliseconds(),
            testBuilder.time.milliseconds(),
            false,
            null,
            null,
            new OffsetCommitResponse(responseData)
        );
    }
}

