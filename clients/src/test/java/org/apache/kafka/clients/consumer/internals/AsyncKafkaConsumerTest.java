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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.FetchCommittedOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.stubbing.Answer;

public class AsyncKafkaConsumerTest {

    private AsyncKafkaConsumer<?, ?> consumer;
    private FetchCollector<?, ?> fetchCollector;
    private ConsumerTestBuilder.AsyncKafkaConsumerTestBuilder testBuilder;
    private ApplicationEventHandler applicationEventHandler;
    private SubscriptionState subscriptions;

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
    public void testFailOnClosedConsumer() {
        consumer.close();
        final IllegalStateException res = assertThrows(IllegalStateException.class, consumer::assignment);
        assertEquals("This consumer has already been closed.", res.getMessage());
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

