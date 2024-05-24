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

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

public class ConsumerNetworkThreadUnitTest {

    private final Time time;
    private final ConsumerMetadata metadata;
    private final ConsumerConfig config;
    private final SubscriptionState subscriptions;
    private final BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final CommitRequestManager commitRequestManager;
    private final HeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerNetworkThread consumerNetworkThread;
    private final MockClient client;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;

    static final int DEFAULT_REQUEST_TIMEOUT_MS = 500;
    static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;

    ConsumerNetworkThreadUnitTest() {
        LogContext logContext = new LogContext();
        this.time = new MockTime();
        this.client = new MockClient(time);

        // usually we don't mock 1. time - MockTime 2. logContext and 3. networkClient - MockClient
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.requestManagers = mock(RequestManagers.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.commitRequestManager = mock(CommitRequestManager.class);
        this.heartbeatRequestManager = mock(HeartbeatRequestManager.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.applicationEventsQueue = mock(LinkedBlockingQueue.class);
        this.config = mock(ConsumerConfig.class);
        this.subscriptions = mock(SubscriptionState.class);
        this.metadata = mock(ConsumerMetadata.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);

        this.consumerNetworkThread = new ConsumerNetworkThread(
                logContext,
                time,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers
        );
    }

    @BeforeEach
    public void setup() {
        consumerNetworkThread.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (consumerNetworkThread != null)
            consumerNetworkThread.close();
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerNetworkThread.start();
        TestCondition isStarted = () -> consumerNetworkThread.isRunning();
        TestCondition isClosed = () -> !(consumerNetworkThread.isRunning() || consumerNetworkThread.isAlive());

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        TestUtils.waitForCondition(isStarted,
                "The consumer network thread did not start within " + DEFAULT_MAX_WAIT_MS + " ms");

        consumerNetworkThread.close(Duration.ofMillis(DEFAULT_MAX_WAIT_MS));

        TestUtils.waitForCondition(isClosed,
                "The consumer network thread did not stop within " + DEFAULT_MAX_WAIT_MS + " ms");
    }

    // can you rename this test? testEnsureApplicationEventProcessorProcesses....
    @Test
    public void testApplicationEvent() {
        //ApplicationEvent e = new PollEvent(100);
        //applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor, times(1)).process();
    }

    // TODO: Remove test, place elsewhere
    // this test is testing ApplicationEventProcessor and ConsumerMetaData
    @Test
    public void testMetadataUpdateEvent() {
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
    }

    // TODO: Remove test, place elsewhere
    // This test is testing behavior of the application event processor
    @Test
    public void testAsyncCommitEvent() {
        ApplicationEvent e = new AsyncCommitEvent(new HashMap<>());
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        //verify(applicationEventProcessor).process(any(AsyncCommitEvent.class));
        verify(applicationEventProcessor).process();
    }

    // TODO: Remove test, place elsewhere
    // This test is testing behavior of the application event processor
    @Test
    public void testSyncCommitEvent() {
        Timer timer = time.timer(100);
        ApplicationEvent e = new SyncCommitEvent(new HashMap<>(), timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        //verify(applicationEventProcessor).process(any(SyncCommitEvent.class));
        verify(applicationEventProcessor).process();
    }

    // TODO: Remove/move this.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        Timer timer = time.timer(100);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, timer, requireTimestamp);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        //verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
        verify(applicationEventProcessor).process();
        //assertTrue(applicationEventsQueue.isEmpty());
    }

    // TODO: Remove test, place elsewhere
    // Redundant
    @Test
    public void testResetPositionsEventIsProcessed() {
        Timer timer = time.timer(100);
        ResetPositionsEvent e = new ResetPositionsEvent(timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        // in the runOnce it is doing processor.process() so we should not be expecting any object being passed into this func.  So clean up the rest of the test.
        verify(applicationEventProcessor).process();
        //assertTrue(applicationEventsQueue.isEmpty());
        //when(coordinatorRequestManager.poll(time.milliseconds())).thenReturn(new NetworkClientDelegate.PollResult(100));
        //when(networkClientDelegate.addAll(new NetworkClientDelegate.PollResult(100))).thenReturn(100L);
    }

    // TODO: This needs to be tested but should be removed from here.
    @Test
    public void testResetPositionsProcessFailureIsIgnored() {
        doThrow(new NullPointerException()).when(offsetsRequestManager).resetPositionsIfNeeded();

        Timer timer = time.timer(100);
        ResetPositionsEvent event = new ResetPositionsEvent(timer);
        applicationEventsQueue.add(event);
        assertDoesNotThrow(() -> consumerNetworkThread.runOnce());

        //verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
        verify(applicationEventProcessor).process();
    }

    // TODO: Remove test, place elsewhere
    // Redundant
    @Test
    public void testValidatePositionsEventIsProcessed() {
        Timer timer = time.timer(100);
        ValidatePositionsEvent e = new ValidatePositionsEvent(timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        //verify(applicationEventProcessor).process(any(ValidatePositionsEvent.class));
        verify(applicationEventProcessor).process();
        //assertTrue(applicationEventsQueue.isEmpty());
    }

    // TODO: Remove test, place elsewhere
    // Move this test to applicationEventProcessor if not already there
    // Seems more like integration testing, also redundant since we just need consumerNetworkThread
    // only needs invoke runOnce() to then invoke applicationEventProcessor.process()
    @Test
    public void testAssignmentChangeEvent() {
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeEvent(offset, currentTimeMs);
        applicationEventsQueue.add(e);

        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process();
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        verify(commitRequestManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        // Assignment change should generate an async commit (not retried).
        verify(commitRequestManager, times(1)).maybeAutoCommitAsync();
    }

    // TODO: Remove/move this.
    @Test
    void testFetchTopicMetadata() {
        Timer timer = time.timer(Long.MAX_VALUE);
        applicationEventsQueue.add(new TopicMetadataEvent("topic", timer));
        consumerNetworkThread.runOnce();
        //verify(applicationEventProcessor).process(any(TopicMetadataEvent.class));
        verify(applicationEventProcessor).process();
    }

    // TODO: Remove test, place elsewhere
    // seems like this belongs to the ncd test. check if there's a similar test there
    // I think this should be moved to NCD test, we are not testing ConsumerNetworkThread
    @Test
    void testPollResultTimer() {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                Optional.empty());
        req.setTimer(time, DEFAULT_REQUEST_TIMEOUT_MS);

        // purposely setting a non-MAX time to ensure it is returning Long.MAX_VALUE upon success
        NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                10,
                Collections.singletonList(req));
        assertEquals(10, networkClientDelegate.addAll(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, networkClientDelegate.addAll(failure));
    }

    // TODO: Remove test, place elsewhere
    // I think this test should be moved to HeartBeatRequestManager, explained below
    @Test
    void testMaximumTimeToWait() {
        // Initial value before runOnce has been called
        // Target: 5000, correct bc maximumTimeToWait is 5000 by default
        // This first part is redundant, it will always be equal
        assertEquals(MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());
        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        // Looking at the implementation, it seems that this is testing HeartBeatRequestManager behavior
        //
        //     In ConsumerNetworkThread.runOnce(), cachedTimeToWait is being calculated, as implied above
        // after running runOnce(), the consumerNetworkThread.maximumTimeToWait() should return default heartbeat interval.
        // How this can be done is when rm.maximumTimeToWait() is called, the HBRM pollTimer is not expired, and
        // pollTimer.remainingMs() returns 1000 exactly
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, consumerNetworkThread.maximumTimeToWait());
    }

    @Test
    void testRequestManagersArePolledOnce() {
        consumerNetworkThread.runOnce();
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).poll(anyLong())));
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).maximumTimeToWait(anyLong())));
        // We just need to test networkClientDelegate not networkClient
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
    }

    /**
     * // Make sure the tests include testing poll with params of (pollWaitTime, currentTimeMs)
     * 1. Add a test that have RM to return different poll times and ensure pollWaitTimeMs is computed correctly. i.e. takes the min of all
     * 2. Test maxTimeToWait with different request manager returns
     * 3. Remove the tests and create a commit for it so taht we can look back later.
     */

    // TODO: Remove test, place elsewhere
    // This test can probably go because it is testing metadata update in the NetworkClient module
    // May move somewhere else if it is not being tested elsewhere
    @Test
    void testEnsureMetadataUpdateOnPoll() {
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        client.prepareMetadataUpdate(metadataResponse);
        metadata.requestUpdate(false);
        consumerNetworkThread.runOnce();
        verify(metadata, times(1)).updateWithCurrentRequestVersion(eq(metadataResponse), eq(false), anyLong());
    }

    // TODO: Remove test, place elsewhere
    // Test should be moved, testing pretty much everything but the ConsumerNetworkThread
    @Test
    void testEnsureEventsAreCompleted() {
        List<Node> list = new ArrayList<>();
        list.add(new Node(1, null, 1));
        when(metadata.fetch().nodes()).thenReturn(list);
        Node node = metadata.fetch().nodes().get(0);
        coordinatorRequestManager.markCoordinatorUnknown("test", time.milliseconds());
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, "group-id", node));
        prepareOffsetCommitRequest(new HashMap<>(), Errors.NONE, false);
        CompletableApplicationEvent<Void> event1 = spy(new AsyncCommitEvent(Collections.emptyMap()));

        ApplicationEvent event2 = new AsyncCommitEvent(Collections.emptyMap());
        CompletableFuture<Void> future = new CompletableFuture<>();
        when(event1.future()).thenReturn(future);
        applicationEventsQueue.add(event1);
        applicationEventsQueue.add(event2);
        assertFalse(future.isDone());
        assertFalse(applicationEventsQueue.isEmpty());

        consumerNetworkThread.cleanup();
        assertTrue(future.isCompletedExceptionally());
        assertTrue(applicationEventsQueue.isEmpty());
    }

    private void prepareOffsetCommitRequest(final Map<TopicPartition, Long> expectedOffsets,
                                            final Errors error,
                                            final boolean disconnected) {
        Map<TopicPartition, Errors> errors = partitionErrors(expectedOffsets.keySet(), error);
        client.prepareResponse(offsetCommitRequestMatcher(expectedOffsets), offsetCommitResponse(errors), disconnected);
    }

    private Map<TopicPartition, Errors> partitionErrors(final Collection<TopicPartition> partitions,
                                                        final Errors error) {
        final Map<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, error);
        }
        return errors;
    }

    private OffsetCommitResponse offsetCommitResponse(final Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    private MockClient.RequestMatcher offsetCommitRequestMatcher(final Map<TopicPartition, Long> expectedOffsets) {
        return body -> {
            OffsetCommitRequest req = (OffsetCommitRequest) body;
            Map<TopicPartition, Long> offsets = req.offsets();
            if (offsets.size() != expectedOffsets.size())
                return false;

            for (Map.Entry<TopicPartition, Long> expectedOffset : expectedOffsets.entrySet()) {
                if (!offsets.containsKey(expectedOffset.getKey())) {
                    return false;
                } else {
                    Long actualOffset = offsets.get(expectedOffset.getKey());
                    if (!actualOffset.equals(expectedOffset.getValue())) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }
}
