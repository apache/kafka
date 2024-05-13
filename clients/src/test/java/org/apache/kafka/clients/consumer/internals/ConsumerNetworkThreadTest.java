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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {

    private ConsumerTestBuilder.ConsumerNetworkThreadTestBuilder testBuilder;
    private Time time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor applicationEventProcessor;
    private OffsetsRequestManager offsetsRequestManager;
    private CommitRequestManager commitRequestManager;
    private CoordinatorRequestManager coordinatorRequestManager;
    private ConsumerNetworkThread consumerNetworkThread;
    private MockClient client;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.ConsumerNetworkThreadTestBuilder();
        time = testBuilder.time;
        metadata = testBuilder.metadata;
        networkClient = testBuilder.networkClientDelegate;
        client = testBuilder.client;
        applicationEventsQueue = testBuilder.applicationEventQueue;
        applicationEventProcessor = testBuilder.applicationEventProcessor;
        commitRequestManager = testBuilder.commitRequestManager.orElseThrow(IllegalStateException::new);
        offsetsRequestManager = testBuilder.offsetsRequestManager;
        coordinatorRequestManager = testBuilder.coordinatorRequestManager.orElseThrow(IllegalStateException::new);
        consumerNetworkThread = testBuilder.consumerNetworkThread;
        consumerNetworkThread.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        // The consumer is closed in ConsumerTestBuilder.ConsumerNetworkThreadTestBuilder.close()
        // which is called from tearDown().
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

    @Test
    public void testApplicationEvent() {
        ApplicationEvent e = new PollEvent(100);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor, times(1)).process(e);
    }

    @Test
    public void testMetadataUpdateEvent() {
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
    }

    @Test
    public void testAsyncCommitEvent() {
        ApplicationEvent e = new AsyncCommitEvent(new HashMap<>());
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(AsyncCommitEvent.class));
    }

    @Test
    public void testSyncCommitEvent() {
        Timer timer = time.timer(100);
        ApplicationEvent e = new SyncCommitEvent(new HashMap<>(), timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(SyncCommitEvent.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        Timer timer = time.timer(100);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, timer, requireTimestamp);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsEventIsProcessed() {
        Timer timer = time.timer(100);
        ResetPositionsEvent e = new ResetPositionsEvent(timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsProcessFailureIsIgnored() {
        doThrow(new NullPointerException()).when(offsetsRequestManager).resetPositionsIfNeeded();

        Timer timer = time.timer(100);
        ResetPositionsEvent event = new ResetPositionsEvent(timer);
        applicationEventsQueue.add(event);
        assertDoesNotThrow(() -> consumerNetworkThread.runOnce());

        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
    }

    @Test
    public void testValidatePositionsEventIsProcessed() {
        Timer timer = time.timer(100);
        ValidatePositionsEvent e = new ValidatePositionsEvent(timer);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ValidatePositionsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testAssignmentChangeEvent() {
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeEvent(offset, currentTimeMs);
        applicationEventsQueue.add(e);

        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(AssignmentChangeEvent.class));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
        verify(commitRequestManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        // Assignment change should generate an async commit (not retried).
        verify(commitRequestManager, times(1)).maybeAutoCommitAsync();
    }

    @Test
    void testFetchTopicMetadata() {
        Timer timer = time.timer(Long.MAX_VALUE);
        applicationEventsQueue.add(new TopicMetadataEvent("topic", timer));
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(TopicMetadataEvent.class));
    }

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
        assertEquals(10, networkClient.addAll(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, networkClient.addAll(failure));
    }

    @Test
    void testMaximumTimeToWait() {
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());
        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, consumerNetworkThread.maximumTimeToWait());
    }

    @Test
    void testRequestManagersArePolledOnce() {
        consumerNetworkThread.runOnce();
        testBuilder.requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).poll(anyLong())));
        testBuilder.requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).maximumTimeToWait(anyLong())));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
    }

    @Test
    void testEnsureMetadataUpdateOnPoll() {
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        client.prepareMetadataUpdate(metadataResponse);
        metadata.requestUpdate(false);
        consumerNetworkThread.runOnce();
        verify(metadata, times(1)).updateWithCurrentRequestVersion(eq(metadataResponse), eq(false), anyLong());
    }

    @Test
    void testEnsureEventsAreCompleted() {
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
