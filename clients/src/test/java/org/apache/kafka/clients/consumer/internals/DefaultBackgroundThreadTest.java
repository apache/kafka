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
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class DefaultBackgroundThreadTest {

    private ConsumerTestBuilder.DefaultBackgroundThreadTestBuilder testBuilder;
    private Time time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor applicationEventProcessor;
    private CoordinatorRequestManager coordinatorManager;
    private OffsetsRequestManager offsetsRequestManager;
    private CommitRequestManager commitManager;
    private DefaultBackgroundThread backgroundThread;
    private MockClient client;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.DefaultBackgroundThreadTestBuilder();
        time = testBuilder.time;
        metadata = testBuilder.metadata;
        networkClient = testBuilder.networkClientDelegate;
        client = testBuilder.client;
        applicationEventsQueue = testBuilder.applicationEventQueue;
        applicationEventProcessor = testBuilder.applicationEventProcessor;
        coordinatorManager = testBuilder.coordinatorRequestManager;
        commitManager = testBuilder.commitRequestManager;
        offsetsRequestManager = testBuilder.offsetsRequestManager;
        backgroundThread = testBuilder.backgroundThread;
        backgroundThread.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        backgroundThread.start();
        TestUtils.waitForCondition(backgroundThread::isRunning, "Failed awaiting for the background thread to be running");

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        int maxWaitMs = 1000;
        TestUtils.waitForCondition(backgroundThread::isRunning,
                maxWaitMs,
                "Thread did not start within " + maxWaitMs + " ms");
        backgroundThread.close(Duration.ofMillis(maxWaitMs));
        TestUtils.waitForCondition(() -> !backgroundThread.isRunning(),
                maxWaitMs,
                "Thread did not stop within " + maxWaitMs + " ms");
    }

    @Test
    public void testApplicationEvent() {
        FetchEvent e = new FetchEvent();
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor, times(1)).process(e);
    }

    @Test
    public void testMetadataUpdateEvent() {
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
    }

    @Test
    public void testCommitEvent() {
        ApplicationEvent e = new CommitApplicationEvent(new HashMap<>());
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(CommitApplicationEvent.class));
    }

    @Test
    public void testListOffsetsEventIsProcessed() {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsApplicationEvent(timestamps, true);
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsEventIsProcessed() {
        ResetPositionsApplicationEvent e = new ResetPositionsApplicationEvent();
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ResetPositionsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsProcessFailureInterruptsBackgroundThread() {
        TopicAuthorizationException authException = new TopicAuthorizationException("Topic authorization failed");
        doThrow(authException).when(offsetsRequestManager).resetPositionsIfNeeded();

        ResetPositionsApplicationEvent event = new ResetPositionsApplicationEvent();
        applicationEventsQueue.add(event);
        assertThrows(TopicAuthorizationException.class, () -> backgroundThread.runOnce());

        verify(applicationEventProcessor).process(any(ResetPositionsApplicationEvent.class));
    }

    @Test
    public void testValidatePositionsEventIsProcessed() {
        ValidatePositionsApplicationEvent e = new ValidatePositionsApplicationEvent();
        applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ValidatePositionsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testValidatePositionsProcessFailure() {
        LogTruncationException logTruncationException = new LogTruncationException(Collections.emptyMap(), Collections.emptyMap());
        doThrow(logTruncationException).when(offsetsRequestManager).validatePositionsIfNeeded();

        ValidatePositionsApplicationEvent event = new ValidatePositionsApplicationEvent();
        applicationEventsQueue.add(event);
        assertThrows(LogTruncationException.class, backgroundThread::runOnce);

        verify(applicationEventProcessor).process(any(ValidatePositionsApplicationEvent.class));
    }

    @Test
    public void testAssignmentChangeEvent() {
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeApplicationEvent(offset, currentTimeMs);
        applicationEventsQueue.add(e);

        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(AssignmentChangeApplicationEvent.class));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
        verify(commitManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        verify(commitManager, times(1)).maybeAutoCommit(offset);
    }

    @Test
    void testFindCoordinator() {
        backgroundThread.runOnce();
        verify(coordinatorManager, times(1)).poll(anyLong());
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
    }

    @Test
    void testFetchTopicMetadata() {
        applicationEventsQueue.add(new TopicMetadataApplicationEvent("topic"));
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(TopicMetadataApplicationEvent.class));
    }

    @Test
    void testPollResultTimer() {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                Optional.empty());
        req.setTimer(time, ConsumerTestBuilder.REQUEST_TIMEOUT_MS);

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
    void testRequestManagersArePolledOnce() {
        backgroundThread.runOnce();
        testBuilder.requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).poll(anyLong())));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
    }

    @Test
    void testEnsureMetadataUpdateOnPoll() {
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(2, Collections.emptyMap());
        client.prepareMetadataUpdate(metadataResponse);
        metadata.requestUpdate(false);
        backgroundThread.runOnce();
        verify(metadata, times(1)).updateWithCurrentRequestVersion(eq(metadataResponse), eq(false), anyLong());
    }

    @Test
    void testEnsureEventsAreCompleted() {
        FetchEvent event = spy(new FetchEvent());
        ApplicationEvent e = new CommitApplicationEvent(Collections.emptyMap());
        CompletableFuture<Queue<CompletedFetch>> future = new CompletableFuture<>();
        when(event.future()).thenReturn(future);
        applicationEventsQueue.add(event);
        applicationEventsQueue.add(e);
        assertFalse(future.isDone());
        assertFalse(applicationEventsQueue.isEmpty());

        backgroundThread.close();
        assertTrue(future.isCompletedExceptionally());
        assertTrue(applicationEventsQueue.isEmpty());
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
