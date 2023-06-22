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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataUpdateApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataApplicationEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {

    private ConsumerTestBuilder.DefaultBackgroundThreadTestBuilder testBuilder;
    private Time time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor<String, String> applicationEventProcessor;
    private CoordinatorRequestManager coordinatorManager;
    private CommitRequestManager commitManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private DefaultBackgroundThread<String, String> backgroundThread;

    @BeforeEach
    public void setup() {
        this.testBuilder = new ConsumerTestBuilder.DefaultBackgroundThreadTestBuilder();
        this.time = testBuilder.time;
        this.metadata = testBuilder.metadata;
        this.networkClient = testBuilder.networkClientDelegate;
        this.applicationEventsQueue = testBuilder.applicationEventQueue;
        this.applicationEventProcessor = testBuilder.applicationEventProcessor;
        this.coordinatorManager = testBuilder.coordinatorRequestManager;
        this.commitManager = testBuilder.commitRequestManager;
        this.topicMetadataRequestManager = testBuilder.topicMetadataRequestManager;
        this.backgroundThread = testBuilder.backgroundThread;
        this.backgroundThread.initializeResources();
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        backgroundThread.start();
        assertFalse(backgroundThread.isRunning());

        // There's a nonzero amount of time between starting the thread and having it
        // begin to execute our code. Wait for a bit before checking...
        int maxWaitMs = 1000;
        TestUtils.waitForCondition(backgroundThread::isRunning,
                maxWaitMs,
                "Thread did not start within " + maxWaitMs + " ms");

        assertTrue(backgroundThread.isRunning());
        backgroundThread.close();
        assertFalse(backgroundThread.isRunning());
    }

    @Test
    public void testApplicationEvent() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    public void testMetadataUpdateEvent() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        ApplicationEvent e = new MetadataUpdateApplicationEvent(time.milliseconds());
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
        backgroundThread.close();
    }

    @Test
    public void testCommitEvent() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        ApplicationEvent e = new CommitApplicationEvent(new HashMap<>());
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(CommitApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    public void testListOffsetsEventIsProcessed() {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsApplicationEvent(timestamps, true);
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorManager, times(1)).poll(anyLong());
        Mockito.verify(networkClient, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testFetchTopicMetadata() {
        when(this.topicMetadataRequestManager.requestTopicMetadata(Optional.of(anyString()))).thenReturn(new CompletableFuture<>());
        this.applicationEventsQueue.add(new TopicMetadataApplicationEvent("topic"));
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(TopicMetadataApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
        NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                10,
                Collections.singletonList(findCoordinatorUnsentRequest()));
        assertEquals(10, backgroundThread.handlePollResult(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, backgroundThread.handlePollResult(failure));
    }

    private NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest() {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                Optional.empty());
        req.setTimer(time, ConsumerTestBuilder.REQUEST_TIMEOUT_MS);
        return req;
    }

    private NetworkClientDelegate.PollResult mockPollCoordinatorResult() {
        return new NetworkClientDelegate.PollResult(
                ConsumerTestBuilder.RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest()));
    }

    private NetworkClientDelegate.PollResult mockPollCommitResult() {
        return new NetworkClientDelegate.PollResult(
                ConsumerTestBuilder.RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest()));
    }
}
