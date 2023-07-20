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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {
    private static final long RETRY_BACKOFF_MS = 100;
    private final Properties properties = new Properties();
    private MockTime time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<BackgroundEvent> backgroundEventsQueue;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor processor;
    private CoordinatorRequestManager coordinatorManager;
    private ErrorEventHandler errorEventHandler;
    private SubscriptionState subscriptionState;
    private int requestTimeoutMs = 500;
    private GroupState groupState;
    private CommitRequestManager commitManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.time = new MockTime(0);
        this.metadata = mock(ConsumerMetadata.class);
        this.networkClient = mock(NetworkClientDelegate.class);
        this.applicationEventsQueue = (BlockingQueue<ApplicationEvent>) mock(BlockingQueue.class);
        this.backgroundEventsQueue = (BlockingQueue<BackgroundEvent>) mock(BlockingQueue.class);
        this.processor = mock(ApplicationEventProcessor.class);
        this.coordinatorManager = mock(CoordinatorRequestManager.class);
        this.errorEventHandler = mock(ErrorEventHandler.class);
        this.subscriptionState = mock(SubscriptionState.class);
        GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                100,
                true);
        this.groupState = new GroupState(rebalanceConfig);
        this.commitManager = mock(CommitRequestManager.class);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        backgroundThread.start();
        TestUtils.waitForCondition(backgroundThread::isRunning, "Failed awaiting for the background thread to be running");
        backgroundThread.close();
        assertFalse(backgroundThread.isRunning());
    }

    @Test
    public void testApplicationEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(processor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    public void testMetadataUpdateEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        this.processor = new ApplicationEventProcessor(this.backgroundEventsQueue, mockRequestManagerRegistry(),
            metadata);
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
        backgroundThread.close();
    }

    @Test
    public void testCommitEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new CommitApplicationEvent(new HashMap<>());
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(processor).process(any(CommitApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    public void testAssignmentChangeEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        this.processor = spy(new ApplicationEventProcessor(this.backgroundEventsQueue, mockRequestManagerRegistry(),
            metadata));

        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeApplicationEvent(offset, currentTimeMs);
        this.applicationEventsQueue.add(e);

        when(this.coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(this.commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());

        backgroundThread.runOnce();
        verify(processor).process(any(AssignmentChangeApplicationEvent.class));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
        verify(commitManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        verify(commitManager, times(1)).maybeAutoCommit(offset);

        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        when(this.coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(this.commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorManager, times(1)).poll(anyLong());
        Mockito.verify(networkClient, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
        NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                10,
                Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
        assertEquals(10, backgroundThread.handlePollResult(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, backgroundThread.handlePollResult(failure));
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private Map<RequestManager.Type, Optional<RequestManager>> mockRequestManagerRegistry() {
        Map<RequestManager.Type, Optional<RequestManager>> registry = new HashMap<>();
        registry.put(RequestManager.Type.COORDINATOR, Optional.of(coordinatorManager));
        registry.put(RequestManager.Type.COMMIT, Optional.of(commitManager));
        return registry;
    }

    private static NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest(
            final Time time,
            final long timeout
    ) {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
            Optional.empty());
        req.setTimer(time, timeout);
        return req;
    }

    private DefaultBackgroundThread mockBackgroundThread() {
        return new DefaultBackgroundThread(
                this.time,
                new ConsumerConfig(properties),
                new LogContext(),
                applicationEventsQueue,
                backgroundEventsQueue,
                subscriptionState,
                this.errorEventHandler,
                processor,
                this.metadata,
                this.networkClient,
                this.groupState,
                this.coordinatorManager,
                this.commitManager);
    }

    private NetworkClientDelegate.PollResult mockPollCoordinatorResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
    }

    private NetworkClientDelegate.PollResult mockPollCommitResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
    }
}
