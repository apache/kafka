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
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataUpdateApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.metrics.Metrics;
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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {

    private static final long RETRY_BACKOFF_MS = 100;

    private final static Time TIME = new MockTime(0);
    private final LogContext logContext = new LogContext();
    private ConsumerConfig config;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClientDelegate;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private BlockingQueue<ApplicationEvent> applicationEventQueue;
    private ApplicationEventProcessor<String, String> applicationEventProcessor;
    private CoordinatorRequestManager coordinatorRequestManager;
    private CommitRequestManager commitRequestManager;
    private FetchRequestManager<String, String> fetchRequestManager;
    private final int requestTimeoutMs = 500;
    private RequestManagers<String, String> requestManagers;

    @BeforeEach
    public void setup() {
        this.metadata = mock(ConsumerMetadata.class);
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                RETRY_BACKOFF_MS,
                true);

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);

        this.config = new ConsumerConfig(properties);
        Metrics metrics = createMetrics(config, TIME);

        SubscriptionState subscriptions = createSubscriptionState(config, logContext);

        this.coordinatorRequestManager = spy(new CoordinatorRequestManager(TIME,
                logContext,
                RETRY_BACKOFF_MS,
                backgroundEventQueue,
                "group_id"));

        GroupState groupState = new GroupState(groupRebalanceConfig);
        this.commitRequestManager = spy(new CommitRequestManager(TIME,
                logContext,
                subscriptions,
                config,
                coordinatorRequestManager,
                groupState));
        FetchConfig<String, String> fetchConfig = createFetchConfig(config);
        FetchMetricsManager metricsManager = createFetchMetricsManager(metrics);

        NodeStatusDeterminator nodeStatusDeterminator = new NodeStatusDeterminator() {

                @Override
                public boolean isUnavailable(Node node) {
                    return false;
                }

                @Override
                public void maybeThrowAuthFailure(Node node) {
                    // Do nothing
                }

        };

        this.fetchRequestManager = new FetchRequestManager<>(logContext,
                TIME,
                backgroundEventQueue,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                nodeStatusDeterminator,
                RETRY_BACKOFF_MS);
        this.requestManagers = new RequestManagers<>(coordinatorRequestManager,
                commitRequestManager,
                fetchRequestManager);
        MockClient client = new MockClient(TIME, Collections.singletonList(new Node(0, "localhost", 99)));
        this.networkClientDelegate = spy(new NetworkClientDelegate(logContext,
                TIME,
                config,
                client));
        this.applicationEventProcessor = spy(new ApplicationEventProcessor<>(logContext,
                backgroundEventQueue,
                requestManagers,
                metadata));
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        backgroundThread.start();

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
        CoordinatorRequestManager coordinatorRequestManager = requestManagers.coordinatorRequestManager.get();
        CommitRequestManager commitRequestManager = requestManagers.commitRequestManager.get();
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        applicationEventQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    public void testMetadataUpdateEvent() {
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new MetadataUpdateApplicationEvent(TIME.milliseconds());
        this.applicationEventQueue.add(e);
        backgroundThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
        backgroundThread.close();
    }

    @Test
    public void testCommitEvent() {
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new CommitApplicationEvent(new HashMap<>());
        this.applicationEventQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(CommitApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        CoordinatorRequestManager coordinatorRequestManager = requestManagers.coordinatorRequestManager.get();
        CommitRequestManager commitRequestManager = requestManagers.commitRequestManager.get();
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorRequestManager, times(1)).poll(anyLong());
        Mockito.verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        try (DefaultBackgroundThread<String, String> backgroundThread = mockBackgroundThread()) {
            // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
            NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                    10,
                    Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
            assertEquals(10, backgroundThread.handlePollResult(success));

            NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                    10,
                    new ArrayList<>());
            assertEquals(10, backgroundThread.handlePollResult(failure));
        }
    }

    private static NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest(final long timeout) {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
            Optional.empty());
        req.setTimer(TIME, timeout);
        return req;
    }

    private DefaultBackgroundThread<String, String> mockBackgroundThread() {
        return new DefaultBackgroundThread<>(logContext,
                TIME,
                config,
                applicationEventQueue,
                backgroundEventQueue,
                metadata,
                networkClientDelegate,
                applicationEventProcessor,
                requestManagers);
    }

    private NetworkClientDelegate.PollResult mockPollCoordinatorResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
    }

    private NetworkClientDelegate.PollResult mockPollCommitResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
    }

}
