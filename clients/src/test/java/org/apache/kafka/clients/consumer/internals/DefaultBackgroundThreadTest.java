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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollContext;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {
    private static final long REFRESH_BACK_OFF_MS = 100;
    private final Properties properties = new Properties();
    private MockTime time;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptions;
    private Metrics metrics;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<BackgroundEvent> backgroundEventsQueue;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor processor;
    private CoordinatorRequestManager coordinatorManager;
    private FetchRequestManager<?, ?> fetchRequestManager;
    private ErrorEventHandler errorEventHandler;
    private final int requestTimeoutMs = 500;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.time = new MockTime(0);
        this.metadata = mock(ConsumerMetadata.class);
        this.subscriptions = mock(SubscriptionState.class);
        this.metrics = mock(Metrics.class);
        this.networkClient = mock(NetworkClientDelegate.class);
        this.applicationEventsQueue = (BlockingQueue<ApplicationEvent>) mock(BlockingQueue.class);
        this.backgroundEventsQueue = (BlockingQueue<BackgroundEvent>) mock(BlockingQueue.class);
        this.processor = mock(ApplicationEventProcessor.class);
        this.coordinatorManager = mock(CoordinatorRequestManager.class);
        this.fetchRequestManager = mock(FetchRequestManager.class);
        this.errorEventHandler = mock(ErrorEventHandler.class);
    }

    @Test
    public void testStartupAndTearDown() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        backgroundThread.start();
        assertTrue(backgroundThread.isRunning());
        backgroundThread.close();
    }

    @Test
    public void testApplicationEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        when(coordinatorManager.poll(any(PollContext.class))).thenReturn(mockPollResult());
        when(fetchRequestManager.poll(any(PollContext.class))).thenReturn(mockPollResult());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(processor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        when(coordinatorManager.poll(any(PollContext.class))).thenReturn(mockPollResult());
        when(fetchRequestManager.poll(any(PollContext.class))).thenReturn(mockPollResult());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorManager, times(1)).poll(any(PollContext.class));
        Mockito.verify(networkClient, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
        PollResult success = new PollResult(
                10,
                Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
        assertEquals(10, backgroundThread.handlePollResult(success));

        PollResult failure = new PollResult(10, new ArrayList<>());
        assertEquals(10, backgroundThread.handlePollResult(failure));
    }

    private static UnsentRequest findCoordinatorUnsentRequest(final Time time, final long timeout) {
        UnsentRequest req = new UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")));
        req.setTimer(time, timeout);
        return req;
    }

    private DefaultBackgroundThread mockBackgroundThread() {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, REFRESH_BACK_OFF_MS);

        return new DefaultBackgroundThread(
                this.time,
                new ConsumerConfig(properties),
                new LogContext(),
                applicationEventsQueue,
                backgroundEventsQueue,
                this.errorEventHandler,
                processor,
                this.metadata,
                this.subscriptions,
                this.metrics,
                this.networkClient,
                this.coordinatorManager,
                this.fetchRequestManager);
    }

    private PollResult mockPollResult() {
        return new PollResult(0, findCoordinatorUnsentRequest(time, requestTimeoutMs));
    }
}
