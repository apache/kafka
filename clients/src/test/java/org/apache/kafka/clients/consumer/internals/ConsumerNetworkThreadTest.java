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
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {
    private final Time time;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final ConsumerHeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerNetworkThread consumerNetworkThread;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;
    private final CompletableEventReaper applicationEventReaper;
    private final AsyncConsumerMetrics asyncConsumerMetrics;

    ConsumerNetworkThreadTest() {
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.requestManagers = mock(RequestManagers.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.heartbeatRequestManager = mock(ConsumerHeartbeatRequestManager.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);
        this.applicationEventReaper = mock(CompletableEventReaper.class);
        this.time = new MockTime();
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.asyncConsumerMetrics = mock(AsyncConsumerMetrics.class);
        LogContext logContext = new LogContext();

        this.consumerNetworkThread = new ConsumerNetworkThread(
                logContext,
                time,
                applicationEventQueue,
                applicationEventReaper,
                () -> applicationEventProcessor,
                () -> networkClientDelegate,
                () -> requestManagers,
                asyncConsumerMetrics
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
    public void testEnsureCloseStopsRunningThread() {
        assertTrue(consumerNetworkThread.isRunning(),
            "ConsumerNetworkThread should start running when created");

        consumerNetworkThread.close();
        assertFalse(consumerNetworkThread.isRunning(),
            "close() should make consumerNetworkThread.running false by calling closeInternal(Duration timeout)");
    }

    @ParameterizedTest
    @ValueSource(longs = {ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS - 1, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS + 1})
    public void testConsumerNetworkThreadPollTimeComputations(long exampleTime) {
        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(Optional.of(coordinatorRequestManager));
        list.add(Optional.of(heartbeatRequestManager));

        when(requestManagers.entries()).thenReturn(list);

        NetworkClientDelegate.PollResult pollResult = new NetworkClientDelegate.PollResult(exampleTime);
        NetworkClientDelegate.PollResult pollResult1 = new NetworkClientDelegate.PollResult(exampleTime + 100);

        long t = time.milliseconds();
        when(coordinatorRequestManager.poll(t)).thenReturn(pollResult);
        when(coordinatorRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime);
        when(heartbeatRequestManager.poll(t)).thenReturn(pollResult1);
        when(heartbeatRequestManager.maximumTimeToWait(t)).thenReturn(exampleTime + 100);
        when(networkClientDelegate.addAll(pollResult)).thenReturn(pollResult.timeUntilNextPollMs);
        when(networkClientDelegate.addAll(pollResult1)).thenReturn(pollResult1.timeUntilNextPollMs);
        consumerNetworkThread.runOnce();

        verify(networkClientDelegate).poll(Math.min(exampleTime, ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS), time.milliseconds());
        assertEquals(consumerNetworkThread.maximumTimeToWait(), exampleTime);
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        consumerNetworkThread.start();
        TestCondition isStarted = consumerNetworkThread::isRunning;
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
    public void testRequestsTransferFromManagersToClientOnThreadRun() {
        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(Optional.of(coordinatorRequestManager));
        list.add(Optional.of(heartbeatRequestManager));
        list.add(Optional.of(offsetsRequestManager));

        when(requestManagers.entries()).thenReturn(list);
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mock(NetworkClientDelegate.PollResult.class));
        consumerNetworkThread.runOnce();
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm).poll(anyLong())));
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm).maximumTimeToWait(anyLong())));
        verify(networkClientDelegate).addAll(any(NetworkClientDelegate.PollResult.class));
        verify(networkClientDelegate).poll(anyLong(), anyLong());
    }

    @Test
    public void testMaximumTimeToWait() {
        final int defaultHeartbeatIntervalMs = 1000;
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());

        when(requestManagers.entries()).thenReturn(Collections.singletonList(Optional.of(heartbeatRequestManager)));
        when(heartbeatRequestManager.maximumTimeToWait(time.milliseconds())).thenReturn((long) defaultHeartbeatIntervalMs);

        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(defaultHeartbeatIntervalMs, consumerNetworkThread.maximumTimeToWait());
    }

    @Test
    public void testCleanupInvokesReaper() {
        LinkedList<NetworkClientDelegate.UnsentRequest> queue = new LinkedList<>();
        when(networkClientDelegate.unsentRequests()).thenReturn(queue);
        when(applicationEventReaper.reap(applicationEventQueue)).thenReturn(1L);
        consumerNetworkThread.cleanup();
        verify(applicationEventReaper).reap(applicationEventQueue);
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testRunOnceInvokesReaper() {
        when(applicationEventReaper.reap(any(Long.class))).thenReturn(1L);
        consumerNetworkThread.runOnce();
        verify(applicationEventReaper).reap(any(Long.class));
        verify(asyncConsumerMetrics).recordApplicationEventExpiredSize(1L);
    }

    @Test
    public void testSendUnsentRequests() {
        when(networkClientDelegate.hasAnyPendingRequests()).thenReturn(true).thenReturn(true).thenReturn(false);
        consumerNetworkThread.cleanup();
        verify(networkClientDelegate, times(2)).poll(anyLong(), anyLong());
    }

    @Test
    public void testRunOnceRecordTimeBetweenNetworkThreadPoll() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics);
             ConsumerNetworkThread consumerNetworkThread = new ConsumerNetworkThread(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerNetworkThread.initializeResources();

            consumerNetworkThread.runOnce();
            time.sleep(10);
            consumerNetworkThread.runOnce();
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("time-between-network-thread-poll-avg", CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("time-between-network-thread-poll-max", CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
        }
    }

    @Test
    public void testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime() {
        try (Metrics metrics = new Metrics();
             AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics);
             ConsumerNetworkThread consumerNetworkThread = new ConsumerNetworkThread(
                     new LogContext(),
                     time,
                     applicationEventQueue,
                     applicationEventReaper,
                     () -> applicationEventProcessor,
                     () -> networkClientDelegate,
                     () -> requestManagers,
                     asyncConsumerMetrics
             )) {
            consumerNetworkThread.initializeResources();

            PollEvent event = new PollEvent(0);
            event.setEnqueuedMs(time.milliseconds());
            applicationEventQueue.add(event);
            asyncConsumerMetrics.recordApplicationEventQueueSize(1);

            time.sleep(10);
            consumerNetworkThread.runOnce();
            assertEquals(
                0,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-size", CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-time-avg", CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
            assertEquals(
                10,
                (double) metrics.metric(
                    metrics.metricName("application-event-queue-time-max", CONSUMER_METRIC_GROUP)
                ).metricValue()
            );
        }
    }
}
