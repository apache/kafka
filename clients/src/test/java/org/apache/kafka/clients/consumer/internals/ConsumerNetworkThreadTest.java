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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.PollEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static org.apache.kafka.clients.consumer.internals.events.CompletableEvent.calculateDeadlineMs;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkThreadTest {
    static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 500;

    private final Time time;
    private final BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private final ApplicationEventProcessor applicationEventProcessor;
    private final OffsetsRequestManager offsetsRequestManager;
    private final HeartbeatRequestManager heartbeatRequestManager;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final ConsumerNetworkThread consumerNetworkThread;
    private final MockClient client;
    private final NetworkClientDelegate networkClientDelegate;
    private final RequestManagers requestManagers;
    private final CompletableEventReaper applicationEventReaper;
    private final LogContext logContext;
    private final ConsumerConfig config;

    ConsumerNetworkThreadTest() {
        this.config = mock(ConsumerConfig.class);
        this.networkClientDelegate = mock(NetworkClientDelegate.class);
        this.requestManagers = mock(RequestManagers.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.heartbeatRequestManager = mock(HeartbeatRequestManager.class);
        this.coordinatorRequestManager = mock(CoordinatorRequestManager.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);
        this.applicationEventReaper = mock(CompletableEventReaper.class);
        this.time = new MockTime();
        this.client = new MockClient(time);
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.logContext = new LogContext();

        this.consumerNetworkThread = new ConsumerNetworkThread(
                logContext,
                time,
                applicationEventsQueue,
                applicationEventReaper,
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
    public void testEnsureCloseStopsRunningThread() {
        assertTrue(consumerNetworkThread.isRunning(),
            "ConsumerNetworkThread should start running when created");

        consumerNetworkThread.close();
        assertFalse(consumerNetworkThread.isRunning(),
            "close() should make consumerNetworkThread.running false by calling closeInternal(Duration timeout)");
    }

    @ParameterizedTest
    @ValueSource(longs = {100, 4999, 5001})
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

        verify(networkClientDelegate).poll(exampleTime < 5001 ? exampleTime : 5000, time.milliseconds());
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

    @ParameterizedTest
    @MethodSource("applicationEvents")
    public void testApplicationEventIsProcessed(ApplicationEvent e) {
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();

        if (e instanceof CompletableEvent)
            verify(applicationEventReaper).add((CompletableEvent<?>) e);

        verify(applicationEventProcessor).process(any(e.getClass()));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testListOffsetsEventIsProcessed(boolean requireTimestamp) {
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsEvent(timestamps, calculateDeadlineMs(time, 100), requireTimestamp);
        applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
    }

    @Test
    public void testResetPositionsProcessFailureIsIgnored() {
        doThrow(new NullPointerException()).when(offsetsRequestManager).resetPositionsIfNeeded();

        ResetPositionsEvent event = new ResetPositionsEvent(calculateDeadlineMs(time, 100));
        applicationEventsQueue.add(event);
        assertDoesNotThrow(() -> consumerNetworkThread.runOnce());

        verify(applicationEventProcessor).process(any(ResetPositionsEvent.class));
    }

    @Test
    public void testPollResultTimer() {
        NetworkClientDelegate networkClientDelegate = new NetworkClientDelegate(
                time,
                config,
                logContext,
                client
        );

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

    @Test
    public void testMaximumTimeToWait() {
        // Initial value before runOnce has been called
        assertEquals(ConsumerNetworkThread.MAX_POLL_TIMEOUT_MS, consumerNetworkThread.maximumTimeToWait());

        when(requestManagers.entries()).thenReturn(Collections.singletonList(Optional.of(heartbeatRequestManager)));
        when(heartbeatRequestManager.maximumTimeToWait(time.milliseconds())).thenReturn((long) DEFAULT_HEARTBEAT_INTERVAL_MS);

        consumerNetworkThread.runOnce();
        // After runOnce has been called, it takes the default heartbeat interval from the heartbeat request manager
        assertEquals(DEFAULT_HEARTBEAT_INTERVAL_MS, consumerNetworkThread.maximumTimeToWait());
    }

    @Test
    public void testCleanupInvokesReaper() {
        LinkedList<NetworkClientDelegate.UnsentRequest> queue = new LinkedList<>();
        when(networkClientDelegate.unsentRequests()).thenReturn(queue);
        consumerNetworkThread.cleanup();
        verify(applicationEventReaper).reap(applicationEventsQueue);
    }

    @Test
    public void testRunOnceInvokesReaper() {
        consumerNetworkThread.runOnce();
        verify(applicationEventReaper).reap(any(Long.class));
    }

    @Test
    public void testSendUnsentRequests() {
        when(networkClientDelegate.hasAnyPendingRequests()).thenReturn(true).thenReturn(true).thenReturn(false);
        consumerNetworkThread.cleanup();
        assertFalse(networkClientDelegate.hasAnyPendingRequests());
    }

    private static Stream<Arguments> applicationEvents() {
        Time time1 = new MockTime();
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        final long currentTimeMs = time1.milliseconds();
        return Stream.of(
                Arguments.of(new PollEvent(100)),
                Arguments.of(new NewTopicsMetadataUpdateRequestEvent()),
                Arguments.of(new AsyncCommitEvent(new HashMap<>())),
                Arguments.of(new SyncCommitEvent(new HashMap<>(), 500)),
                Arguments.of(new ResetPositionsEvent(500)),
                Arguments.of(new ValidatePositionsEvent(500)),
                Arguments.of(new TopicMetadataEvent("topic", Long.MAX_VALUE)),
                Arguments.of(new AssignmentChangeEvent(offset, currentTimeMs)));
    }
}
