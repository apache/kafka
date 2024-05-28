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

    /**
     * // Make sure the tests include testing poll with params of (pollWaitTime, currentTimeMs)
     * 1. Add a test that have RM to return different poll times and ensure pollWaitTimeMs is computed correctly. i.e. takes the min of all
     * 2. Test maxTimeToWait with different request manager returns
     * 3. Remove the tests and create a commit for it so that we can look back later.
     */

    @ParameterizedTest
    @ValueSource(longs = {1, 100, 1000, 4999, 5001})
    public void testConsumerNetworkThreadWaitTimeComputations(long exampleTime) {
        List<Optional<? extends RequestManager>> requestManagersList = new ArrayList<>();
        requestManagersList.add(Optional.of(coordinatorRequestManager));
//        rmsList.add(Optional.of(commitRequestManager));
//        rmsList.add(Optional.of(heartbeatRequestManager));
//        rmsList.add(Optional.of(offsetsRequestManager));
        when(requestManagers.entries()).thenReturn(requestManagersList);

        NetworkClientDelegate.PollResult pollResult = new NetworkClientDelegate.PollResult(exampleTime);

        when(coordinatorRequestManager.poll(anyLong())).thenReturn(pollResult);
        when(coordinatorRequestManager.maximumTimeToWait(anyLong())).thenReturn(exampleTime);
//        when(commitRequestManager.poll(anyLong())).thenReturn(new NetworkClientDelegate.PollResult(10000L));
//        when(heartbeatRequestManager.poll(anyLong())).thenReturn(new NetworkClientDelegate.PollResult(10000L));
//        when(offsetsRequestManager.poll(anyLong())).thenReturn(new NetworkClientDelegate.PollResult(10000L));
        when(networkClientDelegate.addAll(pollResult)).thenReturn(pollResult.timeUntilNextPollMs);
        consumerNetworkThread.runOnce();

        assertEquals(consumerNetworkThread.maximumTimeToWait(), exampleTime);
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

    @Test
    public void testEnsureApplicationEventProcessorInvokesProcess() {
        //ApplicationEvent e = new PollEvent(100);
        //applicationEventsQueue.add(e);
        consumerNetworkThread.runOnce();
        verify(applicationEventProcessor, times(1)).process();
    }

    @Test
    void testRequestManagersArePolledOnce() {
        consumerNetworkThread.runOnce();
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).poll(anyLong())));
        requestManagers.entries().forEach(rmo -> rmo.ifPresent(rm -> verify(rm, times(1)).maximumTimeToWait(anyLong())));
        // We just need to test networkClientDelegate not networkClient
        verify(networkClientDelegate, times(1)).poll(anyLong(), anyLong());
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
