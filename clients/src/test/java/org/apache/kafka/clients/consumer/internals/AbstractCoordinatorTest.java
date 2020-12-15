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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractCoordinatorTest {
    private static final ByteBuffer EMPTY_DATA = ByteBuffer.wrap(new byte[0]);
    private static final int REBALANCE_TIMEOUT_MS = 60000;
    private static final int SESSION_TIMEOUT_MS = 10000;
    private static final int HEARTBEAT_INTERVAL_MS = 3000;
    private static final int RETRY_BACKOFF_MS = 100;
    private static final int REQUEST_TIMEOUT_MS = 40000;
    private static final String GROUP_ID = "dummy-group";
    private static final String METRIC_GROUP_PREFIX = "consumer";
    private static final String PROTOCOL_TYPE = "dummy";
    private static final String PROTOCOL_NAME = "dummy-subprotocol";

    private Node node;
    private Metrics metrics;
    private MockTime mockTime;
    private Node coordinatorNode;
    private MockClient mockClient;
    private DummyCoordinator coordinator;
    private ConsumerNetworkClient consumerClient;

    private final String memberId = "memberId";
    private final String leaderId = "leaderId";
    private final int defaultGeneration = -1;

    private void setupCoordinator() {
        setupCoordinator(RETRY_BACKOFF_MS, REBALANCE_TIMEOUT_MS,
            Optional.empty());
    }

    private void setupCoordinator(int retryBackoffMs) {
        setupCoordinator(retryBackoffMs, REBALANCE_TIMEOUT_MS,
            Optional.empty());
    }

    private void setupCoordinator(int retryBackoffMs, int rebalanceTimeoutMs, Optional<String> groupInstanceId) {
        LogContext logContext = new LogContext();
        this.mockTime = new MockTime();
        ConsumerMetadata metadata = new ConsumerMetadata(retryBackoffMs, 60 * 60 * 1000L,
                false, false, new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST),
                logContext, new ClusterResourceListeners());

        this.mockClient = new MockClient(mockTime, metadata);
        this.consumerClient = new ConsumerNetworkClient(logContext,
                                                        mockClient,
                                                        metadata,
                                                        mockTime,
                                                        retryBackoffMs,
                                                        REQUEST_TIMEOUT_MS,
                                                        HEARTBEAT_INTERVAL_MS);
        metrics = new Metrics(mockTime);

        mockClient.updateMetadata(RequestTestUtils.metadataUpdateWith(1, emptyMap()));
        this.node = metadata.fetch().nodes().get(0);
        this.coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());

        GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(SESSION_TIMEOUT_MS,
                                                                        rebalanceTimeoutMs,
                                                                        HEARTBEAT_INTERVAL_MS,
                                                                        GROUP_ID,
                                                                        groupInstanceId,
                                                                        retryBackoffMs,
                                                                        !groupInstanceId.isPresent());
        this.coordinator = new DummyCoordinator(rebalanceConfig,
                                                consumerClient,
                                                metrics,
                                                mockTime);
    }

    private void joinGroup() {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        final int generation = 1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        coordinator.ensureActiveGroup();
    }

    @Test
    public void testMetrics() {
        setupCoordinator();

        assertNotNull(getMetric("heartbeat-response-time-max"));
        assertNotNull(getMetric("heartbeat-rate"));
        assertNotNull(getMetric("heartbeat-total"));
        assertNotNull(getMetric("last-heartbeat-seconds-ago"));
        assertNotNull(getMetric("join-time-avg"));
        assertNotNull(getMetric("join-time-max"));
        assertNotNull(getMetric("join-rate"));
        assertNotNull(getMetric("join-total"));
        assertNotNull(getMetric("sync-time-avg"));
        assertNotNull(getMetric("sync-time-max"));
        assertNotNull(getMetric("sync-rate"));
        assertNotNull(getMetric("sync-total"));
        assertNotNull(getMetric("rebalance-latency-avg"));
        assertNotNull(getMetric("rebalance-latency-max"));
        assertNotNull(getMetric("rebalance-latency-total"));
        assertNotNull(getMetric("rebalance-rate-per-hour"));
        assertNotNull(getMetric("rebalance-total"));
        assertNotNull(getMetric("last-rebalance-seconds-ago"));
        assertNotNull(getMetric("failed-rebalance-rate-per-hour"));
        assertNotNull(getMetric("failed-rebalance-total"));

        metrics.sensor("heartbeat-latency").record(1.0d);
        metrics.sensor("heartbeat-latency").record(6.0d);
        metrics.sensor("heartbeat-latency").record(2.0d);

        assertEquals(6.0d, getMetric("heartbeat-response-time-max").metricValue());
        assertEquals(0.1d, getMetric("heartbeat-rate").metricValue());
        assertEquals(3.0d, getMetric("heartbeat-total").metricValue());

        assertEquals(-1.0d, getMetric("last-heartbeat-seconds-ago").metricValue());
        coordinator.heartbeat().sentHeartbeat(mockTime.milliseconds());
        assertEquals(0.0d, getMetric("last-heartbeat-seconds-ago").metricValue());
        mockTime.sleep(10 * 1000L);
        assertEquals(10.0d, getMetric("last-heartbeat-seconds-ago").metricValue());

        metrics.sensor("join-latency").record(1.0d);
        metrics.sensor("join-latency").record(6.0d);
        metrics.sensor("join-latency").record(2.0d);

        assertEquals(3.0d, getMetric("join-time-avg").metricValue());
        assertEquals(6.0d, getMetric("join-time-max").metricValue());
        assertEquals(0.1d, getMetric("join-rate").metricValue());
        assertEquals(3.0d, getMetric("join-total").metricValue());

        metrics.sensor("sync-latency").record(1.0d);
        metrics.sensor("sync-latency").record(6.0d);
        metrics.sensor("sync-latency").record(2.0d);

        assertEquals(3.0d, getMetric("sync-time-avg").metricValue());
        assertEquals(6.0d, getMetric("sync-time-max").metricValue());
        assertEquals(0.1d, getMetric("sync-rate").metricValue());
        assertEquals(3.0d, getMetric("sync-total").metricValue());

        metrics.sensor("rebalance-latency").record(1.0d);
        metrics.sensor("rebalance-latency").record(6.0d);
        metrics.sensor("rebalance-latency").record(2.0d);

        assertEquals(3.0d, getMetric("rebalance-latency-avg").metricValue());
        assertEquals(6.0d, getMetric("rebalance-latency-max").metricValue());
        assertEquals(9.0d, getMetric("rebalance-latency-total").metricValue());
        assertEquals(360.0d, getMetric("rebalance-rate-per-hour").metricValue());
        assertEquals(3.0d, getMetric("rebalance-total").metricValue());

        metrics.sensor("failed-rebalance").record(1.0d);
        metrics.sensor("failed-rebalance").record(6.0d);
        metrics.sensor("failed-rebalance").record(2.0d);

        assertEquals(360.0d, getMetric("failed-rebalance-rate-per-hour").metricValue());
        assertEquals(3.0d, getMetric("failed-rebalance-total").metricValue());

        assertEquals(-1.0d, getMetric("last-rebalance-seconds-ago").metricValue());
        coordinator.setLastRebalanceTime(mockTime.milliseconds());
        assertEquals(0.0d, getMetric("last-rebalance-seconds-ago").metricValue());
        mockTime.sleep(10 * 1000L);
        assertEquals(10.0d, getMetric("last-rebalance-seconds-ago").metricValue());
    }

    private KafkaMetric getMetric(final String name) {
        return metrics.metrics().get(metrics.metricName(name, "consumer-coordinator-metrics"));
    }

    @Test
    public void testCoordinatorDiscoveryBackoff() {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        // cut out the coordinator for 10 milliseconds to simulate a disconnect.
        // after backing off, we should be able to connect.
        mockClient.backoff(coordinatorNode, 10L);

        long initialTime = mockTime.milliseconds();
        coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE));
        long endTime = mockTime.milliseconds();

        assertTrue(endTime - initialTime >= RETRY_BACKOFF_MS);
    }

    @Test
    public void testTimeoutAndRetryJoinGroupIfNeeded() throws Exception {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            Timer firstAttemptTimer = mockTime.timer(REQUEST_TIMEOUT_MS);
            Future<Boolean> firstAttempt = executor.submit(() -> coordinator.joinGroupIfNeeded(firstAttemptTimer));

            mockTime.sleep(REQUEST_TIMEOUT_MS);
            assertFalse(firstAttempt.get());
            assertTrue(consumerClient.hasPendingRequests(coordinatorNode));

            mockClient.respond(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
            mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

            Timer secondAttemptTimer = mockTime.timer(REQUEST_TIMEOUT_MS);
            Future<Boolean> secondAttempt = executor.submit(() -> coordinator.joinGroupIfNeeded(secondAttemptTimer));

            assertTrue(secondAttempt.get());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testGroupMaxSizeExceptionIsFatal() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(joinGroupFollowerResponse(defaultGeneration, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertTrue(future.exception().getClass().isInstance(Errors.GROUP_MAX_SIZE_REACHED.exception()));
        assertFalse(future.isRetriable());
    }

    @Test
    public void testJoinGroupRequestTimeout() {
        setupCoordinator(RETRY_BACKOFF_MS, REBALANCE_TIMEOUT_MS,
            Optional.empty());
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        mockTime.sleep(REQUEST_TIMEOUT_MS + 1);
        assertFalse(consumerClient.poll(future, mockTime.timer(0)));

        mockTime.sleep(REBALANCE_TIMEOUT_MS - REQUEST_TIMEOUT_MS + AbstractCoordinator.JOIN_GROUP_TIMEOUT_LAPSE);
        assertTrue(consumerClient.poll(future, mockTime.timer(0)));
        assertTrue(future.exception() instanceof DisconnectException);
    }

    @Test
    public void testJoinGroupRequestTimeoutLowerBoundedByDefaultRequestTimeout() {
        int rebalanceTimeoutMs = REQUEST_TIMEOUT_MS - 10000;
        setupCoordinator(RETRY_BACKOFF_MS, rebalanceTimeoutMs, Optional.empty());
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        long expectedRequestDeadline = mockTime.milliseconds() + REQUEST_TIMEOUT_MS;
        mockTime.sleep(rebalanceTimeoutMs + AbstractCoordinator.JOIN_GROUP_TIMEOUT_LAPSE + 1);
        assertFalse(consumerClient.poll(future, mockTime.timer(0)));

        mockTime.sleep(expectedRequestDeadline - mockTime.milliseconds() + 1);
        assertTrue(consumerClient.poll(future, mockTime.timer(0)));
        assertTrue(future.exception() instanceof DisconnectException);
    }

    @Test
    public void testJoinGroupRequestMaxTimeout() {
        // Ensure we can handle the maximum allowed rebalance timeout

        setupCoordinator(RETRY_BACKOFF_MS, Integer.MAX_VALUE,
            Optional.empty());
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertFalse(consumerClient.poll(future, mockTime.timer(0)));

        mockTime.sleep(Integer.MAX_VALUE + 1L);
        assertTrue(consumerClient.poll(future, mockTime.timer(0)));
    }

    @Test
    public void testJoinGroupRequestWithMemberIdRequired() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(joinGroupFollowerResponse(defaultGeneration, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.MEMBER_ID_REQUIRED));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof JoinGroupRequest)) {
                return false;
            }
            JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
            return joinGroupRequest.data().memberId().equals(memberId);
        }, joinGroupResponse(Errors.UNKNOWN_MEMBER_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.MEMBER_ID_REQUIRED.message(), future.exception().getMessage());
        assertTrue(coordinator.rejoinNeededOrPending());
        assertTrue(coordinator.hasValidMemberId());
        assertTrue(coordinator.hasMatchingGenerationId(defaultGeneration));
        future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REBALANCE_TIMEOUT_MS)));
    }

    @Test
    public void testJoinGroupRequestWithFencedInstanceIdException() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(joinGroupFollowerResponse(defaultGeneration, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.FENCED_INSTANCE_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.FENCED_INSTANCE_ID.message(), future.exception().getMessage());
        // Make sure the exception is fatal.
        assertFalse(future.isRetriable());
    }

    @Test
    public void testJoinGroupProtocolTypeAndName() {
        final String wrongProtocolType = "wrong-type";
        final String wrongProtocolName = "wrong-name";

        // No Protocol Type in both JoinGroup and SyncGroup responses
        assertTrue(joinGroupWithProtocolTypeAndName(null, null, null));

        // Protocol Type in both JoinGroup and SyncGroup responses
        assertTrue(joinGroupWithProtocolTypeAndName(PROTOCOL_TYPE, PROTOCOL_TYPE, PROTOCOL_NAME));

        // Wrong protocol type in the JoinGroupResponse
        assertThrows(InconsistentGroupProtocolException.class,
            () -> joinGroupWithProtocolTypeAndName("wrong", null, null));

        // Correct protocol type in the JoinGroupResponse
        // Wrong protocol type in the SyncGroupResponse
        // Correct protocol name in the SyncGroupResponse
        assertThrows(InconsistentGroupProtocolException.class,
            () -> joinGroupWithProtocolTypeAndName(PROTOCOL_TYPE, wrongProtocolType, PROTOCOL_NAME));

        // Correct protocol type in the JoinGroupResponse
        // Correct protocol type in the SyncGroupResponse
        // Wrong protocol name in the SyncGroupResponse
        assertThrows(InconsistentGroupProtocolException.class,
            () -> joinGroupWithProtocolTypeAndName(PROTOCOL_TYPE, PROTOCOL_TYPE, wrongProtocolName));
    }

    @Test
    public void testNoGenerationWillNotTriggerProtocolNameCheck() {
        final String wrongProtocolName = "wrong-name";

        setupCoordinator();
        mockClient.reset();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof JoinGroupRequest)) {
                return false;
            }
            JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
            return joinGroupRequest.data().protocolType().equals(PROTOCOL_TYPE);
        }, joinGroupFollowerResponse(defaultGeneration, memberId,
            "memberid", Errors.NONE, PROTOCOL_TYPE));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof SyncGroupRequest)) {
                return false;
            }
            coordinator.resetGenerationOnLeaveGroup();

            SyncGroupRequest syncGroupRequest = (SyncGroupRequest) body;
            return syncGroupRequest.data().protocolType().equals(PROTOCOL_TYPE)
                       && syncGroupRequest.data().protocolName().equals(PROTOCOL_NAME);
        }, syncGroupResponse(Errors.NONE, PROTOCOL_TYPE, wrongProtocolName));

        // let the retry to complete successfully to break out of the while loop
        mockClient.prepareResponse(body -> {
            if (!(body instanceof JoinGroupRequest)) {
                return false;
            }
            JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
            return joinGroupRequest.data().protocolType().equals(PROTOCOL_TYPE);
        }, joinGroupFollowerResponse(1, memberId,
                "memberid", Errors.NONE, PROTOCOL_TYPE));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof SyncGroupRequest)) {
                return false;
            }

            SyncGroupRequest syncGroupRequest = (SyncGroupRequest) body;
            return syncGroupRequest.data().protocolType().equals(PROTOCOL_TYPE)
                    && syncGroupRequest.data().protocolName().equals(PROTOCOL_NAME);
        }, syncGroupResponse(Errors.NONE, PROTOCOL_TYPE, PROTOCOL_NAME));

        // No exception shall be thrown as the generation is reset.
        coordinator.joinGroupIfNeeded(mockTime.timer(100L));
    }

    private boolean joinGroupWithProtocolTypeAndName(String joinGroupResponseProtocolType,
                                                     String syncGroupResponseProtocolType,
                                                     String syncGroupResponseProtocolName) {
        setupCoordinator();
        mockClient.reset();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof JoinGroupRequest)) {
                return false;
            }
            JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
            return joinGroupRequest.data().protocolType().equals(PROTOCOL_TYPE);
        }, joinGroupFollowerResponse(defaultGeneration, memberId,
            "memberid", Errors.NONE, joinGroupResponseProtocolType));

        mockClient.prepareResponse(body -> {
            if (!(body instanceof SyncGroupRequest)) {
                return false;
            }
            SyncGroupRequest syncGroupRequest = (SyncGroupRequest) body;
            return syncGroupRequest.data().protocolType().equals(PROTOCOL_TYPE)
                && syncGroupRequest.data().protocolName().equals(PROTOCOL_NAME);
        }, syncGroupResponse(Errors.NONE, syncGroupResponseProtocolType, syncGroupResponseProtocolName));

        return coordinator.joinGroupIfNeeded(mockTime.timer(5000L));
    }

    @Test
    public void testSyncGroupRequestWithFencedInstanceIdException() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.FENCED_INSTANCE_ID));

        assertThrows(FencedInstanceIdException.class, () -> coordinator.ensureActiveGroup());
    }

    @Test
    public void testJoinGroupUnknownMemberResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
            "The join-group request was not sent");

        // change the generation after the join-group request
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId,
            currGen.memberId + "-new",
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(joinGroupFollowerResponse(currGen.generationId + 1, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID));

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertTrue(future.exception().getClass().isInstance(Errors.UNKNOWN_MEMBER_ID.exception()));

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testSyncGroupUnknownMemberResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE);
        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        TestUtils.waitForCondition(() -> {
            consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS));
            return !mockClient.requests().isEmpty();
        }, 2000,
            "The join-group request was not sent");

        mockClient.respond(joinGroupFollowerResponse(currGen.generationId, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        assertTrue(mockClient.requests().isEmpty());

        TestUtils.waitForCondition(() -> {
            consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS));
            return !mockClient.requests().isEmpty();
        }, 2000,
            "The sync-group request was not sent");

        // change the generation after the sync-group request
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId,
            currGen.memberId + "-new",
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(syncGroupResponse(Errors.UNKNOWN_MEMBER_ID));
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertTrue(future.exception().getClass().isInstance(Errors.UNKNOWN_MEMBER_ID.exception()));

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testSyncGroupIllegalGenerationResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        coordinator.setNewState(AbstractCoordinator.MemberState.PREPARING_REBALANCE);
        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        TestUtils.waitForCondition(() -> {
            consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS));
            return !mockClient.requests().isEmpty();
        }, 2000,
            "The join-group request was not sent");

        mockClient.respond(joinGroupFollowerResponse(currGen.generationId, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        assertTrue(mockClient.requests().isEmpty());

        TestUtils.waitForCondition(() -> {
            consumerClient.poll(mockTime.timer(REQUEST_TIMEOUT_MS));
            return !mockClient.requests().isEmpty();
        }, 2000,
            "The sync-group request was not sent");

        // change the generation after the sync-group request
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId,
            currGen.memberId + "-new",
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(syncGroupResponse(Errors.ILLEGAL_GENERATION));
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertTrue(future.exception().getClass().isInstance(Errors.ILLEGAL_GENERATION.exception()));

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatSentWhenCompletingRebalance() throws Exception {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        coordinator.setNewState(AbstractCoordinator.MemberState.COMPLETING_REBALANCE);

        // the heartbeat should be sent out during a rebalance
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);
        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
                "The heartbeat request was not sent");
        assertTrue(coordinator.heartbeat().hasInflight());

        mockClient.respond(heartbeatResponse(Errors.REBALANCE_IN_PROGRESS));
        assertEquals(currGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatIllegalGenerationResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        // let the heartbeat thread send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);

        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
            "The heartbeat request was not sent");
        assertTrue(coordinator.heartbeat().hasInflight());

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId + 1,
            currGen.memberId,
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(heartbeatResponse(Errors.ILLEGAL_GENERATION));

        // the heartbeat error code should be ignored
        TestUtils.waitForCondition(() -> {
            coordinator.pollHeartbeat(mockTime.milliseconds());
            return !coordinator.heartbeat().hasInflight();
        }, 2000,
            "The heartbeat response was not received");

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatUnknownMemberResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);

        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
            "The heartbeat request was not sent");
        assertTrue(coordinator.heartbeat().hasInflight());

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId,
            currGen.memberId + "-new",
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(heartbeatResponse(Errors.UNKNOWN_MEMBER_ID));

        // the heartbeat error code should be ignored
        TestUtils.waitForCondition(() -> {
            coordinator.pollHeartbeat(mockTime.milliseconds());
            return !coordinator.heartbeat().hasInflight();
        }, 2000,
            "The heartbeat response was not received");

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatRebalanceInProgressResponseDuringRebalancing() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);

        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
            "The heartbeat request was not sent");

        assertTrue(coordinator.heartbeat().hasInflight());

        mockClient.respond(heartbeatResponse(Errors.REBALANCE_IN_PROGRESS));

        coordinator.requestRejoin();

        TestUtils.waitForCondition(() -> {
            coordinator.ensureActiveGroup(new MockTime(1L).timer(100L));
            return !coordinator.heartbeat().hasInflight();
        },
            2000,
            "The heartbeat response was not received");

        // the generation would not be reset while the rebalance is in progress
        assertEquals(currGen, coordinator.generation());

        mockClient.respond(joinGroupFollowerResponse(currGen.generationId, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        coordinator.ensureActiveGroup();
        assertEquals(currGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatInstanceFencedResponseWithOldGeneration() throws InterruptedException {
        setupCoordinator();
        joinGroup();

        final AbstractCoordinator.Generation currGen = coordinator.generation();

        // let the heartbeat request to send out a request
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);

        TestUtils.waitForCondition(() -> !mockClient.requests().isEmpty(), 2000,
            "The heartbeat request was not sent");
        assertTrue(coordinator.heartbeat().hasInflight());

        // change the generation
        final AbstractCoordinator.Generation newGen = new AbstractCoordinator.Generation(
            currGen.generationId,
            currGen.memberId + "-new",
            currGen.protocolName);
        coordinator.setNewGeneration(newGen);

        mockClient.respond(heartbeatResponse(Errors.FENCED_INSTANCE_ID));

        // the heartbeat error code should be ignored
        TestUtils.waitForCondition(() -> {
            coordinator.pollHeartbeat(mockTime.milliseconds());
            return !coordinator.heartbeat().hasInflight();
        }, 2000,
            "The heartbeat response was not received");

        // the generation should not be reset
        assertEquals(newGen, coordinator.generation());
    }

    @Test
    public void testHeartbeatRequestWithFencedInstanceIdException() throws InterruptedException {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        mockClient.prepareResponse(heartbeatResponse(Errors.FENCED_INSTANCE_ID));

        try {
            coordinator.ensureActiveGroup();
            mockTime.sleep(HEARTBEAT_INTERVAL_MS);
            long startMs = System.currentTimeMillis();
            while (System.currentTimeMillis() - startMs < 1000) {
                Thread.sleep(10);
                coordinator.pollHeartbeat(mockTime.milliseconds());
            }
            fail("Expected pollHeartbeat to raise fenced instance id exception in 1 second");
        } catch (RuntimeException exception) {
            assertTrue(exception instanceof FencedInstanceIdException);
        }
    }

    @Test
    public void testJoinGroupRequestWithGroupInstanceIdNotFound() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        mockClient.prepareResponse(joinGroupFollowerResponse(defaultGeneration, memberId, JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.UNKNOWN_MEMBER_ID.message(), future.exception().getMessage());
        assertTrue(coordinator.rejoinNeededOrPending());
        assertTrue(coordinator.hasUnknownGeneration());
    }

    @Test
    public void testLeaveGroupSentWithGroupInstanceIdUnSet() {
        checkLeaveGroupRequestSent(Optional.empty());
        checkLeaveGroupRequestSent(Optional.of("groupInstanceId"));
    }

    private void checkLeaveGroupRequestSent(Optional<String> groupInstanceId) {
        setupCoordinator(RETRY_BACKOFF_MS, Integer.MAX_VALUE, groupInstanceId);

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        final RuntimeException e = new RuntimeException();

        // raise the error when the coordinator tries to send leave group request.
        mockClient.prepareResponse(body -> {
            if (body instanceof LeaveGroupRequest)
                throw e;
            return false;
        }, heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR));

        try {
            coordinator.ensureActiveGroup();
            coordinator.close();
            if (coordinator.isDynamicMember()) {
                fail("Expected leavegroup to raise an error.");
            }
        } catch (RuntimeException exception) {
            if (coordinator.isDynamicMember()) {
                assertEquals(exception, e);
            } else {
                fail("Coordinator with group.instance.id set shouldn't send leave group request.");
            }
        }
    }

    @Test
    public void testHandleNormalLeaveGroupResponse() {
        MemberResponse memberResponse = new MemberResponse()
                                            .setMemberId(memberId)
                                            .setErrorCode(Errors.NONE.code());
        LeaveGroupResponse response =
            leaveGroupResponse(Collections.singletonList(memberResponse));
        RequestFuture<Void> leaveGroupFuture = setupLeaveGroup(response);
        assertNotNull(leaveGroupFuture);
        assertTrue(leaveGroupFuture.succeeded());
    }

    @Test
    public void testHandleMultipleMembersLeaveGroupResponse() {
        MemberResponse memberResponse = new MemberResponse()
                                            .setMemberId(memberId)
                                            .setErrorCode(Errors.NONE.code());
        LeaveGroupResponse response =
            leaveGroupResponse(Arrays.asList(memberResponse, memberResponse));
        RequestFuture<Void> leaveGroupFuture = setupLeaveGroup(response);
        assertNotNull(leaveGroupFuture);
        assertTrue(leaveGroupFuture.exception() instanceof IllegalStateException);
    }

    @Test
    public void testHandleLeaveGroupResponseWithEmptyMemberResponse() {
        LeaveGroupResponse response =
            leaveGroupResponse(Collections.emptyList());
        RequestFuture<Void> leaveGroupFuture = setupLeaveGroup(response);
        assertNotNull(leaveGroupFuture);
        assertTrue(leaveGroupFuture.succeeded());
    }

    @Test
    public void testHandleLeaveGroupResponseWithException() {
        MemberResponse memberResponse = new MemberResponse()
                                            .setMemberId(memberId)
                                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());
        LeaveGroupResponse response =
            leaveGroupResponse(Collections.singletonList(memberResponse));
        RequestFuture<Void> leaveGroupFuture = setupLeaveGroup(response);
        assertNotNull(leaveGroupFuture);
        assertTrue(leaveGroupFuture.exception() instanceof UnknownMemberIdException);
    }

    private RequestFuture<Void> setupLeaveGroup(LeaveGroupResponse leaveGroupResponse) {
        setupCoordinator(RETRY_BACKOFF_MS, Integer.MAX_VALUE, Optional.empty());

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        mockClient.prepareResponse(leaveGroupResponse);

        coordinator.ensureActiveGroup();
        return coordinator.maybeLeaveGroup("test maybe leave group");
    }

    @Test
    public void testUncaughtExceptionInHeartbeatThread() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        final RuntimeException e = new RuntimeException();

        // raise the error when the background thread tries to send a heartbeat
        mockClient.prepareResponse(body -> {
            if (body instanceof HeartbeatRequest)
                throw e;
            return false;
        }, heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR));

        try {
            coordinator.ensureActiveGroup();
            mockTime.sleep(HEARTBEAT_INTERVAL_MS);
            long startMs = System.currentTimeMillis();
            while (System.currentTimeMillis() - startMs < 1000) {
                Thread.sleep(10);
                coordinator.pollHeartbeat(mockTime.milliseconds());
            }
            fail("Expected pollHeartbeat to raise an error in 1 second");
        } catch (RuntimeException exception) {
            assertEquals(exception, e);
        }
    }

    @Test
    public void testPollHeartbeatAwakesHeartbeatThread() throws Exception {
        final int longRetryBackoffMs = 10000;
        setupCoordinator(longRetryBackoffMs);

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        coordinator.ensureActiveGroup();

        final CountDownLatch heartbeatDone = new CountDownLatch(1);
        mockClient.prepareResponse(body -> {
            heartbeatDone.countDown();
            return body instanceof HeartbeatRequest;
        }, heartbeatResponse(Errors.NONE));

        mockTime.sleep(HEARTBEAT_INTERVAL_MS);
        coordinator.pollHeartbeat(mockTime.milliseconds());

        if (!heartbeatDone.await(1, TimeUnit.SECONDS)) {
            fail("Should have received a heartbeat request after calling pollHeartbeat");
        }
    }

    @Test
    public void testLookupCoordinator() {
        setupCoordinator();

        mockClient.backoff(node, 50);
        RequestFuture<Void> noBrokersAvailableFuture = coordinator.lookupCoordinator();
        assertTrue("Failed future expected", noBrokersAvailableFuture.failed());
        mockTime.sleep(50);

        RequestFuture<Void> future = coordinator.lookupCoordinator();
        assertFalse("Request not sent", future.isDone());
        assertSame("New request sent while one is in progress", future, coordinator.lookupCoordinator());

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE));
        assertNotSame("New request not sent after previous completed", future, coordinator.lookupCoordinator());
    }

    @Test
    public void testWakeupAfterJoinGroupSent() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(AbstractRequest body) {
                invocations++;
                boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
                if (isJoinGroupRequest && invocations == 1)
                    // simulate wakeup before the request returns
                    throw new WakeupException();
                return isJoinGroupRequest;
            }
        }, joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException ignored) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterJoinGroupSentExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(AbstractRequest body) {
                invocations++;
                boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
                if (isJoinGroupRequest && invocations == 1)
                    // simulate wakeup before the request returns
                    throw new WakeupException();
                return isJoinGroupRequest;
            }
        }, joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException ignored) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0));
        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterJoinGroupReceived() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(body -> {
            boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
            if (isJoinGroupRequest)
                // wakeup after the request returns
                consumerClient.wakeup();
            return isJoinGroupRequest;
        }, joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException ignored) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterJoinGroupReceivedExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(body -> {
            boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
            if (isJoinGroupRequest)
                // wakeup after the request returns
                consumerClient.wakeup();
            return isJoinGroupRequest;
        }, joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException e) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0));
        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterSyncGroupSentExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(AbstractRequest body) {
                invocations++;
                boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
                if (isSyncGroupRequest && invocations == 1)
                    // wakeup after the request returns
                    consumerClient.wakeup();
                return isSyncGroupRequest;
            }
        }, syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException e) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        // the join group completes in this poll()
        consumerClient.poll(mockTime.timer(0));
        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterSyncGroupReceived() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(body -> {
            boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
            if (isSyncGroupRequest)
                // wakeup after the request returns
                consumerClient.wakeup();
            return isSyncGroupRequest;
        }, syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException ignored) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterSyncGroupReceivedExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(body -> {
            boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
            if (isSyncGroupRequest)
                // wakeup after the request returns
                consumerClient.wakeup();
            return isSyncGroupRequest;
        }, syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException e) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupInOnJoinComplete() throws Exception {
        setupCoordinator();

        coordinator.wakeupOnJoinComplete = true;
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, memberId, leaderId, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));
        AtomicBoolean heartbeatReceived = prepareFirstHeartbeat();

        try {
            coordinator.ensureActiveGroup();
            fail("Should have woken up from ensureActiveGroup()");
        } catch (WakeupException ignored) {
        }

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(0, coordinator.onJoinCompleteInvokes);
        assertFalse(heartbeatReceived.get());

        // the join group completes in this poll()
        coordinator.wakeupOnJoinComplete = false;
        consumerClient.poll(mockTime.timer(0));
        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testAuthenticationErrorInEnsureCoordinatorReady() {
        setupCoordinator();

        mockClient.createPendingAuthenticationError(node, 300);

        try {
            coordinator.ensureCoordinatorReady(mockTime.timer(Long.MAX_VALUE));
            fail("Expected an authentication error.");
        } catch (AuthenticationException e) {
            // OK
        }
    }

    private AtomicBoolean prepareFirstHeartbeat() {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        mockClient.prepareResponse(body -> {
            boolean isHeartbeatRequest = body instanceof HeartbeatRequest;
            if (isHeartbeatRequest)
                heartbeatReceived.set(true);
            return isHeartbeatRequest;
        }, heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR));
        return heartbeatReceived;
    }

    private void awaitFirstHeartbeat(final AtomicBoolean heartbeatReceived) throws Exception {
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);
        TestUtils.waitForCondition(heartbeatReceived::get,
            3000, "Should have received a heartbeat request after joining the group");
    }

    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return FindCoordinatorResponse.prepareResponse(error, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId,
                                                        String memberId,
                                                        String leaderId,
                                                        Errors error) {
        return joinGroupFollowerResponse(generationId, memberId, leaderId, error, null);
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId,
                                                        String memberId,
                                                        String leaderId,
                                                        Errors error,
                                                        String protocolType) {
        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolType(protocolType)
                        .setProtocolName(PROTOCOL_NAME)
                        .setMemberId(memberId)
                        .setLeader(leaderId)
                        .setMembers(Collections.emptyList())
        );
    }

    private JoinGroupResponse joinGroupResponse(Errors error) {
        return joinGroupFollowerResponse(JoinGroupRequest.UNKNOWN_GENERATION_ID,
            JoinGroupRequest.UNKNOWN_MEMBER_ID, JoinGroupRequest.UNKNOWN_MEMBER_ID, error);
    }

    private SyncGroupResponse syncGroupResponse(Errors error) {
        return syncGroupResponse(error, null, null);
    }

    private SyncGroupResponse syncGroupResponse(Errors error,
                                                String protocolType,
                                                String protocolName) {
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setProtocolType(protocolType)
                        .setProtocolName(protocolName)
                        .setAssignment(new byte[0])
        );
    }

    private LeaveGroupResponse leaveGroupResponse(List<MemberResponse> members) {
        return new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMembers(members));
    }

    public static class DummyCoordinator extends AbstractCoordinator {

        private int onJoinPrepareInvokes = 0;
        private int onJoinCompleteInvokes = 0;
        private boolean wakeupOnJoinComplete = false;

        DummyCoordinator(GroupRebalanceConfig rebalanceConfig,
                         ConsumerNetworkClient client,
                         Metrics metrics,
                         Time time) {
            super(rebalanceConfig, new LogContext(), client, metrics, METRIC_GROUP_PREFIX, time);
        }

        @Override
        protected String protocolType() {
            return PROTOCOL_TYPE;
        }

        @Override
        protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
            return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                    Collections.singleton(new JoinGroupRequestData.JoinGroupRequestProtocol()
                            .setName(PROTOCOL_NAME)
                            .setMetadata(EMPTY_DATA.array())).iterator()
            );
        }

        @Override
        protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                            String protocol,
                                                            List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata) {
            Map<String, ByteBuffer> assignment = new HashMap<>();
            for (JoinGroupResponseData.JoinGroupResponseMember member : allMemberMetadata) {
                assignment.put(member.memberId(), EMPTY_DATA);
            }
            return assignment;
        }

        @Override
        protected void onJoinPrepare(int generation, String memberId) {
            onJoinPrepareInvokes++;
        }

        @Override
        protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {
            if (wakeupOnJoinComplete)
                throw new WakeupException();
            onJoinCompleteInvokes++;
        }
    }

}
