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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
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
    private static final int LONG_RETRY_BACKOFF_MS = 10000;
    private static final int REQUEST_TIMEOUT_MS = 40000;
    private static final String GROUP_ID = "dummy-group";
    private static final String METRIC_GROUP_PREFIX = "consumer";

    private MockClient mockClient;
    private MockTime mockTime;
    private Node node;
    private Node coordinatorNode;
    private ConsumerNetworkClient consumerClient;
    private DummyCoordinator coordinator;

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
        this.consumerClient = new ConsumerNetworkClient(logContext, mockClient, metadata, mockTime,
                retryBackoffMs, REQUEST_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS);
        Metrics metrics = new Metrics();

        mockClient.updateMetadata(TestUtils.metadataUpdateWith(1, emptyMap()));
        this.node = metadata.fetch().nodes().get(0);
        this.coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        this.coordinator = new DummyCoordinator(consumerClient, metrics, mockTime, rebalanceTimeoutMs, retryBackoffMs, groupInstanceId);
    }

    @Test
    public void testCoordinatorDiscoveryBackoff() {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        // blackout the coordinator for 10 milliseconds to simulate a disconnect.
        // after backing off, we should be able to connect.
        mockClient.blackout(coordinatorNode, 10L);

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

            mockClient.respond(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED));

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

        mockTime.sleep(REBALANCE_TIMEOUT_MS - REQUEST_TIMEOUT_MS + 5000);
        assertTrue(consumerClient.poll(future, mockTime.timer(0)));
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

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.MEMBER_ID_REQUIRED));

        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                if (!(body instanceof JoinGroupRequest)) {
                    return false;
                }
                JoinGroupRequest joinGroupRequest = (JoinGroupRequest) body;
                if (!joinGroupRequest.data().memberId().equals(memberId)) {
                    return false;
                }
                return true;
            }
        }, joinGroupResponse(Errors.UNKNOWN_MEMBER_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.MEMBER_ID_REQUIRED.message(), future.exception().getMessage());
        assertTrue(coordinator.rejoinNeededOrPending());
        assertTrue(coordinator.hasValidMemberId());
        assertTrue(coordinator.hasMatchingGenerationId(generation));
        future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REBALANCE_TIMEOUT_MS)));
    }

    @Test
    public void testJoinGroupRequestWithFencedInstanceIdException() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(mockTime.timer(0));

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.FENCED_INSTANCE_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.FENCED_INSTANCE_ID.message(), future.exception().getMessage());
        // Make sure the exception is fatal.
        assertFalse(future.isRetriable());
    }

    @Test
    public void testSyncGroupRequestWithFencedInstanceIdException() {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.FENCED_INSTANCE_ID));

        assertThrows(FencedInstanceIdException.class, () -> coordinator.ensureActiveGroup());
    }

    @Test
    public void testHeartbeatRequestWithFencedInstanceIdException() throws InterruptedException {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.NONE));
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

        final String memberId = "memberId";
        final int generation = -1;

        mockClient.prepareResponse(joinGroupFollowerResponse(generation, memberId, JoinGroupResponse.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID));

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        assertTrue(consumerClient.poll(future, mockTime.timer(REQUEST_TIMEOUT_MS)));
        assertEquals(Errors.UNKNOWN_MEMBER_ID.message(), future.exception().getMessage());
        assertTrue(coordinator.rejoinNeededOrPending());
        assertTrue(coordinator.hasMatchingGenerationId(generation));
    }

    @Test
    public void testLeaveGroupSentWithGroupInstanceIdUnSet() {
        checkLeaveGroupRequestSent(Optional.empty());
        checkLeaveGroupRequestSent(Optional.of("groupInstanceId"));
    }

    private void checkLeaveGroupRequestSent(Optional<String> groupInstanceId) {
        setupCoordinator(RETRY_BACKOFF_MS, Integer.MAX_VALUE, groupInstanceId);

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        final RuntimeException e = new RuntimeException();

        // raise the error when the coordinator tries to send leave group request.
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                if (body instanceof LeaveGroupRequest)
                    throw e;
                return false;
            }
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
    public void testUncaughtExceptionInHeartbeatThread() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        final RuntimeException e = new RuntimeException();

        // raise the error when the background thread tries to send a heartbeat
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                if (body instanceof HeartbeatRequest)
                    throw e;
                return false;
            }
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
        setupCoordinator(LONG_RETRY_BACKOFF_MS);

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        coordinator.ensureActiveGroup();

        final CountDownLatch heartbeatDone = new CountDownLatch(1);
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                heartbeatDone.countDown();
                return body instanceof HeartbeatRequest;
            }
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

        mockClient.blackout(node, 50);
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
        }, joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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
        }, joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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
    public void testWakeupAfterJoinGroupReceived() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
                if (isJoinGroupRequest)
                    // wakeup after the request returns
                    consumerClient.wakeup();
                return isJoinGroupRequest;
            }
        }, joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterJoinGroupReceivedExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                boolean isJoinGroupRequest = body instanceof JoinGroupRequest;
                if (isJoinGroupRequest)
                    // wakeup after the request returns
                    consumerClient.wakeup();
                return isJoinGroupRequest;
            }
        }, joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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
    public void testWakeupAfterSyncGroupSent() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(AbstractRequest body) {
                invocations++;
                boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
                if (isSyncGroupRequest && invocations == 1)
                    // simulate wakeup after the request sent
                    throw new WakeupException();
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

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterSyncGroupSentExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(AbstractRequest body) {
                invocations++;
                boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
                if (isSyncGroupRequest && invocations == 1)
                    // simulate wakeup after the request sent
                    throw new WakeupException();
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
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
                if (isSyncGroupRequest)
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

        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    @Test
    public void testWakeupAfterSyncGroupReceivedExternalCompletion() throws Exception {
        setupCoordinator();

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                boolean isSyncGroupRequest = body instanceof SyncGroupRequest;
                if (isSyncGroupRequest)
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
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
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
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                boolean isHeartbeatRequest = body instanceof HeartbeatRequest;
                if (isHeartbeatRequest)
                    heartbeatReceived.set(true);
                return isHeartbeatRequest;
            }
        }, heartbeatResponse(Errors.UNKNOWN_SERVER_ERROR));
        return heartbeatReceived;
    }

    private void awaitFirstHeartbeat(final AtomicBoolean heartbeatReceived) throws Exception {
        mockTime.sleep(HEARTBEAT_INTERVAL_MS);
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return heartbeatReceived.get();
            }
        }, 3000, "Should have received a heartbeat request after joining the group");
    }

    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return FindCoordinatorResponse.prepareResponse(error, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(
                new JoinGroupResponseData()
                        .setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName("dummy-subprotocol")
                        .setMemberId(memberId)
                        .setLeader(leaderId)
                        .setMembers(Collections.emptyList())
        );
    }

    private JoinGroupResponse joinGroupResponse(Errors error) {
        return joinGroupFollowerResponse(JoinGroupResponse.UNKNOWN_GENERATION_ID,
            JoinGroupResponse.UNKNOWN_MEMBER_ID, JoinGroupResponse.UNKNOWN_MEMBER_ID, error);
    }

    private SyncGroupResponse syncGroupResponse(Errors error) {
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(new byte[0])
        );
    }

    public static class DummyCoordinator extends AbstractCoordinator {

        private int onJoinPrepareInvokes = 0;
        private int onJoinCompleteInvokes = 0;
        private boolean wakeupOnJoinComplete = false;

        public DummyCoordinator(ConsumerNetworkClient client,
                                Metrics metrics,
                                Time time,
                                int rebalanceTimeoutMs,
                                int retryBackoffMs,
                                Optional<String> groupInstanceId) {
            super(new LogContext(), client, GROUP_ID, groupInstanceId, rebalanceTimeoutMs,
                    SESSION_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, metrics, METRIC_GROUP_PREFIX, time, retryBackoffMs, !groupInstanceId.isPresent());
        }

        @Override
        protected String protocolType() {
            return "dummy";
        }

        @Override
        protected JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
            return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
                    Collections.singleton(new JoinGroupRequestData.JoinGroupRequestProtocol()
                            .setName("dummy-subprotocol")
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
