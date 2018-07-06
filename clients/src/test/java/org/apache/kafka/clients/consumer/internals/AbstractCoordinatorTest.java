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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
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
        setupCoordinator(RETRY_BACKOFF_MS, REBALANCE_TIMEOUT_MS);
    }

    private void setupCoordinator(int retryBackoffMs) {
        setupCoordinator(retryBackoffMs, REBALANCE_TIMEOUT_MS);
    }

    private void setupCoordinator(int retryBackoffMs, int rebalanceTimeoutMs) {
        this.mockTime = new MockTime();
        this.mockClient = new MockClient(mockTime);

        Metadata metadata = new Metadata(retryBackoffMs, 60 * 60 * 1000L, true);
        this.consumerClient = new ConsumerNetworkClient(new LogContext(), mockClient, metadata, mockTime,
                retryBackoffMs, REQUEST_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS);
        Metrics metrics = new Metrics();

        Cluster cluster = TestUtils.singletonCluster("topic", 1);
        metadata.update(cluster, Collections.emptySet(), mockTime.milliseconds());
        this.node = cluster.nodes().get(0);
        mockClient.setNode(node);

        this.coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        this.coordinator = new DummyCoordinator(consumerClient, metrics, mockTime, rebalanceTimeoutMs, retryBackoffMs);
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
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
        long endTime = mockTime.milliseconds();

        assertTrue(endTime - initialTime >= RETRY_BACKOFF_MS);
    }

    @Test
    public void testTimeoutAndRetryJoinGroupIfNeeded() throws Exception {
        setupCoordinator();
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(0);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            long firstAttemptStartMs = mockTime.milliseconds();
            Future<Boolean> firstAttempt = executor.submit(() ->
                    coordinator.joinGroupIfNeeded(REQUEST_TIMEOUT_MS, firstAttemptStartMs));

            mockTime.sleep(REQUEST_TIMEOUT_MS);
            assertFalse(firstAttempt.get());
            assertTrue(consumerClient.hasPendingRequests(coordinatorNode));

            mockClient.respond(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
            mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

            long secondAttemptMs = mockTime.milliseconds();
            Future<Boolean> secondAttempt = executor.submit(() ->
                    coordinator.joinGroupIfNeeded(REQUEST_TIMEOUT_MS, secondAttemptMs));

            assertTrue(secondAttempt.get());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testJoinGroupRequestTimeout() {
        setupCoordinator(RETRY_BACKOFF_MS, REBALANCE_TIMEOUT_MS);
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(0);

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();

        mockTime.sleep(REQUEST_TIMEOUT_MS + 1);
        assertFalse(consumerClient.poll(future, 0));

        mockTime.sleep(REBALANCE_TIMEOUT_MS - REQUEST_TIMEOUT_MS + 5000);
        assertTrue(consumerClient.poll(future, 0));
    }

    @Test
    public void testJoinGroupRequestMaxTimeout() {
        // Ensure we can handle the maximum allowed rebalance timeout

        setupCoordinator(RETRY_BACKOFF_MS, Integer.MAX_VALUE);
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(0);

        RequestFuture<ByteBuffer> future = coordinator.sendJoinGroupRequest();
        assertFalse(consumerClient.poll(future, 0));

        mockTime.sleep(Integer.MAX_VALUE + 1L);
        assertTrue(consumerClient.poll(future, 0));
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

        mockClient.setNode(null);
        RequestFuture<Void> noBrokersAvailableFuture = coordinator.lookupCoordinator();
        assertTrue("Failed future expected", noBrokersAvailableFuture.failed());

        mockClient.setNode(node);
        RequestFuture<Void> future = coordinator.lookupCoordinator();
        assertFalse("Request not sent", future.isDone());
        assertSame("New request sent while one is in progress", future, coordinator.lookupCoordinator());

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
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
        consumerClient.poll(0);
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
        consumerClient.poll(0);
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
        consumerClient.poll(0);
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
        consumerClient.poll(0);
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
            coordinator.ensureCoordinatorReady(Long.MAX_VALUE);
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
        return new FindCoordinatorResponse(error, node);
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(error);
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(error, generationId, "dummy-subprotocol", memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap());
    }

    private SyncGroupResponse syncGroupResponse(Errors error) {
        return new SyncGroupResponse(error, ByteBuffer.allocate(0));
    }

    public static class DummyCoordinator extends AbstractCoordinator {

        private int onJoinPrepareInvokes = 0;
        private int onJoinCompleteInvokes = 0;
        private boolean wakeupOnJoinComplete = false;

        public DummyCoordinator(ConsumerNetworkClient client,
                                Metrics metrics,
                                Time time,
                                int rebalanceTimeoutMs,
                                int retryBackoffMs) {
            super(new LogContext(), client, GROUP_ID, rebalanceTimeoutMs, SESSION_TIMEOUT_MS,
                    HEARTBEAT_INTERVAL_MS, metrics, METRIC_GROUP_PREFIX, time, retryBackoffMs, false);
        }

        @Override
        protected String protocolType() {
            return "dummy";
        }

        @Override
        protected List<JoinGroupRequest.ProtocolMetadata> metadata() {
            return Collections.singletonList(new JoinGroupRequest.ProtocolMetadata("dummy-subprotocol", EMPTY_DATA));
        }

        @Override
        protected Map<String, ByteBuffer> performAssignment(String leaderId, String protocol, Map<String, ByteBuffer> allMemberMetadata) {
            Map<String, ByteBuffer> assignment = new HashMap<>();
            for (Map.Entry<String, ByteBuffer> metadata : allMemberMetadata.entrySet())
                assignment.put(metadata.getKey(), EMPTY_DATA);
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
