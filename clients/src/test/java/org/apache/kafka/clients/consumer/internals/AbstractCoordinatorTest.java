/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractCoordinatorTest {

    private static final ByteBuffer EMPTY_DATA = ByteBuffer.wrap(new byte[0]);
    private static final int REBALANCE_TIMEOUT_MS = 60000;
    private static final int SESSION_TIMEOUT_MS = 10000;
    private static final int HEARTBEAT_INTERVAL_MS = 3000;
    private static final long RETRY_BACKOFF_MS = 100;
    private static final long REQUEST_TIMEOUT_MS = 40000;
    private static final String GROUP_ID = "dummy-group";
    private static final String METRIC_GROUP_PREFIX = "consumer";

    private MockClient mockClient;
    private MockTime mockTime;
    private Node node;
    private Node coordinatorNode;
    private ConsumerNetworkClient consumerClient;
    private DummyCoordinator coordinator;

    @Before
    public void setupCoordinator() {
        this.mockTime = new MockTime();
        this.mockClient = new MockClient(mockTime);

        Metadata metadata = new Metadata();
        this.consumerClient = new ConsumerNetworkClient(mockClient, metadata, mockTime,
                RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS);
        Metrics metrics = new Metrics();

        Cluster cluster = TestUtils.singletonCluster("topic", 1);
        metadata.update(cluster, mockTime.milliseconds());
        this.node = cluster.nodes().get(0);
        mockClient.setNode(node);

        this.coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        this.coordinator = new DummyCoordinator(consumerClient, metrics, mockTime);
    }

    @Test
    public void testCoordinatorDiscoveryBackoff() {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));

        // blackout the coordinator for 50 milliseconds to simulate a disconnect.
        // after backing off, we should be able to connect.
        mockClient.blackout(coordinatorNode, 50L);

        long initialTime = mockTime.milliseconds();
        coordinator.ensureCoordinatorReady();
        long endTime = mockTime.milliseconds();

        assertTrue(endTime - initialTime >= RETRY_BACKOFF_MS);
    }

    @Test
    public void testUncaughtExceptionInHeartbeatThread() throws Exception {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(syncGroupResponse(Errors.NONE));

        final RuntimeException e = new RuntimeException();

        // raise the error when the background thread tries to send a heartbeat
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                if (request.request().header().apiKey() == ApiKeys.HEARTBEAT.id)
                    throw e;
                return false;
            }
        }, heartbeatResponse(Errors.UNKNOWN));

        try {
            coordinator.ensureActiveGroup();
            mockTime.sleep(HEARTBEAT_INTERVAL_MS);
            synchronized (coordinator) {
                coordinator.notify();
            }
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
    public void testLookupCoordinator() throws Exception {
        mockClient.setNode(null);
        RequestFuture<Void> noBrokersAvailableFuture = coordinator.lookupCoordinator();
        assertTrue("Failed future expected", noBrokersAvailableFuture.failed());

        mockClient.setNode(node);
        RequestFuture<Void> future = coordinator.lookupCoordinator();
        assertFalse("Request not sent", future.isDone());
        assertTrue("New request sent while one is in progress", future == coordinator.lookupCoordinator());

        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady();
        assertTrue("New request not sent after previous completed", future != coordinator.lookupCoordinator());
    }

    @Test
    public void testWakeupAfterJoinGroupSent() throws Exception {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(ClientRequest request) {
                invocations++;
                boolean isJoinGroupRequest = request.request().header().apiKey() == ApiKeys.JOIN_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(ClientRequest request) {
                invocations++;
                boolean isJoinGroupRequest = request.request().header().apiKey() == ApiKeys.JOIN_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                boolean isJoinGroupRequest = request.request().header().apiKey() == ApiKeys.JOIN_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                boolean isJoinGroupRequest = request.request().header().apiKey() == ApiKeys.JOIN_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(ClientRequest request) {
                invocations++;
                boolean isSyncGroupRequest = request.request().header().apiKey() == ApiKeys.SYNC_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            private int invocations = 0;
            @Override
            public boolean matches(ClientRequest request) {
                invocations++;
                boolean isSyncGroupRequest = request.request().header().apiKey() == ApiKeys.SYNC_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                boolean isSyncGroupRequest = request.request().header().apiKey() == ApiKeys.SYNC_GROUP.id;
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
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        mockClient.prepareResponse(joinGroupFollowerResponse(1, "memberId", "leaderId", Errors.NONE));
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                boolean isSyncGroupRequest = request.request().header().apiKey() == ApiKeys.SYNC_GROUP.id;
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

        // the join group completes in this poll()
        consumerClient.poll(0);
        coordinator.ensureActiveGroup();

        assertEquals(1, coordinator.onJoinPrepareInvokes);
        assertEquals(1, coordinator.onJoinCompleteInvokes);

        awaitFirstHeartbeat(heartbeatReceived);
    }

    private AtomicBoolean prepareFirstHeartbeat() {
        final AtomicBoolean heartbeatReceived = new AtomicBoolean(false);
        mockClient.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(ClientRequest request) {
                boolean isHeartbeatRequest = request.request().header().apiKey() == ApiKeys.HEARTBEAT.id;
                if (isHeartbeatRequest)
                    heartbeatReceived.set(true);
                return isHeartbeatRequest;
            }
        }, heartbeatResponse(Errors.UNKNOWN));
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

    private Struct groupCoordinatorResponse(Node node, Errors error) {
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(error.code(), node);
        return response.toStruct();
    }

    private Struct heartbeatResponse(Errors error) {
        HeartbeatResponse response = new HeartbeatResponse(error.code());
        return response.toStruct();
    }

    private Struct joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(error.code(), generationId, "dummy-subprotocol", memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap()).toStruct();
    }

    private Struct syncGroupResponse(Errors error) {
        return new SyncGroupResponse(error.code(), ByteBuffer.allocate(0)).toStruct();
    }

    public class DummyCoordinator extends AbstractCoordinator {

        private int onJoinPrepareInvokes = 0;
        private int onJoinCompleteInvokes = 0;

        public DummyCoordinator(ConsumerNetworkClient client,
                                Metrics metrics,
                                Time time) {
            super(client, GROUP_ID, REBALANCE_TIMEOUT_MS, SESSION_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, metrics,
                    METRIC_GROUP_PREFIX, time, RETRY_BACKOFF_MS);
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
            onJoinCompleteInvokes++;
        }
    }

}
