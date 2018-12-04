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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WorkerCoordinatorTest {

    private static final String LEADER_URL = "leaderUrl:8083";
    private static final String MEMBER_URL = "memberUrl:8083";

    private String connectorId1 = "connector1";
    private String connectorId2 = "connector2";
    private String connectorId3 = "connector3";
    private ConnectorTaskId taskId1x0 = new ConnectorTaskId(connectorId1, 0);
    private ConnectorTaskId taskId1x1 = new ConnectorTaskId(connectorId1, 1);
    private ConnectorTaskId taskId2x0 = new ConnectorTaskId(connectorId2, 0);
    private ConnectorTaskId taskId3x0 = new ConnectorTaskId(connectorId3, 0);

    private String groupId = "test-group";
    private int sessionTimeoutMs = 10;
    private int rebalanceTimeoutMs = 60;
    private int heartbeatIntervalMs = 2;
    private long retryBackoffMs = 100;
    private MockTime time;
    private MockClient client;
    private Node node;
    private Metadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    @Mock private KafkaConfigBackingStore configStorage;
    private WorkerCoordinator coordinator;

    private ClusterConfigState configState1;
    private ClusterConfigState configState2;
    private ClusterConfigState configStateSingleTaskConnectors;

    @Before
    public void setup() {
        LogContext loggerFactory = new LogContext();

        this.time = new MockTime();
        this.metadata = new Metadata(0, Long.MAX_VALUE, true);
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(TestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)));
        this.node = metadata.fetch().nodes().get(0);
        this.consumerClient = new ConsumerNetworkClient(loggerFactory, client, metadata, time, 100, 1000, heartbeatIntervalMs);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.configStorage = PowerMock.createMock(KafkaConfigBackingStore.class);

        this.coordinator = new WorkerCoordinator(
                loggerFactory,
                consumerClient,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                "consumer" + groupId,
                time,
                retryBackoffMs,
                LEADER_URL,
                configStorage,
                rebalanceListener);

        configState1 = new ClusterConfigState(
                1L,
                Collections.singletonMap(connectorId1, 1),
                Collections.singletonMap(connectorId1, (Map<String, String>) new HashMap<String, String>()),
                Collections.singletonMap(connectorId1, TargetState.STARTED),
                Collections.singletonMap(taskId1x0, (Map<String, String>) new HashMap<String, String>()),
                Collections.<String>emptySet()
        );

        Map<String, Integer> configState2ConnectorTaskCounts = new HashMap<>();
        configState2ConnectorTaskCounts.put(connectorId1, 2);
        configState2ConnectorTaskCounts.put(connectorId2, 1);
        Map<String, Map<String, String>> configState2ConnectorConfigs = new HashMap<>();
        configState2ConnectorConfigs.put(connectorId1, new HashMap<String, String>());
        configState2ConnectorConfigs.put(connectorId2, new HashMap<String, String>());
        Map<String, TargetState> configState2TargetStates = new HashMap<>();
        configState2TargetStates.put(connectorId1, TargetState.STARTED);
        configState2TargetStates.put(connectorId2, TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> configState2TaskConfigs = new HashMap<>();
        configState2TaskConfigs.put(taskId1x0, new HashMap<String, String>());
        configState2TaskConfigs.put(taskId1x1, new HashMap<String, String>());
        configState2TaskConfigs.put(taskId2x0, new HashMap<String, String>());
        configState2 = new ClusterConfigState(
                2L,
                configState2ConnectorTaskCounts,
                configState2ConnectorConfigs,
                configState2TargetStates,
                configState2TaskConfigs,
                Collections.<String>emptySet()
        );

        Map<String, Integer> configStateSingleTaskConnectorsConnectorTaskCounts = new HashMap<>();
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId1, 1);
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId2, 1);
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId3, 1);
        Map<String, Map<String, String>> configStateSingleTaskConnectorsConnectorConfigs = new HashMap<>();
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId1, new HashMap<String, String>());
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId2, new HashMap<String, String>());
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId3, new HashMap<String, String>());
        Map<String, TargetState> configStateSingleTaskConnectorsTargetStates = new HashMap<>();
        configStateSingleTaskConnectorsTargetStates.put(connectorId1, TargetState.STARTED);
        configStateSingleTaskConnectorsTargetStates.put(connectorId2, TargetState.STARTED);
        configStateSingleTaskConnectorsTargetStates.put(connectorId3, TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> configStateSingleTaskConnectorsTaskConfigs = new HashMap<>();
        configStateSingleTaskConnectorsTaskConfigs.put(taskId1x0, new HashMap<String, String>());
        configStateSingleTaskConnectorsTaskConfigs.put(taskId2x0, new HashMap<String, String>());
        configStateSingleTaskConnectorsTaskConfigs.put(taskId3x0, new HashMap<String, String>());
        configStateSingleTaskConnectors = new ClusterConfigState(
                2L,
                configStateSingleTaskConnectorsConnectorTaskCounts,
                configStateSingleTaskConnectorsConnectorConfigs,
                configStateSingleTaskConnectorsTargetStates,
                configStateSingleTaskConnectorsTaskConfigs,
                Collections.<String>emptySet()
        );
    }

    @After
    public void teardown() {
        this.metrics.close();
    }

    // We only test functionality unique to WorkerCoordinator. Most functionality is already well tested via the tests
    // that cover AbstractCoordinator & ConsumerCoordinator.

    @Test
    public void testMetadata() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        List<ProtocolMetadata> serialized = coordinator.metadata();
        assertEquals(1, serialized.size());

        ProtocolMetadata defaultMetadata = serialized.get(0);
        assertEquals(WorkerCoordinator.DEFAULT_SUBPROTOCOL, defaultMetadata.name());
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(defaultMetadata.metadata());
        assertEquals(1, state.offset());

        PowerMock.verifyAll();
    }

    @Test
    public void testNormalJoinGroupLeader() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        final String consumerId = "leader";

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, Long> memberConfigOffsets = new HashMap<>();
        memberConfigOffsets.put("leader", 1L);
        memberConfigOffsets.put("member", 1L);
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberConfigOffsets, Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(consumerId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().containsKey(consumerId);
            }
        }, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.singletonList(connectorId1),
                Collections.<ConnectorTaskId>emptyList(), Errors.NONE));
        coordinator.ensureActiveGroup();

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(0, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertFalse(rebalanceListener.assignment.failed());
        assertEquals(1L, rebalanceListener.assignment.offset());
        assertEquals("leader", rebalanceListener.assignment.leader());
        assertEquals(Collections.singletonList(connectorId1), rebalanceListener.assignment.connectors());
        assertEquals(Collections.emptyList(), rebalanceListener.assignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testNormalJoinGroupFollower() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        final String memberId = "member";

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        client.prepareResponse(new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(memberId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().isEmpty();
            }
        }, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.<String>emptyList(),
                Collections.singletonList(taskId1x0), Errors.NONE));
        coordinator.ensureActiveGroup();

        assertFalse(coordinator.rejoinNeededOrPending());
        assertEquals(0, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertFalse(rebalanceListener.assignment.failed());
        assertEquals(1L, rebalanceListener.assignment.offset());
        assertEquals(Collections.emptyList(), rebalanceListener.assignment.connectors());
        assertEquals(Collections.singletonList(taskId1x0), rebalanceListener.assignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testJoinLeaderCannotAssign() {
        // If the selected leader can't get up to the maximum offset, it will fail to assign and we should immediately
        // need to retry the join.

        // When the first round fails, we'll take an updated config snapshot
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);
        EasyMock.expect(configStorage.snapshot()).andReturn(configState2);

        PowerMock.replayAll();

        final String memberId = "member";

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // config mismatch results in assignment error
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        MockClient.RequestMatcher matcher = new MockClient.RequestMatcher() {
            @Override
            public boolean matches(AbstractRequest body) {
                SyncGroupRequest sync = (SyncGroupRequest) body;
                return sync.memberId().equals(memberId) &&
                        sync.generationId() == 1 &&
                        sync.groupAssignment().isEmpty();
            }
        };
        client.prepareResponse(matcher, syncGroupResponse(ConnectProtocol.Assignment.CONFIG_MISMATCH, "leader", 10L,
                Collections.<String>emptyList(), Collections.<ConnectorTaskId>emptyList(), Errors.NONE));
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        client.prepareResponse(matcher, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L,
                Collections.<String>emptyList(), Collections.singletonList(taskId1x0), Errors.NONE));
        coordinator.ensureActiveGroup();

        PowerMock.verifyAll();
    }

    @Test
    public void testRejoinGroup() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join the group once
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.<String>emptyList(),
                Collections.singletonList(taskId1x0), Errors.NONE));
        coordinator.ensureActiveGroup();

        assertEquals(0, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertFalse(rebalanceListener.assignment.failed());
        assertEquals(1L, rebalanceListener.assignment.offset());
        assertEquals(Collections.emptyList(), rebalanceListener.assignment.connectors());
        assertEquals(Collections.singletonList(taskId1x0), rebalanceListener.assignment.tasks());

        // and join the group again
        coordinator.requestRejoin();
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.singletonList(connectorId1),
                Collections.<ConnectorTaskId>emptyList(), Errors.NONE));
        coordinator.ensureActiveGroup();

        assertEquals(1, rebalanceListener.revokedCount);
        assertEquals(Collections.emptyList(), rebalanceListener.revokedConnectors);
        assertEquals(Collections.singletonList(taskId1x0), rebalanceListener.revokedTasks);
        assertEquals(2, rebalanceListener.assignedCount);
        assertFalse(rebalanceListener.assignment.failed());
        assertEquals(1L, rebalanceListener.assignment.offset());
        assertEquals(Collections.singletonList(connectorId1), rebalanceListener.assignment.connectors());
        assertEquals(Collections.emptyList(), rebalanceListener.assignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testLeaderPerformAssignment1() throws Exception {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        Map<String, ByteBuffer> configs = new HashMap<>();
        // Mark everyone as in sync with configState1
        configs.put("leader", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)));
        configs.put("member", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)));
        Map<String, ByteBuffer> result = Whitebox.invokeMethod(coordinator, "performAssignment", "leader", WorkerCoordinator.DEFAULT_SUBPROTOCOL, configs);

        // configState1 has 1 connector, 1 task
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertEquals(false, leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Collections.singletonList(connectorId1), leaderAssignment.connectors());
        assertEquals(Collections.emptyList(), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertEquals(false, memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.emptyList(), memberAssignment.connectors());
        assertEquals(Collections.singletonList(taskId1x0), memberAssignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testLeaderPerformAssignment2() throws Exception {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configState2);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        Map<String, ByteBuffer> configs = new HashMap<>();
        // Mark everyone as in sync with configState1
        configs.put("leader", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)));
        configs.put("member", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)));
        Map<String, ByteBuffer> result = Whitebox.invokeMethod(coordinator, "performAssignment", "leader", WorkerCoordinator.DEFAULT_SUBPROTOCOL, configs);

        // configState2 has 2 connector, 3 tasks and should trigger round robin assignment
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertEquals(false, leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Collections.singletonList(connectorId1), leaderAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertEquals(false, memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Collections.singletonList(taskId1x1), memberAssignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testLeaderPerformAssignmentSingleTaskConnectors() throws Exception {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configStateSingleTaskConnectors);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        Map<String, ByteBuffer> configs = new HashMap<>();
        // Mark everyone as in sync with configState1
        configs.put("leader", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)));
        configs.put("member", ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)));
        Map<String, ByteBuffer> result = Whitebox.invokeMethod(coordinator, "performAssignment", "leader", WorkerCoordinator.DEFAULT_SUBPROTOCOL, configs);

        // Round robin assignment when there are the same number of connectors and tasks should result in each being
        // evenly distributed across the workers, i.e. round robin assignment of connectors first, then followed by tasks
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertEquals(false, leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Arrays.asList(taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertEquals(false, memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());

        PowerMock.verifyAll();
    }


    private FindCoordinatorResponse groupCoordinatorResponse(Node node, Errors error) {
        return new FindCoordinatorResponse(error, node);
    }

    private JoinGroupResponse joinGroupLeaderResponse(int generationId, String memberId,
                                           Map<String, Long> configOffsets, Errors error) {
        Map<String, ByteBuffer> metadata = new HashMap<>();
        for (Map.Entry<String, Long> configStateEntry : configOffsets.entrySet()) {
            // We need a member URL, but it doesn't matter for the purposes of this test. Just set it to the member ID
            String memberUrl = configStateEntry.getKey();
            long configOffset = configStateEntry.getValue();
            ByteBuffer buf = ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(memberUrl, configOffset));
            metadata.put(configStateEntry.getKey(), buf);
        }
        return new JoinGroupResponse(error, generationId, WorkerCoordinator.DEFAULT_SUBPROTOCOL, memberId, memberId, metadata);
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(error, generationId, WorkerCoordinator.DEFAULT_SUBPROTOCOL, memberId, leaderId,
                Collections.<String, ByteBuffer>emptyMap());
    }

    private SyncGroupResponse syncGroupResponse(short assignmentError, String leader, long configOffset, List<String> connectorIds,
                                     List<ConnectorTaskId> taskIds, Errors error) {
        ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(assignmentError, leader, LEADER_URL, configOffset, connectorIds, taskIds);
        ByteBuffer buf = ConnectProtocol.serializeAssignment(assignment);
        return new SyncGroupResponse(error, buf);
    }

    private static class MockRebalanceListener implements WorkerRebalanceListener {
        public ConnectProtocol.Assignment assignment = null;

        public String revokedLeader;
        public Collection<String> revokedConnectors;
        public Collection<ConnectorTaskId> revokedTasks;

        public int revokedCount = 0;
        public int assignedCount = 0;

        @Override
        public void onAssigned(ConnectProtocol.Assignment assignment, int generation) {
            this.assignment = assignment;
            assignedCount++;
        }

        @Override
        public void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            this.revokedLeader = leader;
            this.revokedConnectors = connectors;
            this.revokedTasks = tasks;
            revokedCount++;
        }
    }
}
