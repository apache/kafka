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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.easymock.PowerMock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
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
    private GroupRebalanceConfig rebalanceConfig;
    private WorkerCoordinator coordinator;

    private ClusterConfigState configState1;
    private ClusterConfigState configState2;
    private ClusterConfigState configStateSingleTaskConnectors;

    // Arguments are:
    // - Protocol type
    // - Expected metadata size
    @Parameters
    public static Iterable<?> mode() {
        return Arrays.asList(new Object[][]{
                {EAGER, 1},
                {COMPATIBLE, 2}});
    }

    @Parameter
    public ConnectProtocolCompatibility compatibility;

    @Parameter(1)
    public int expectedMetadataSize;

    @Before
    public void setup() {
        LogContext logContext = new LogContext();

        this.time = new MockTime();
        this.metadata = new Metadata(0, Long.MAX_VALUE, logContext, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)));
        this.node = metadata.fetch().nodes().get(0);
        this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100, 1000, heartbeatIntervalMs);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();
        this.configStorage = PowerMock.createMock(KafkaConfigBackingStore.class);
        this.rebalanceConfig = new GroupRebalanceConfig(sessionTimeoutMs,
                                                        rebalanceTimeoutMs,
                                                        heartbeatIntervalMs,
                                                        groupId,
                                                        Optional.empty(),
                                                        retryBackoffMs,
                                                        true);
        this.coordinator = new WorkerCoordinator(rebalanceConfig,
                                                 logContext,
                                                 consumerClient,
                                                 metrics,
                                                 "consumer" + groupId,
                                                 time,
                                                 LEADER_URL,
                                                 configStorage,
                                                 rebalanceListener,
                                                 compatibility,
                                                 0);

        configState1 = new ClusterConfigState(
                1L,
                null,
                Collections.singletonMap(connectorId1, 1),
                Collections.singletonMap(connectorId1, new HashMap<>()),
                Collections.singletonMap(connectorId1, TargetState.STARTED),
                Collections.singletonMap(taskId1x0, new HashMap<>()),
                Collections.emptySet()
        );

        Map<String, Integer> configState2ConnectorTaskCounts = new HashMap<>();
        configState2ConnectorTaskCounts.put(connectorId1, 2);
        configState2ConnectorTaskCounts.put(connectorId2, 1);
        Map<String, Map<String, String>> configState2ConnectorConfigs = new HashMap<>();
        configState2ConnectorConfigs.put(connectorId1, new HashMap<>());
        configState2ConnectorConfigs.put(connectorId2, new HashMap<>());
        Map<String, TargetState> configState2TargetStates = new HashMap<>();
        configState2TargetStates.put(connectorId1, TargetState.STARTED);
        configState2TargetStates.put(connectorId2, TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> configState2TaskConfigs = new HashMap<>();
        configState2TaskConfigs.put(taskId1x0, new HashMap<>());
        configState2TaskConfigs.put(taskId1x1, new HashMap<>());
        configState2TaskConfigs.put(taskId2x0, new HashMap<>());
        configState2 = new ClusterConfigState(
                2L,
                null,
                configState2ConnectorTaskCounts,
                configState2ConnectorConfigs,
                configState2TargetStates,
                configState2TaskConfigs,
                Collections.emptySet()
        );

        Map<String, Integer> configStateSingleTaskConnectorsConnectorTaskCounts = new HashMap<>();
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId1, 1);
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId2, 1);
        configStateSingleTaskConnectorsConnectorTaskCounts.put(connectorId3, 1);
        Map<String, Map<String, String>> configStateSingleTaskConnectorsConnectorConfigs = new HashMap<>();
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId1, new HashMap<>());
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId2, new HashMap<>());
        configStateSingleTaskConnectorsConnectorConfigs.put(connectorId3, new HashMap<>());
        Map<String, TargetState> configStateSingleTaskConnectorsTargetStates = new HashMap<>();
        configStateSingleTaskConnectorsTargetStates.put(connectorId1, TargetState.STARTED);
        configStateSingleTaskConnectorsTargetStates.put(connectorId2, TargetState.STARTED);
        configStateSingleTaskConnectorsTargetStates.put(connectorId3, TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> configStateSingleTaskConnectorsTaskConfigs = new HashMap<>();
        configStateSingleTaskConnectorsTaskConfigs.put(taskId1x0, new HashMap<>());
        configStateSingleTaskConnectorsTaskConfigs.put(taskId2x0, new HashMap<>());
        configStateSingleTaskConnectorsTaskConfigs.put(taskId3x0, new HashMap<>());
        configStateSingleTaskConnectors = new ClusterConfigState(
                2L,
                null,
                configStateSingleTaskConnectorsConnectorTaskCounts,
                configStateSingleTaskConnectorsConnectorConfigs,
                configStateSingleTaskConnectorsTargetStates,
                configStateSingleTaskConnectorsTaskConfigs,
                Collections.emptySet()
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

        JoinGroupRequestData.JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
        assertEquals(expectedMetadataSize, serialized.size());

        Iterator<JoinGroupRequestData.JoinGroupRequestProtocol> protocolIterator = serialized.iterator();
        assertTrue(protocolIterator.hasNext());
        JoinGroupRequestData.JoinGroupRequestProtocol defaultMetadata = protocolIterator.next();
        assertEquals(compatibility.protocol(), defaultMetadata.name());
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(
                ByteBuffer.wrap(defaultMetadata.metadata()));
        assertEquals(1, state.offset());

        PowerMock.verifyAll();
    }

    @Test
    public void testNormalJoinGroupLeader() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        final String consumerId = "leader";

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        Map<String, Long> memberConfigOffsets = new HashMap<>();
        memberConfigOffsets.put("leader", 1L);
        memberConfigOffsets.put("member", 1L);
        client.prepareResponse(joinGroupLeaderResponse(1, consumerId, memberConfigOffsets, Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(consumerId) &&
                    sync.data().generationId() == 1 &&
                    sync.groupAssignments().containsKey(consumerId);
        }, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.singletonList(connectorId1),
                Collections.emptyList(), Errors.NONE));
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

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // normal join group
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        client.prepareResponse(body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(memberId) &&
                    sync.data().generationId() == 1 &&
                    sync.data().assignments().isEmpty();
        }, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.emptyList(),
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

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // config mismatch results in assignment error
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        MockClient.RequestMatcher matcher = body -> {
            SyncGroupRequest sync = (SyncGroupRequest) body;
            return sync.data().memberId().equals(memberId) &&
                    sync.data().generationId() == 1 &&
                    sync.data().assignments().isEmpty();
        };
        client.prepareResponse(matcher, syncGroupResponse(ConnectProtocol.Assignment.CONFIG_MISMATCH, "leader", 10L,
                Collections.emptyList(), Collections.emptyList(), Errors.NONE));
        client.prepareResponse(joinGroupFollowerResponse(1, memberId, "leader", Errors.NONE));
        client.prepareResponse(matcher, syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L,
                Collections.emptyList(), Collections.singletonList(taskId1x0), Errors.NONE));
        coordinator.ensureActiveGroup();

        PowerMock.verifyAll();
    }

    @Test
    public void testRejoinGroup() {
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        client.prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, groupId, node));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // join the group once
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.emptyList(),
                Collections.singletonList(taskId1x0), Errors.NONE));
        coordinator.ensureActiveGroup();

        assertEquals(0, rebalanceListener.revokedCount);
        assertEquals(1, rebalanceListener.assignedCount);
        assertFalse(rebalanceListener.assignment.failed());
        assertEquals(1L, rebalanceListener.assignment.offset());
        assertEquals(Collections.emptyList(), rebalanceListener.assignment.connectors());
        assertEquals(Collections.singletonList(taskId1x0), rebalanceListener.assignment.tasks());

        // and join the group again
        coordinator.requestRejoin("test");
        client.prepareResponse(joinGroupFollowerResponse(1, "consumer", "leader", Errors.NONE));
        client.prepareResponse(syncGroupResponse(ConnectProtocol.Assignment.NO_ERROR, "leader", 1L, Collections.singletonList(connectorId1),
                Collections.emptyList(), Errors.NONE));
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
    public void testLeaderPerformAssignment1() {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        // Mark everyone as in sync with configState1
        List<JoinGroupResponseData.JoinGroupResponseMember> responseMembers = new ArrayList<>();
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("leader")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)).array())
        );
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("member")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)).array())
        );
        Map<String, ByteBuffer> result = coordinator.onLeaderElected("leader", EAGER.protocol(), responseMembers, false);

        // configState1 has 1 connector, 1 task
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Collections.singletonList(connectorId1), leaderAssignment.connectors());
        assertEquals(Collections.emptyList(), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertFalse(memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.emptyList(), memberAssignment.connectors());
        assertEquals(Collections.singletonList(taskId1x0), memberAssignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testLeaderPerformAssignment2() {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configState2);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        // Mark everyone as in sync with configState1
        List<JoinGroupResponseData.JoinGroupResponseMember> responseMembers = new ArrayList<>();
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("leader")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)).array())
        );
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("member")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)).array())
        );

        Map<String, ByteBuffer> result = coordinator.onLeaderElected("leader", EAGER.protocol(), responseMembers, false);

        // configState2 has 2 connector, 3 tasks and should trigger round robin assignment
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Collections.singletonList(connectorId1), leaderAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertFalse(memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Collections.singletonList(taskId1x1), memberAssignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testLeaderPerformAssignmentSingleTaskConnectors() {
        // Since all the protocol responses are mocked, the other tests validate doSync runs, but don't validate its
        // output. So we test it directly here.

        EasyMock.expect(configStorage.snapshot()).andReturn(configStateSingleTaskConnectors);

        PowerMock.replayAll();

        // Prime the current configuration state
        coordinator.metadata();

        // Mark everyone as in sync with configState1
        List<JoinGroupResponseData.JoinGroupResponseMember> responseMembers = new ArrayList<>();
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("leader")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(LEADER_URL, 1L)).array())
        );
        responseMembers.add(new JoinGroupResponseData.JoinGroupResponseMember()
                .setMemberId("member")
                .setMetadata(ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(MEMBER_URL, 1L)).array())
        );

        Map<String, ByteBuffer> result = coordinator.onLeaderElected("leader", EAGER.protocol(), responseMembers, false);

        // Round robin assignment when there are the same number of connectors and tasks should result in each being
        // evenly distributed across the workers, i.e. round robin assignment of connectors first, then followed by tasks
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(result.get("leader"));
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Arrays.asList(taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(result.get("member"));
        assertFalse(memberAssignment.failed());
        assertEquals("leader", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());

        PowerMock.verifyAll();
    }

    @Test
    public void testSkippingAssignmentFails() {
        // Connect does not support static membership so skipping assignment should
        // never be set to true by the group coordinator. It is treated as an
        // illegal state if it would.
        EasyMock.expect(configStorage.snapshot()).andReturn(configState1);
        PowerMock.replayAll();

        coordinator.metadata();

        assertThrows(IllegalStateException.class,
            () -> coordinator.onLeaderElected("leader", EAGER.protocol(), Collections.emptyList(), true));

        PowerMock.verifyAll();
    }

    private JoinGroupResponse joinGroupLeaderResponse(int generationId, String memberId,
                                                      Map<String, Long> configOffsets, Errors error) {
        List<JoinGroupResponseData.JoinGroupResponseMember> metadata = new ArrayList<>();
        for (Map.Entry<String, Long> configStateEntry : configOffsets.entrySet()) {
            // We need a member URL, but it doesn't matter for the purposes of this test. Just set it to the member ID
            String memberUrl = configStateEntry.getKey();
            long configOffset = configStateEntry.getValue();
            ByteBuffer buf = ConnectProtocol.serializeMetadata(new ConnectProtocol.WorkerState(memberUrl, configOffset));
            metadata.add(new JoinGroupResponseData.JoinGroupResponseMember()
                    .setMemberId(configStateEntry.getKey())
                    .setMetadata(buf.array())
            );
        }
        return new JoinGroupResponse(
                new JoinGroupResponseData().setErrorCode(error.code())
                .setGenerationId(generationId)
                .setProtocolName(EAGER.protocol())
                .setLeader(memberId)
                .setMemberId(memberId)
                .setMembers(metadata)
        );
    }

    private JoinGroupResponse joinGroupFollowerResponse(int generationId, String memberId, String leaderId, Errors error) {
        return new JoinGroupResponse(
                new JoinGroupResponseData().setErrorCode(error.code())
                        .setGenerationId(generationId)
                        .setProtocolName(EAGER.protocol())
                        .setLeader(leaderId)
                        .setMemberId(memberId)
                        .setMembers(Collections.emptyList())
        );
    }

    private SyncGroupResponse syncGroupResponse(short assignmentError, String leader, long configOffset, List<String> connectorIds,
                                     List<ConnectorTaskId> taskIds, Errors error) {
        ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(assignmentError, leader, LEADER_URL, configOffset, connectorIds, taskIds);
        ByteBuffer buf = ConnectProtocol.serializeAssignment(assignment);
        return new SyncGroupResponse(
                new SyncGroupResponseData()
                        .setErrorCode(error.code())
                        .setAssignment(Utils.toArray(buf))
        );
    }

    private static class MockRebalanceListener implements WorkerRebalanceListener {
        public ExtendedAssignment assignment = null;

        public String revokedLeader;
        public Collection<String> revokedConnectors;
        public Collection<ConnectorTaskId> revokedTasks;

        public int revokedCount = 0;
        public int assignedCount = 0;

        @Override
        public void onAssigned(ExtendedAssignment assignment, int generation) {
            this.assignment = assignment;
            assignedCount++;
        }

        @Override
        public void onRevoked(String leader, Collection<String> connectors, Collection<ConnectorTaskId> tasks) {
            if (connectors.isEmpty() && tasks.isEmpty()) {
                return;
            }
            this.revokedLeader = leader;
            this.revokedConnectors = connectors;
            this.revokedTasks = tasks;
            revokedCount++;
        }
    }
}
