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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.WorkerTestUtils.assertAssignment;
import static org.apache.kafka.connect.runtime.WorkerTestUtils.clusterConfigState;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.WorkerState;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class WorkerCoordinatorIncrementalTest {

    private final String connectorId1 = "connector1";
    private final String connectorId2 = "connector2";
    private final ConnectorTaskId taskId1x0 = new ConnectorTaskId(connectorId1, 0);
    private final ConnectorTaskId taskId2x0 = new ConnectorTaskId(connectorId2, 0);

    private final String groupId = "test-group";
    private final int sessionTimeoutMs = 10;
    private final int rebalanceTimeoutMs = 60;
    private final int heartbeatIntervalMs = 2;
    private final long retryBackoffMs = 100;
    private final long retryBackoffMaxMs = 1000;
    private final int requestTimeoutMs = 1000;
    private MockTime time;
    private MockClient client;
    private Node node;
    private Metadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    @Mock
    private KafkaConfigBackingStore configStorage;
    private GroupRebalanceConfig rebalanceConfig;
    private WorkerCoordinator coordinator;
    private int rebalanceDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;

    private String leaderId;
    private String memberId;
    private String anotherMemberId;
    private String leaderUrl;
    private String memberUrl;
    private String anotherMemberUrl;
    private int generationId;
    private long offset;

    private int configStorageCalls;

    private ClusterConfigState configState1;

    // Arguments are:
    // - Protocol type
    // - Expected metadata size
    static Stream<Arguments> mode() {
        return Stream.of(
            Arguments.of(ConnectProtocolCompatibility.COMPATIBLE, 2),
            Arguments.of(ConnectProtocolCompatibility.SESSIONED, 3)
        );
    }

    public void init(ConnectProtocolCompatibility compatibility) {
        LogContext loggerFactory = new LogContext();

        this.time = new MockTime();
        this.metadata = new Metadata(0, 0, Long.MAX_VALUE, loggerFactory, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, Map.of("topic", 1)));
        this.node = metadata.fetch().nodes().get(0);
        this.consumerClient = new ConsumerNetworkClient(loggerFactory, client, metadata, time,
            retryBackoffMs, requestTimeoutMs, heartbeatIntervalMs);
        this.metrics = new Metrics(time);
        this.rebalanceListener = new MockRebalanceListener();

        this.leaderId = "worker1";
        this.memberId = "worker2";
        this.anotherMemberId = "worker3";
        this.leaderUrl = expectedUrl(leaderId);
        this.memberUrl = expectedUrl(memberId);
        this.anotherMemberUrl = expectedUrl(anotherMemberId);
        this.generationId = 3;
        this.offset = 10L;

        this.configStorageCalls = 0;

        this.rebalanceConfig = new GroupRebalanceConfig(sessionTimeoutMs,
            rebalanceTimeoutMs,
            heartbeatIntervalMs,
            groupId,
            Optional.empty(),
            null,
            retryBackoffMs,
            retryBackoffMaxMs);
        this.coordinator = new WorkerCoordinator(rebalanceConfig,
            loggerFactory,
            consumerClient,
            metrics,
            "worker" + groupId,
            time,
            expectedUrl(leaderId),
            configStorage,
            rebalanceListener,
            compatibility,
            rebalanceDelay);

        configState1 = clusterConfigState(offset, 2, 4);
    }

    @AfterEach
    public void teardown() {
        this.metrics.close();
        verifyNoMoreInteractions(configStorage);
    }

    private static String expectedUrl(String member) {
        return "http://" + member + ":8083";
    }

    // We only test functionality unique to WorkerCoordinator. Other functionality is already
    // well tested via the tests that cover AbstractCoordinator & ConsumerCoordinator.

    @ParameterizedTest
    @MethodSource("mode")
    public void testMetadata(ConnectProtocolCompatibility compatibility, int expectedMetadataSize) {
        init(compatibility);
        System.err.println(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
        assertEquals(expectedMetadataSize, serialized.size());

        Iterator<JoinGroupRequestProtocol> protocolIterator = serialized.iterator();
        assertTrue(protocolIterator.hasNext());
        JoinGroupRequestProtocol defaultMetadata = protocolIterator.next();
        assertEquals(compatibility.protocol(), defaultMetadata.name());
        WorkerState state = IncrementalCooperativeConnectProtocol
                .deserializeMetadata(ByteBuffer.wrap(defaultMetadata.metadata()));
        assertEquals(offset, state.offset());

        verify(configStorage, times(1)).snapshot();
    }

    @ParameterizedTest
    @MethodSource("mode")
    public void testMetadataWithExistingAssignment(ConnectProtocolCompatibility compatibility, int expectedMetadataSize) {
        init(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        ExtendedAssignment assignment = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ExtendedAssignment.NO_ERROR, leaderId, leaderUrl, configState1.offset(),
                List.of(connectorId1), List.of(taskId1x0, taskId2x0),
                List.of(), List.of(), 0);
        ByteBuffer buf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment, false);
        // Using onJoinComplete to register the protocol selection decided by the broker
        // coordinator as well as an existing previous assignment that the call to metadata will
        // include with v1 but not with v0
        coordinator.onJoinComplete(generationId, memberId, compatibility.protocol(), buf);

        JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
        assertEquals(expectedMetadataSize, serialized.size());

        Iterator<JoinGroupRequestProtocol> protocolIterator = serialized.iterator();
        assertTrue(protocolIterator.hasNext());
        JoinGroupRequestProtocol selectedMetadata = protocolIterator.next();
        assertEquals(compatibility.protocol(), selectedMetadata.name());
        ExtendedWorkerState state = IncrementalCooperativeConnectProtocol
                .deserializeMetadata(ByteBuffer.wrap(selectedMetadata.metadata()));
        assertEquals(offset, state.offset());
        assertNotEquals(ExtendedAssignment.empty(), state.assignment());
        assertEquals(List.of(connectorId1), state.assignment().connectors());
        assertEquals(List.of(taskId1x0, taskId2x0), state.assignment().tasks());

        verify(configStorage, times(1)).snapshot();
    }

    @ParameterizedTest
    @MethodSource("mode")
    public void testMetadataWithExistingAssignmentButOlderProtocolSelection(ConnectProtocolCompatibility compatibility, int expectedMetadataSize) {
        init(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        ExtendedAssignment assignment = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ExtendedAssignment.NO_ERROR, leaderId, leaderUrl, configState1.offset(),
                List.of(connectorId1), List.of(taskId1x0, taskId2x0),
                List.of(), List.of(), 0);
        ByteBuffer buf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment, false);
        // Using onJoinComplete to register the protocol selection decided by the broker
        // coordinator as well as an existing previous assignment that the call to metadata will
        // include with v1 but not with v0
        coordinator.onJoinComplete(generationId, memberId, EAGER.protocol(), buf);

        JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
        assertEquals(expectedMetadataSize, serialized.size());

        Iterator<JoinGroupRequestProtocol> protocolIterator = serialized.iterator();
        assertTrue(protocolIterator.hasNext());
        JoinGroupRequestProtocol selectedMetadata = protocolIterator.next();
        assertEquals(compatibility.protocol(), selectedMetadata.name());
        ExtendedWorkerState state = IncrementalCooperativeConnectProtocol
                .deserializeMetadata(ByteBuffer.wrap(selectedMetadata.metadata()));
        assertEquals(offset, state.offset());
        assertNotEquals(ExtendedAssignment.empty(), state.assignment());

        verify(configStorage, times(1)).snapshot();
    }

    @ParameterizedTest
    @MethodSource("mode")
    public void testTaskAssignmentWhenWorkerJoins(ConnectProtocolCompatibility compatibility) {
        init(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null, compatibility);

        Map<String, ByteBuffer> result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(connectorId1), 4,
                List.of(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(connectorId2), 4,
                List.of(), 0,
                memberAssignment);

        coordinator.metadata();
        ++configStorageCalls;

        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment, compatibility);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null, compatibility);

        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        //Equally distributing tasks across member
        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
            List.of(), 0,
            List.of(), 1,
            leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
            List.of(), 0,
            List.of(), 1,
            memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
            List.of(), 0,
            List.of(), 0,
            anotherMemberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    @ParameterizedTest
    @MethodSource("mode")
    public void testTaskAssignmentWhenWorkerLeavesPermanently(ConnectProtocolCompatibility compatibility) {
        init(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        // First assignment distributes configured connectors and tasks
        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null, compatibility);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null, compatibility);

        Map<String, ByteBuffer> result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(connectorId1), 3,
                List.of(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(connectorId2), 3,
                List.of(), 0,
                memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                List.of(), 2,
                List.of(), 0,
                anotherMemberAssignment);

        // Second rebalance detects a worker is missing
        coordinator.metadata();
        ++configStorageCalls;

        // Mark everyone as in sync with configState1
        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment, compatibility);

        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                memberAssignment);

        rebalanceDelay /= 2;
        time.sleep(rebalanceDelay);

        // A third rebalance before the delay expires won't change the assignments
        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                memberAssignment);

        time.sleep(rebalanceDelay + 1);

        // A rebalance after the delay expires re-assigns the lost tasks
        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 1,
                List.of(), 0,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 1,
                List.of(), 0,
                memberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    @ParameterizedTest
    @MethodSource("mode")
    public void testTaskAssignmentWhenWorkerBounces(ConnectProtocolCompatibility compatibility) {
        init(compatibility);
        when(configStorage.snapshot()).thenReturn(configState1);

        // First assignment distributes configured connectors and tasks
        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null, compatibility);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null, compatibility);

        Map<String, ByteBuffer> result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(connectorId1), 3,
                List.of(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(connectorId2), 3,
                List.of(), 0,
                memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                List.of(), 2,
                List.of(), 0,
                anotherMemberAssignment);

        // Second rebalance detects a worker is missing
        coordinator.metadata();
        ++configStorageCalls;

        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment, compatibility);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment, compatibility);

        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                memberAssignment);

        rebalanceDelay /= 2;
        time.sleep(rebalanceDelay);

        // A third rebalance before the delay expires won't change the assignments even if the
        // member returns in the meantime
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null, compatibility);
        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                memberAssignment);

        anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                rebalanceDelay,
                anotherMemberAssignment);

        time.sleep(rebalanceDelay + 1);

        result = coordinator.onLeaderElected(leaderId, compatibility.protocol(), responseMembers, false);

        // A rebalance after the delay expires re-assigns the lost tasks to the returning member
        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                List.of(), 0,
                List.of(), 0,
                memberAssignment);

        anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                List.of(), 2,
                List.of(), 0,
                anotherMemberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    private static class MockRebalanceListener implements WorkerRebalanceListener {
        public ExtendedAssignment assignment = null;

        public String revokedLeader;
        public Collection<String> revokedConnectors = List.of();
        public Collection<ConnectorTaskId> revokedTasks = List.of();

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

        @Override
        public void onPollTimeoutExpiry() {}
    }

    private static ExtendedAssignment deserializeAssignment(Map<String, ByteBuffer> assignment,
                                                           String member) {
        return IncrementalCooperativeConnectProtocol.deserializeAssignment(assignment.get(member));
    }


    private void addJoinGroupResponseMember(List<JoinGroupResponseMember> responseMembers,
                                            String member,
                                            long offset,
                                            ExtendedAssignment assignment,
                                            ConnectProtocolCompatibility compatibility) {
        responseMembers.add(new JoinGroupResponseMember()
                .setMemberId(member)
                .setMetadata(
                    IncrementalCooperativeConnectProtocol.serializeMetadata(
                        new ExtendedWorkerState(expectedUrl(member), offset, assignment),
                        compatibility != COMPATIBLE
                    ).array()
                )
        );
    }
}
