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
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import static org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import static org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import static org.apache.kafka.connect.runtime.WorkerTestUtils.assertAssignment;
import static org.apache.kafka.connect.runtime.WorkerTestUtils.clusterConfigState;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.COMPATIBLE;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.EAGER;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility.SESSIONED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class WorkerCoordinatorIncrementalTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    private final String connectorId1 = "connector1";
    private final String connectorId2 = "connector2";
    private final ConnectorTaskId taskId1x0 = new ConnectorTaskId(connectorId1, 0);
    private final ConnectorTaskId taskId2x0 = new ConnectorTaskId(connectorId2, 0);
    private final MockTime time = new MockTime();
    private final Metrics metrics = new Metrics(time);
    @Mock
    private KafkaConfigBackingStore configStorage;
    private WorkerCoordinator coordinator;
    private int rebalanceDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;
    private final String leaderId = "worker1";
    private final String memberId = "worker2";
    private final String anotherMemberId = "worker3";
    private final String leaderUrl = expectedUrl(leaderId);
    private final int generationId = 3;
    private final long offset = 10L;
    private int configStorageCalls = 0;

    private ClusterConfigState configState;

    // Arguments are:
    // - Protocol type
    // - Expected metadata size
    @Parameters
    public static Iterable<?> mode() {
        return Arrays.asList(new Object[][]{{COMPATIBLE, 2}, {SESSIONED, 3}});
    }

    @Parameter
    public ConnectProtocolCompatibility compatibility;

    @Parameter(1)
    public int expectedMetadataSize;

    @Before
    public void setup() {
        LogContext loggerFactory = new LogContext();
        Metadata metadata = new Metadata(0, Long.MAX_VALUE, loggerFactory, new ClusterResourceListeners());
        MockClient client = new MockClient(time, metadata);
        client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)));
        int heartbeatIntervalMs = 2;
        long retryBackoffMs = 100;
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(loggerFactory, client, metadata, time,
                retryBackoffMs, 1000, heartbeatIntervalMs);
        MockRebalanceListener rebalanceListener = new MockRebalanceListener();

        int rebalanceTimeoutMs = 60;
        int sessionTimeoutMs = 10;
        String groupId = "test-group";
        GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                Optional.empty(),
                retryBackoffMs,
                true);
        this.coordinator = new WorkerCoordinator(
                rebalanceConfig,
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

        configState = clusterConfigState(offset, 2, 4);
    }

    @After
    public void teardown() {
        this.metrics.close();
        verifyNoMoreInteractions(configStorage);
    }

    private static String expectedUrl(String member) {
        return "http://" + member + ":8083";
    }

    // We only test functionality unique to WorkerCoordinator. Other functionality is already
    // well tested via the tests that cover AbstractCoordinator & ConsumerCoordinator.

    @Test
    public void testMetadata() {
        when(configStorage.snapshot()).thenReturn(configState);

        JoinGroupRequestProtocolCollection serialized = coordinator.metadata();
        assertEquals(expectedMetadataSize, serialized.size());

        Iterator<JoinGroupRequestProtocol> protocolIterator = serialized.iterator();
        assertTrue(protocolIterator.hasNext());
        JoinGroupRequestProtocol defaultMetadata = protocolIterator.next();
        assertEquals(compatibility.protocol(), defaultMetadata.name());
        WorkerState state = WorkerState.of(ByteBuffer.wrap(defaultMetadata.metadata()));
        assertEquals(offset, state.offset());

        verify(configStorage, times(1)).snapshot();
    }

    @Test
    public void testMetadataWithExistingAssignment() {
        when(configStorage.snapshot()).thenReturn(configState);

        ExtendedAssignment assignment = new ExtendedAssignment(
                ConnectProtocolCompatibility.COMPATIBLE.protocolVersion(), ExtendedAssignment.NO_ERROR, leaderId, leaderUrl, configState.offset(),
                Collections.singletonList(connectorId1), Arrays.asList(taskId1x0, taskId2x0),
                Collections.emptyList(), Collections.emptyList(), 0);
        ByteBuffer buf = ExtendedAssignment.toByteBuffer(assignment);
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
        ExtendedWorkerState state = ExtendedWorkerState.of(ByteBuffer.wrap(selectedMetadata.metadata()));
        assertEquals(offset, state.offset());
        assertNotEquals(ExtendedAssignment.empty(), state.assignment());
        assertEquals(Collections.singletonList(connectorId1), state.assignment().connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId2x0), state.assignment().tasks());

        verify(configStorage, times(1)).snapshot();
    }

    @Test
    public void testMetadataWithExistingAssignmentButOlderProtocolSelection() {
        when(configStorage.snapshot()).thenReturn(configState);

        ExtendedAssignment assignment = new ExtendedAssignment(
                ConnectProtocolCompatibility.COMPATIBLE.protocolVersion(), ExtendedAssignment.NO_ERROR, leaderId, leaderUrl, configState.offset(),
                Collections.singletonList(connectorId1), Arrays.asList(taskId1x0, taskId2x0),
                Collections.emptyList(), Collections.emptyList(), 0);
        ByteBuffer buf = ExtendedAssignment.toByteBuffer(assignment);
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
        ExtendedWorkerState state = ExtendedWorkerState.of(ByteBuffer.wrap(selectedMetadata.metadata()));
        assertEquals(offset, state.offset());
        assertNotEquals(ExtendedAssignment.empty(), state.assignment());

        verify(configStorage, times(1)).snapshot();
    }

    @Test
    public void testTaskAssignmentWhenWorkerJoins() {
        when(configStorage.snapshot()).thenReturn(configState);

        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null);

        Map<String, ByteBuffer> result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId1), 4,
                Collections.emptyList(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId2), 4,
                Collections.emptyList(), 0,
                memberAssignment);

        coordinator.metadata();
        ++configStorageCalls;

        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null);

        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 2,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                anotherMemberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    @Test
    public void testTaskAssignmentWhenWorkerLeavesPermanently() {
        when(configStorage.snapshot()).thenReturn(configState);

        // First assignment distributes configured connectors and tasks
        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null);

        Map<String, ByteBuffer> result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId1), 3,
                Collections.emptyList(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId2), 3,
                Collections.emptyList(), 0,
                memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 2,
                Collections.emptyList(), 0,
                anotherMemberAssignment);

        // Second rebalance detects a worker is missing
        coordinator.metadata();
        ++configStorageCalls;

        // Mark everyone as in sync with configState
        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment);

        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                memberAssignment);

        rebalanceDelay /= 2;
        time.sleep(rebalanceDelay);

        // A third rebalance before the delay expires won't change the assignments
        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                memberAssignment);

        time.sleep(rebalanceDelay + 1);

        // A rebalance after the delay expires re-assigns the lost tasks
        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 1,
                Collections.emptyList(), 0,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 1,
                Collections.emptyList(), 0,
                memberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    @Test
    public void testTaskAssignmentWhenWorkerBounces() {
        when(configStorage.snapshot()).thenReturn(configState);

        // First assignment distributes configured connectors and tasks
        coordinator.metadata();
        ++configStorageCalls;

        List<JoinGroupResponseMember> responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, null);
        addJoinGroupResponseMember(responseMembers, memberId, offset, null);
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null);

        Map<String, ByteBuffer> result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        ExtendedAssignment leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId1), 3,
                Collections.emptyList(), 0,
                leaderAssignment);

        ExtendedAssignment memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.singletonList(connectorId2), 3,
                Collections.emptyList(), 0,
                memberAssignment);

        ExtendedAssignment anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 2,
                Collections.emptyList(), 0,
                anotherMemberAssignment);

        // Second rebalance detects a worker is missing
        coordinator.metadata();
        ++configStorageCalls;

        responseMembers = new ArrayList<>();
        addJoinGroupResponseMember(responseMembers, leaderId, offset, leaderAssignment);
        addJoinGroupResponseMember(responseMembers, memberId, offset, memberAssignment);

        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                memberAssignment);

        rebalanceDelay /= 2;
        time.sleep(rebalanceDelay);

        // A third rebalance before the delay expires won't change the assignments even if the
        // member returns in the meantime
        addJoinGroupResponseMember(responseMembers, anotherMemberId, offset, null);
        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                memberAssignment);

        anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                rebalanceDelay,
                anotherMemberAssignment);

        time.sleep(rebalanceDelay + 1);

        result = coordinator.performAssignment(leaderId, compatibility.protocol(), responseMembers);

        // A rebalance after the delay expires re-assigns the lost tasks to the returning member
        leaderAssignment = deserializeAssignment(result, leaderId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                leaderAssignment);

        memberAssignment = deserializeAssignment(result, memberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 0,
                Collections.emptyList(), 0,
                memberAssignment);

        anotherMemberAssignment = deserializeAssignment(result, anotherMemberId);
        assertAssignment(leaderId, offset,
                Collections.emptyList(), 2,
                Collections.emptyList(), 0,
                anotherMemberAssignment);

        verify(configStorage, times(configStorageCalls)).snapshot();
    }

    private static class MockRebalanceListener implements WorkerRebalanceListener {
        public ExtendedAssignment assignment = null;

        public String revokedLeader;
        public Collection<String> revokedConnectors = Collections.emptyList();
        public Collection<ConnectorTaskId> revokedTasks = Collections.emptyList();

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

    private static ExtendedAssignment deserializeAssignment(Map<String, ByteBuffer> assignment,
                                                           String member) {
        return ExtendedAssignment.of(assignment.get(member));
    }

    private void addJoinGroupResponseMember(List<JoinGroupResponseMember> responseMembers,
                                                   String member,
                                                   long offset,
                                                   ExtendedAssignment assignment) {
        responseMembers.add(new JoinGroupResponseMember()
                .setMemberId(member)
                .setMetadata(
                        ExtendedWorkerState.toByteBuffer(
                        new ExtendedWorkerState(expectedUrl(member), offset, assignment),
                        compatibility != COMPATIBLE
                    ).array()
                )
        );
    }
}
