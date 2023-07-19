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

import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ConnectProtocolCompatibilityTest {
    private static final String LEADER = "leader";
    private static final String LEADER_URL = "leaderUrl:8083";
    private static final long CONFIG_OFFSET = 1;

    private String connectorId1 = "connector1";
    private String connectorId2 = "connector2";
    private String connectorId3 = "connector3";
    private ConnectorTaskId taskId1x0 = new ConnectorTaskId(connectorId1, 0);
    private ConnectorTaskId taskId1x1 = new ConnectorTaskId(connectorId1, 1);
    private ConnectorTaskId taskId2x0 = new ConnectorTaskId(connectorId2, 0);
    private ConnectorTaskId taskId3x0 = new ConnectorTaskId(connectorId3, 0);

    @Test
    public void testEagerToEagerMetadata() {
        ConnectProtocol.WorkerState workerState = emptyWorkerState();
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testCoopToCoopMetadata() {
        ExtendedWorkerState workerState = emptyExtendedWorkerState(CONNECT_PROTOCOL_V1);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, false);
        ExtendedWorkerState state = IncrementalCooperativeConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testSessionedToCoopMetadata() {
        ExtendedWorkerState workerState = emptyExtendedWorkerState(CONNECT_PROTOCOL_V2);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, true);
        ExtendedWorkerState state = IncrementalCooperativeConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testSessionedToEagerMetadata() {
        ExtendedWorkerState workerState = emptyExtendedWorkerState(CONNECT_PROTOCOL_V2);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, true);
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testCoopToEagerMetadata() {
        ExtendedWorkerState workerState = emptyExtendedWorkerState(CONNECT_PROTOCOL_V1);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState, false);
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testEagerToCoopMetadata() {
        ConnectProtocol.WorkerState workerState = emptyWorkerState();
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        ConnectProtocol.WorkerState state = IncrementalCooperativeConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
    }

    @Test
    public void testEagerToEagerAssignment() {
        ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "leader", LEADER_URL, 1L,
                Arrays.asList(connectorId1, connectorId3), Arrays.asList(taskId2x0));

        ByteBuffer leaderBuf = ConnectProtocol.serializeAssignment(assignment);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment assignment2 = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0));

        ByteBuffer memberBuf = ConnectProtocol.serializeAssignment(assignment2);
        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(memberBuf);
        assertFalse(memberAssignment.failed());
        assertEquals("member", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());
    }

    @Test
    public void testCoopToCoopAssignment() {
        ExtendedAssignment assignment = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, "leader", LEADER_URL, 1L,
                Arrays.asList(connectorId1, connectorId3), Arrays.asList(taskId2x0),
                Collections.emptyList(), Collections.emptyList(), 0);

        ByteBuffer leaderBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment, false);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ExtendedAssignment assignment2 = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0),
                Collections.emptyList(), Collections.emptyList(), 0);

        ByteBuffer memberBuf = ConnectProtocol.serializeAssignment(assignment2);
        ConnectProtocol.Assignment memberAssignment =
                IncrementalCooperativeConnectProtocol.deserializeAssignment(memberBuf);
        assertFalse(memberAssignment.failed());
        assertEquals("member", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());
    }

    @Test
    public void testEagerToCoopAssignment() {
        ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "leader", LEADER_URL, 1L,
                Arrays.asList(connectorId1, connectorId3), Arrays.asList(taskId2x0));

        ByteBuffer leaderBuf = ConnectProtocol.serializeAssignment(assignment);
        ConnectProtocol.Assignment leaderAssignment =
                IncrementalCooperativeConnectProtocol.deserializeAssignment(leaderBuf);
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment assignment2 = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0));

        ByteBuffer memberBuf = ConnectProtocol.serializeAssignment(assignment2);
        ConnectProtocol.Assignment memberAssignment =
                IncrementalCooperativeConnectProtocol.deserializeAssignment(memberBuf);
        assertFalse(memberAssignment.failed());
        assertEquals("member", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());
    }

    @Test
    public void testCoopToEagerAssignment() {
        ExtendedAssignment assignment = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, "leader", LEADER_URL, 1L,
                Arrays.asList(connectorId1, connectorId3), Arrays.asList(taskId2x0),
                Collections.emptyList(), Collections.emptyList(), 0);

        ByteBuffer leaderBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment, false);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertFalse(leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ExtendedAssignment assignment2 = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0),
                Collections.emptyList(), Collections.emptyList(), 0);

        ByteBuffer memberBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment2, false);
        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(memberBuf);
        assertFalse(memberAssignment.failed());
        assertEquals("member", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());
    }

    private ConnectProtocol.WorkerState emptyWorkerState() {
        return new ConnectProtocol.WorkerState(LEADER_URL, CONFIG_OFFSET);
    }

    private ExtendedWorkerState emptyExtendedWorkerState(short protocolVersion) {
        ExtendedAssignment assignment = new ExtendedAssignment(
                protocolVersion,
                ConnectProtocol.Assignment.NO_ERROR,
                LEADER,
                LEADER_URL,
                CONFIG_OFFSET,
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                0
        );
        return new ExtendedWorkerState(LEADER_URL, CONFIG_OFFSET, assignment);
    }

}
