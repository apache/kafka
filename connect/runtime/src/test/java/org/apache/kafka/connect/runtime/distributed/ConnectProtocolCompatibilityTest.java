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

import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConnectProtocolCompatibilityTest {
    private static final String LEADER_URL = "leaderUrl:8083";

    private String connectorId1 = "connector1";
    private String connectorId2 = "connector2";
    private String connectorId3 = "connector3";
    private ConnectorTaskId taskId1x0 = new ConnectorTaskId(connectorId1, 0);
    private ConnectorTaskId taskId1x1 = new ConnectorTaskId(connectorId1, 1);
    private ConnectorTaskId taskId2x0 = new ConnectorTaskId(connectorId2, 0);
    private ConnectorTaskId taskId3x0 = new ConnectorTaskId(connectorId3, 0);

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private KafkaConfigBackingStore configStorage;
    private ClusterConfigState configState;

    @Before
    public void setup() {
        configStorage = mock(KafkaConfigBackingStore.class);
        configState = new ClusterConfigState(
                1L,
                Collections.singletonMap(connectorId1, 1),
                Collections.singletonMap(connectorId1, new HashMap<>()),
                Collections.singletonMap(connectorId1, TargetState.STARTED),
                Collections.singletonMap(taskId1x0, new HashMap<>()),
                Collections.emptySet());
    }

    @After
    public void teardown() {
        verifyNoMoreInteractions(configStorage);
    }

    @Test
    public void testEagerToEagerMetadata() {
        when(configStorage.snapshot()).thenReturn(configState);
        ExtendedWorkerState workerState = new ExtendedWorkerState(LEADER_URL, configStorage.snapshot().offset(), null);
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
        verify(configStorage).snapshot();
    }

    @Test
    public void testCoopToCoopMetadata() {
        when(configStorage.snapshot()).thenReturn(configState);
        ExtendedWorkerState workerState = new ExtendedWorkerState(LEADER_URL, configStorage.snapshot().offset(), null);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState);
        ExtendedWorkerState state = IncrementalCooperativeConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
        verify(configStorage).snapshot();
    }

    @Test
    public void testCoopToEagerMetadata() {
        when(configStorage.snapshot()).thenReturn(configState);
        ExtendedWorkerState workerState = new ExtendedWorkerState(LEADER_URL, configStorage.snapshot().offset(), null);
        ByteBuffer metadata = IncrementalCooperativeConnectProtocol.serializeMetadata(workerState);
        ConnectProtocol.WorkerState state = ConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
        verify(configStorage).snapshot();
    }

    @Test
    public void testEagerToCoopMetadata() {
        when(configStorage.snapshot()).thenReturn(configState);
        ConnectProtocol.WorkerState workerState = new ConnectProtocol.WorkerState(LEADER_URL, configStorage.snapshot().offset());
        ByteBuffer metadata = ConnectProtocol.serializeMetadata(workerState);
        ConnectProtocol.WorkerState state = IncrementalCooperativeConnectProtocol.deserializeMetadata(metadata);
        assertEquals(LEADER_URL, state.url());
        assertEquals(1, state.offset());
        verify(configStorage).snapshot();
    }

    @Test
    public void testEagerToEagerAssignment() {
        ConnectProtocol.Assignment assignment = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "leader", LEADER_URL, 1L,
                Arrays.asList(connectorId1, connectorId3), Arrays.asList(taskId2x0));

        ByteBuffer leaderBuf = ConnectProtocol.serializeAssignment(assignment);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertEquals(false, leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ConnectProtocol.Assignment assignment2 = new ConnectProtocol.Assignment(
                ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0));

        ByteBuffer memberBuf = ConnectProtocol.serializeAssignment(assignment2);
        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(memberBuf);
        assertEquals(false, memberAssignment.failed());
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

        ByteBuffer leaderBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertEquals(false, leaderAssignment.failed());
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
        assertEquals(false, memberAssignment.failed());
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
        assertEquals(false, leaderAssignment.failed());
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
        assertEquals(false, memberAssignment.failed());
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

        ByteBuffer leaderBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment);
        ConnectProtocol.Assignment leaderAssignment = ConnectProtocol.deserializeAssignment(leaderBuf);
        assertEquals(false, leaderAssignment.failed());
        assertEquals("leader", leaderAssignment.leader());
        assertEquals(1, leaderAssignment.offset());
        assertEquals(Arrays.asList(connectorId1, connectorId3), leaderAssignment.connectors());
        assertEquals(Collections.singletonList(taskId2x0), leaderAssignment.tasks());

        ExtendedAssignment assignment2 = new ExtendedAssignment(
                CONNECT_PROTOCOL_V1, ConnectProtocol.Assignment.NO_ERROR, "member", LEADER_URL, 1L,
                Arrays.asList(connectorId2), Arrays.asList(taskId1x0, taskId3x0),
                Collections.emptyList(), Collections.emptyList(), 0);

        ByteBuffer memberBuf = IncrementalCooperativeConnectProtocol.serializeAssignment(assignment2);
        ConnectProtocol.Assignment memberAssignment = ConnectProtocol.deserializeAssignment(memberBuf);
        assertEquals(false, memberAssignment.failed());
        assertEquals("member", memberAssignment.leader());
        assertEquals(1, memberAssignment.offset());
        assertEquals(Collections.singletonList(connectorId2), memberAssignment.connectors());
        assertEquals(Arrays.asList(taskId1x0, taskId3x0), memberAssignment.tasks());
    }

}
