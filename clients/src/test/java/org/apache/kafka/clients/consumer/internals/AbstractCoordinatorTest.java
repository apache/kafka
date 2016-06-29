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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.SpecificMetrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AbstractCoordinatorTest {

    private static final ByteBuffer EMPTY_DATA = ByteBuffer.wrap(new byte[0]);
    private static final int SESSION_TIMEOUT_MS = 30000;
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
        SpecificMetrics metrics = new SpecificMetrics();

        Cluster cluster = TestUtils.singletonCluster("topic", 1);
        metadata.update(cluster, mockTime.milliseconds());
        this.node = cluster.nodes().get(0);
        mockClient.setNode(node);

        this.coordinatorNode = new Node(Integer.MAX_VALUE - node.id(), node.host(), node.port());
        this.coordinator = new DummyCoordinator(consumerClient, metrics, mockTime);
    }

    @Test
    public void testCoordinatorDiscoveryBackoff() {
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));
        mockClient.prepareResponse(groupCoordinatorResponse(node, Errors.NONE.code()));

        // blackout the coordinator for 50 milliseconds to simulate a disconnect.
        // after backing off, we should be able to connect.
        mockClient.blackout(coordinatorNode, 50L);

        long initialTime = mockTime.milliseconds();
        coordinator.ensureCoordinatorReady();
        long endTime = mockTime.milliseconds();

        assertTrue(endTime - initialTime >= RETRY_BACKOFF_MS);
    }

    private Struct groupCoordinatorResponse(Node node, short error) {
        GroupCoordinatorResponse response = new GroupCoordinatorResponse(error, node);
        return response.toStruct();
    }

    public class DummyCoordinator extends AbstractCoordinator {

        public DummyCoordinator(ConsumerNetworkClient client,
                                SpecificMetrics metrics,
                                Time time) {
            super(client, GROUP_ID, SESSION_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, metrics,
                    new AbstractCoordinatorMetrics(METRIC_GROUP_PREFIX), time, RETRY_BACKOFF_MS);
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

        }

        @Override
        protected void onJoinComplete(int generation, String memberId, String protocol, ByteBuffer memberAssignment) {

        }
    }

}
