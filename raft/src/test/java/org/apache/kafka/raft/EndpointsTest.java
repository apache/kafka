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
package org.apache.kafka.raft;

import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class EndpointsTest {
    private final ListenerName testListener = ListenerName.normalised("listener");
    private final InetSocketAddress testSocketAddress = InetSocketAddress.createUnresolved("localhost", 9092);

    @Test
    void testAddressWithValidEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        Optional<InetSocketAddress> address = endpoints.address(testListener);

        assertEquals(Optional.of(testSocketAddress), address);
    }

    @Test
    void testAddressWithEmptyEndpoint() {
        Endpoints endpoints = Endpoints.empty();

        Optional<InetSocketAddress> address = endpoints.address(ListenerName.normalised("nonExistentListener"));

        assertEquals(Optional.empty(), address);
    }

    @Test
    void testVotersRecordEndpointsWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        VotersRecord.Endpoint endpoint = endpoints.votersRecordEndpoints().next();

        assertEquals(testListener.value(), endpoint.name());
        assertEquals("localhost", endpoint.host());
        assertEquals(9092, endpoint.port());
    }

    @Test
    void testVotersRecordEndpointsWithEmptyEndpoint() {
        assertFalse(Endpoints.empty().votersRecordEndpoints().hasNext());
    }

    @Test
    void testSize() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));

        assertEquals(1, Endpoints.fromInetSocketAddresses(endpointMap).size());
        assertEquals(0, Endpoints.empty().size());
    }

    @Test
    void testIsEmptyWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));

        assertFalse(Endpoints.fromInetSocketAddresses(endpointMap).isEmpty());
    }

    @Test
    void testEqualsAndHashCodeWithSameEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));

        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);
        Endpoints sameEndpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        assertEquals(endpoints, sameEndpoints);
        assertEquals(endpoints.hashCode(), sameEndpoints.hashCode());
    }

    @Test
    void testEqualsAndHashCodeWithDifferentEndpoints() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        Map<ListenerName, InetSocketAddress> anotherEndpointMap = Utils.mkMap(
                Utils.mkEntry(
                        ListenerName.normalised("another"),
                        InetSocketAddress.createUnresolved("localhost", 9093)));
        Endpoints differentEndpoints = Endpoints.fromInetSocketAddresses(anotherEndpointMap);

        assertNotEquals(endpoints, differentEndpoints);
        assertNotEquals(endpoints.hashCode(), differentEndpoints.hashCode());
    }

    @Test
    void testToBeginQuorumEpochRequestWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        BeginQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints = endpoints.toBeginQuorumEpochRequest();

        assertEquals(1, leaderEndpoints.size());

        BeginQuorumEpochRequestData.LeaderEndpoint leaderEndpoint = leaderEndpoints.iterator().next();
        assertEquals(testListener.value(), leaderEndpoint.name());
        assertEquals("localhost", leaderEndpoint.host());
        assertEquals(9092, leaderEndpoint.port());
    }

    @Test
    void testToBeginQuorumEpochRequestWithEmptyEndpoint() {
        assertEquals(0, Endpoints.empty().toBeginQuorumEpochRequest().size());
    }

    @Test
    void testFromInetSocketAddressesWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));

        assertEquals(1, Endpoints.fromInetSocketAddresses(endpointMap).size());
    }

    @Test
    void testFromVotersRecordEndpointsWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        List<VotersRecord.Endpoint> votersEndpoints = new ArrayList<>();
        votersEndpoints.add(
                new VotersRecord.Endpoint()
                        .setName("listener")
                        .setHost("localhost")
                        .setPort(9092));

        Endpoints createdEndpoints = Endpoints.fromVotersRecordEndpoints(votersEndpoints);

        assertEquals(endpoints, createdEndpoints);
    }

    @Test
    void testFromVotersRecordEndpointsWithEmptyEndpoint() {
        List<VotersRecord.Endpoint> votersEndpoints = Collections.emptyList();

        assertEquals(Endpoints.empty(), Endpoints.fromVotersRecordEndpoints(votersEndpoints));
    }

    @Test
    void testFromBeginQuorumEpochRequestWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        BeginQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints = new BeginQuorumEpochRequestData.LeaderEndpointCollection();
        leaderEndpoints.add(
                new BeginQuorumEpochRequestData.LeaderEndpoint()
                        .setName("listener")
                        .setHost("localhost")
                        .setPort(9092));

        Endpoints createdEndpoints = Endpoints.fromBeginQuorumEpochRequest(leaderEndpoints);

        assertEquals(endpoints, createdEndpoints);
    }

    @Test
    void testFromBeginQuorumEpochRequestWithEmptyEndpoint() {
        BeginQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints = new BeginQuorumEpochRequestData.LeaderEndpointCollection();

        Endpoints createdEndpoints = Endpoints.fromBeginQuorumEpochRequest(leaderEndpoints);

        assertEquals(Endpoints.empty(), createdEndpoints);
    }

    @Test
    void testFromBeginQuorumEpochResponseWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        BeginQuorumEpochResponseData.NodeEndpointCollection nodeEndpointCollection = new BeginQuorumEpochResponseData.NodeEndpointCollection();
        nodeEndpointCollection.add(
                new BeginQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(1)
                        .setHost("localhost")
                        .setPort(9092));

        Endpoints createdEndpoints = Endpoints.fromBeginQuorumEpochResponse(testListener, 1, nodeEndpointCollection);

        assertEquals(endpoints, createdEndpoints);
    }

    @Test
    void testFromBeginQuorumEpochResponseWithEmptyEndpoint() {
        BeginQuorumEpochResponseData.NodeEndpointCollection nodeEndpointCollection = new BeginQuorumEpochResponseData.NodeEndpointCollection();

        Endpoints createdEndpoints = Endpoints.fromBeginQuorumEpochResponse(ListenerName.normalised("nonExistListener"),
                1, nodeEndpointCollection);

        assertEquals(Endpoints.empty(), createdEndpoints);
    }

    @Test
    void testFromEndQuorumEpochRequestWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        EndQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints = new EndQuorumEpochRequestData.LeaderEndpointCollection();
        leaderEndpoints.add(
                new EndQuorumEpochRequestData.LeaderEndpoint()
                        .setName("listener")
                        .setHost("localhost")
                        .setPort(9092));

        assertEquals(endpoints, Endpoints.fromEndQuorumEpochRequest(leaderEndpoints));
    }

    @Test
    void testFromEndQuorumEpochRequestWithEmptyEndpoint() {
        EndQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints = new EndQuorumEpochRequestData.LeaderEndpointCollection();

        assertEquals(Endpoints.empty(), Endpoints.fromEndQuorumEpochRequest(leaderEndpoints));
    }

    @Test
    void testFromEndQuorumEpochResponseWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        EndQuorumEpochResponseData.NodeEndpointCollection nodeEndpointCollection = new EndQuorumEpochResponseData.NodeEndpointCollection();
        nodeEndpointCollection.add(
                new EndQuorumEpochResponseData.NodeEndpoint()
                        .setNodeId(1)
                        .setHost("localhost")
                        .setPort(9092));

        assertEquals(endpoints, Endpoints.fromEndQuorumEpochResponse(testListener, 1, nodeEndpointCollection));
    }

    @Test
    void testFromEndQuorumEpochResponseWithEmptyEndpoint() {
        EndQuorumEpochResponseData.NodeEndpointCollection nodeEndpointCollection = new EndQuorumEpochResponseData.NodeEndpointCollection();

        Endpoints createdEndpoints = Endpoints.fromEndQuorumEpochResponse(ListenerName.normalised("nonExistListener"),
                1, nodeEndpointCollection);

        assertEquals(Endpoints.empty(), createdEndpoints);
    }

    @Test
    void testFromVoteResponseWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        VoteResponseData.NodeEndpointCollection nodeEndpointCollection = new VoteResponseData.NodeEndpointCollection();
        nodeEndpointCollection.add(
                new VoteResponseData.NodeEndpoint()
                        .setNodeId(1)
                        .setHost("localhost")
                        .setPort(9092));

        assertEquals(endpoints, Endpoints.fromVoteResponse(testListener, 1, nodeEndpointCollection));
    }

    @Test
    void testFromVoteResponseWithEmptyEndpoint() {
        VoteResponseData.NodeEndpointCollection nodeEndpointCollection = new VoteResponseData.NodeEndpointCollection();

        Endpoints createdEndpoints = Endpoints.fromVoteResponse(ListenerName.normalised("nonExistListener"), 1,
                nodeEndpointCollection);

        assertEquals(Endpoints.empty(), createdEndpoints);
    }

    @Test
    void testFromFetchResponseWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        FetchResponseData.NodeEndpointCollection nodeEndpointCollection = new FetchResponseData.NodeEndpointCollection();
        nodeEndpointCollection.add(
                new FetchResponseData.NodeEndpoint()
                        .setNodeId(1)
                        .setHost("localhost")
                        .setPort(9092));

        assertEquals(endpoints, Endpoints.fromFetchResponse(testListener, 1, nodeEndpointCollection));
    }

    @Test
    void testFromFetchResponseWithEmptyEndpoint() {
        FetchResponseData.NodeEndpointCollection nodeEndpointCollection = new FetchResponseData.NodeEndpointCollection();

        assertEquals(Endpoints.empty(), Endpoints.fromFetchResponse(ListenerName.normalised("nonExistListener"), 1,
                nodeEndpointCollection));
    }

    @Test
    void testFromFetchSnapshotResponseWithEndpoint() {
        Map<ListenerName, InetSocketAddress> endpointMap = Utils.mkMap(
                Utils.mkEntry(testListener, testSocketAddress));
        Endpoints endpoints = Endpoints.fromInetSocketAddresses(endpointMap);

        FetchSnapshotResponseData.NodeEndpointCollection nodeEndpointCollection = new FetchSnapshotResponseData.NodeEndpointCollection();
        nodeEndpointCollection.add(
                new FetchSnapshotResponseData.NodeEndpoint()
                        .setNodeId(1)
                        .setHost("localhost")
                        .setPort(9092));

        assertEquals(endpoints, Endpoints.fromFetchSnapshotResponse(testListener, 1, nodeEndpointCollection));
    }

    @Test
    void testFromFetchSnapshotResponseWithEmptyEndpoint() {
        FetchSnapshotResponseData.NodeEndpointCollection nodeEndpointCollection = new FetchSnapshotResponseData.NodeEndpointCollection();

        Endpoints createdEndpoints = Endpoints.fromFetchSnapshotResponse(ListenerName.normalised("nonExistListener"), 1,
                nodeEndpointCollection);

        assertEquals(Endpoints.empty(), createdEndpoints);
    }
}
