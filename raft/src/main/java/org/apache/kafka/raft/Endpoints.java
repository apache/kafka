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

import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class Endpoints {
    private final Map<ListenerName, InetSocketAddress> endpoints;

    private Endpoints(Map<ListenerName, InetSocketAddress> endpoints) {
        this.endpoints = endpoints;
    }

    public Optional<InetSocketAddress> address(ListenerName listener) {
        return Optional.ofNullable(endpoints.get(listener));
    }

    public Iterator<VotersRecord.Endpoint> votersRecordEndpoints() {
        return endpoints.entrySet()
            .stream()
            .map(entry ->
                new VotersRecord.Endpoint()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            )
            .iterator();
    }

    public int size() {
        return endpoints.size();
    }

    public boolean isEmpty() {
        return endpoints.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Endpoints that = (Endpoints) o;

        return endpoints.equals(that.endpoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoints);
    }

    @Override
    public String toString() {
        return String.format("Endpoints(endpoints=%s)", endpoints);
    }

    public BeginQuorumEpochRequestData.LeaderEndpointCollection toBeginQuorumEpochRequest() {
        BeginQuorumEpochRequestData.LeaderEndpointCollection leaderEndpoints =
            new BeginQuorumEpochRequestData.LeaderEndpointCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            leaderEndpoints.add(
                new BeginQuorumEpochRequestData.LeaderEndpoint()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }

        return leaderEndpoints;
    }

    public AddRaftVoterRequestData.ListenerCollection toAddVoterRequest() {
        AddRaftVoterRequestData.ListenerCollection listeners =
            new AddRaftVoterRequestData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new AddRaftVoterRequestData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }
        return listeners;
    }

    public DescribeQuorumResponseData.ListenerCollection toDescribeQuorumResponseListeners() {
        DescribeQuorumResponseData.ListenerCollection listeners =
            new DescribeQuorumResponseData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new DescribeQuorumResponseData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }
        return listeners;
    }

    public UpdateRaftVoterRequestData.ListenerCollection toUpdateVoterRequest() {
        UpdateRaftVoterRequestData.ListenerCollection listeners =
            new UpdateRaftVoterRequestData.ListenerCollection(endpoints.size());
        for (Map.Entry<ListenerName, InetSocketAddress> entry : endpoints.entrySet()) {
            listeners.add(
                new UpdateRaftVoterRequestData.Listener()
                    .setName(entry.getKey().value())
                    .setHost(entry.getValue().getHostString())
                    .setPort(entry.getValue().getPort())
            );
        }

        return listeners;
    }

    private static final Endpoints EMPTY = new Endpoints(Map.of());
    public static Endpoints empty() {
        return EMPTY;
    }

    public static Endpoints fromInetSocketAddresses(Map<ListenerName, InetSocketAddress> endpoints) {
        return new Endpoints(endpoints);
    }

    public static Endpoints fromVotersRecordEndpoints(Collection<VotersRecord.Endpoint> endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (VotersRecord.Endpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    public static Endpoints fromBeginQuorumEpochRequest(BeginQuorumEpochRequestData.LeaderEndpointCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (BeginQuorumEpochRequestData.LeaderEndpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    public static Endpoints fromBeginQuorumEpochResponse(
        ListenerName listenerName,
        int leaderId,
        BeginQuorumEpochResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    public static Endpoints fromEndQuorumEpochRequest(EndQuorumEpochRequestData.LeaderEndpointCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (EndQuorumEpochRequestData.LeaderEndpoint endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    public static Endpoints fromEndQuorumEpochResponse(
        ListenerName listenerName,
        int leaderId,
        EndQuorumEpochResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    public static Endpoints fromVoteResponse(
        ListenerName listenerName,
        int leaderId,
        VoteResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    public static Endpoints fromFetchResponse(
        ListenerName listenerName,
        int leaderId,
        FetchResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    public static Endpoints fromFetchSnapshotResponse(
        ListenerName listenerName,
        int leaderId,
        FetchSnapshotResponseData.NodeEndpointCollection endpoints
    ) {
        return Optional.ofNullable(endpoints.find(leaderId))
            .map(endpoint ->
                new Endpoints(
                    Map.of(
                        listenerName,
                        InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
                    )
                )
            )
            .orElse(Endpoints.empty());
    }

    public static Endpoints fromAddVoterRequest(AddRaftVoterRequestData.ListenerCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (AddRaftVoterRequestData.Listener endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }

    public static Endpoints fromUpdateVoterRequest(UpdateRaftVoterRequestData.ListenerCollection endpoints) {
        Map<ListenerName, InetSocketAddress> listeners = new HashMap<>(endpoints.size());
        for (UpdateRaftVoterRequestData.Listener endpoint : endpoints) {
            listeners.put(
                ListenerName.normalised(endpoint.name()),
                InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port())
            );
        }

        return new Endpoints(listeners);
    }
}
