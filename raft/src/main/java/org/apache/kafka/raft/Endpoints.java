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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;

final public class Endpoints {
    Map<ListenerName, InetSocketAddress> endpoints;

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

    final private static Endpoints EMPTY = new Endpoints(Collections.emptyMap());
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
}
