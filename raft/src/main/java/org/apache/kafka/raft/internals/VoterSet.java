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
package org.apache.kafka.raft.internals;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.Utils;

// TODO: write unittest for VoterSet
// TODO: Write documentation
final public class VoterSet {
    private final Map<Integer, VoterNode> voters;

    VoterSet(Map<Integer, VoterNode> voters) {
        this.voters = voters;
    }

    Optional<InetSocketAddress> voterAddress(int voter, String listener) {
        return Optional.ofNullable(voters.get(voter))
            .flatMap(voterNode -> voterNode.address(listener));
    }

    VotersRecord toVotersRecord(short version) {
        return new VotersRecord()
            .setVersion(version)
            .setVoters(
                voters
                    .values()
                    .stream()
                    .map(voter -> {
                        Iterator<VotersRecord.Endpoint> endpoints = voter
                            .listeners()
                            .entrySet()
                            .stream()
                            .map(entry ->
                                new VotersRecord.Endpoint()
                                    .setName(entry.getKey())
                                    .setHost(entry.getValue().getHostString())
                                    .setPort(entry.getValue().getPort())
                            )
                            .iterator();

                        return new VotersRecord.Voter()
                            .setVoterId(voter.id())
                            .setVoterUuid(voter.uuid().orElse(Uuid.ZERO_UUID))
                            .setEndpoints(new VotersRecord.EndpointCollection(endpoints))
                            .setKRaftVersionFeature(voter.feature());
                    })
                    .collect(Collectors.toList())
            );
    }

    boolean hasOverlappingMajority(VoterSet that) {
        if (Utils.diff(HashSet::new, voters.keySet(), that.voters.keySet()).size() > 2) return false;
        if (Utils.diff(HashSet::new, that.voters.keySet(), voters.keySet()).size() > 2) return false;

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VoterSet that = (VoterSet) o;

        return voters.equals(that.voters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(voters);
    }

    @Override
    public String toString() {
        return String.format("VoterSet(voters=%s)", voters);
    }

    final static class VoterNode {
        private final int id;
        private final Optional<Uuid> uuid;
        private final Map<String, InetSocketAddress> listeners;
        // TODO: is there a better type for this?
        private final VotersRecord.KRaftVersionFeature feature;

        VoterNode(
            int id,
            Optional<Uuid> uuid,
            Map<String, InetSocketAddress> listeners,
            VotersRecord.KRaftVersionFeature feature
        ) {
            this.id = id;
            this.uuid = uuid;
            this.listeners = listeners;
            this.feature = feature;
        }

        int id() {
            return id;
        }

        Optional<Uuid> uuid() {
            return uuid;
        }

        Map<String, InetSocketAddress> listeners() {
            return listeners;
        }

        VotersRecord.KRaftVersionFeature feature() {
            return feature;
        }


        Optional<InetSocketAddress> address(String listener) {
            return Optional.ofNullable(listeners.get(listener));
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoterNode that = (VoterNode) o;

            if (id != that.id) return false;
            if (!Objects.equals(uuid, that.uuid)) return false;
            if (!Objects.equals(feature, that.feature)) return false;
            if (!Objects.equals(listeners, that.listeners)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, uuid, listeners, feature);
        }

        @Override
        public String toString() {
            return String.format("VoterNode(id=%d, uuid=%s, listeners=%s, feature=%s)", id, uuid, listeners, feature);
        }
    }

    public static VoterSet fromVotersRecord(VotersRecord voters) {
        Map<Integer, VoterNode> voterNodes = new HashMap<>(voters.voters().size());
        for (VotersRecord.Voter voter: voters.voters()) {
            final Optional<Uuid> uuid;
            if (!voter.voterUuid().equals(Uuid.ZERO_UUID)) {
                uuid = Optional.of(voter.voterUuid());
            } else {
                uuid = Optional.empty();
            }

            Map<String, InetSocketAddress> listeners = new HashMap<>(voter.endpoints().size());
            for (VotersRecord.Endpoint endpoint : voter.endpoints()) {
                listeners.put(endpoint.name(), InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port()));
            }

            voterNodes.put(voter.voterId(), new VoterNode(voter.voterId(), uuid, listeners, voter.kRaftVersionFeature()));
        }

        return new VoterSet(voterNodes);
    }

    public static VoterSet fromAddressSpecs(String listener, Map<Integer, InetSocketAddress> voters) {
        Map<Integer, VoterNode> voterNodes = voters
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new VoterNode(
                        entry.getKey(),
                        Optional.empty(),
                        Collections.singletonMap(listener, entry.getValue()),
                        new VotersRecord.KRaftVersionFeature()
                    )
                )
            );
        return new VoterSet(voterNodes);
    }
}
