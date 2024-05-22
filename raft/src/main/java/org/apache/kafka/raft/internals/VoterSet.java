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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.Utils;

/**
 * A type for representing the set of voters for a topic partition.
 *
 * It encapsulates static information like a voter's endpoint and their supported kraft.version.
 *
 * It provides functionality for converting to and from {@code VotersRecord} and for converting
 * from the static configuration.
 */
final public class VoterSet {
    private final Map<Integer, VoterNode> voters;

    VoterSet(Map<Integer, VoterNode> voters) {
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("Voters cannot be empty");
        }

        this.voters = voters;
    }

    /**
     * Returns the socket address for a given voter at a given listener.
     *
     * @param voter the id of the voter
     * @param listener the name of the listener
     * @return the socket address if it exists, otherwise {@code Optional.empty()}
     */
    public Optional<InetSocketAddress> voterAddress(int voter, String listener) {
        return Optional.ofNullable(voters.get(voter))
            .flatMap(voterNode -> voterNode.address(listener));
    }

    /**
     * Returns if the node is a voter in the set of voters.
     *
     * If the voter set includes the directory id, the {@code nodeKey} directory id must match the
     * directory id specified by the voter set.
     *
     * If the voter set doesn't include the directory id ({@code Optional.empty()}), a node is in
     * the voter set as long as the node id matches. The directory id is not checked.
     *
     * @param nodeKey the node's id and directory id
     * @return true if the node is a voter in the voter set, otherwise false
     */
    public boolean isVoter(ReplicaKey nodeKey) {
        VoterNode node = voters.get(nodeKey.id());
        if (node != null) {
            if (node.voterKey().directoryId().isPresent()) {
                return node.voterKey().directoryId().equals(nodeKey.directoryId());
            } else {
                // configured voter set doesn't include a directory id so it is a voter as long as the node id
                // matches
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * Returns if the node is the only voter in the set of voters.
     *
     * @param nodeKey the node's id and directory id
     * @return true if the node is the only voter in the voter set, otherwise false
     */
    public boolean isOnlyVoter(ReplicaKey nodeKey) {
        return voters.size() == 1 && isVoter(nodeKey);
    }

    /**
     * Returns all of the voter ids.
     */
    public Set<Integer> voterIds() {
        return voters.keySet();
    }

    /**
     * Adds a voter to the voter set.
     *
     * This object is immutable. A new voter set is returned if the voter was added.
     *
     * A new voter can be added to a voter set if its id doesn't already exist in the voter set.
     *
     * @param voter the new voter to add
     * @return a new voter set if the voter was added, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> addVoter(VoterNode voter) {
        if (voters.containsKey(voter.voterKey().id())) {
            return Optional.empty();
        }

        HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.put(voter.voterKey().id(), voter);

        return Optional.of(new VoterSet(newVoters));
    }

    /**
     * Remove a voter from the voter set.
     *
     * This object is immutable. A new voter set is returned if the voter was removed.
     *
     * A voter can be removed from the voter set if its id and directory id match.
     *
     * @param voterKey the voter key
     * @return a new voter set if the voter was removed, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> removeVoter(ReplicaKey voterKey) {
        VoterNode oldVoter = voters.get(voterKey.id());
        if (oldVoter != null && Objects.equals(oldVoter.voterKey(), voterKey)) {
            HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
            newVoters.remove(voterKey.id());

            return Optional.of(new VoterSet(newVoters));
        }

        return Optional.empty();
    }

    /**
     * Converts a voter set to a voters record for a given version.
     *
     * @param version the version of the voters record
     */
    public VotersRecord toVotersRecord(short version) {
        Function<VoterNode, VotersRecord.Voter> voterConvertor = voter -> {
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

            VotersRecord.KRaftVersionFeature kraftVersionFeature = new VotersRecord.KRaftVersionFeature()
                .setMinSupportedVersion(voter.supportedKRaftVersion().min())
                .setMaxSupportedVersion(voter.supportedKRaftVersion().max());

            return new VotersRecord.Voter()
                .setVoterId(voter.voterKey().id())
                .setVoterDirectoryId(voter.voterKey().directoryId().orElse(Uuid.ZERO_UUID))
                .setEndpoints(new VotersRecord.EndpointCollection(endpoints))
                .setKRaftVersionFeature(kraftVersionFeature);
        };

        List<VotersRecord.Voter> voterRecordVoters = voters
            .values()
            .stream()
            .map(voterConvertor)
            .collect(Collectors.toList());

        return new VotersRecord()
            .setVersion(version)
            .setVoters(voterRecordVoters);
    }

    /**
     * Determines if two sets of voters have an overlapping majority.
     *
     * An overlapping majority means that for all majorities in {@code this} set of voters and for
     * all majority in {@code that} set of voters, they have at least one voter in common.
     *
     * If this function returns true, it means that if one of the set of voters commits an offset,
     * the other set of voters cannot commit a conflicting offset.
     *
     * @param that the other voter set to compare
     * @return true if they have an overlapping majority, false otherwise
     */
    public boolean hasOverlappingMajority(VoterSet that) {
        Set<ReplicaKey> thisReplicaKeys = voters
            .values()
            .stream()
            .map(VoterNode::voterKey)
            .collect(Collectors.toSet());

        Set<ReplicaKey> thatReplicaKeys = that.voters
            .values()
            .stream()
            .map(VoterNode::voterKey)
            .collect(Collectors.toSet());

        if (Utils.diff(HashSet::new, thisReplicaKeys, thatReplicaKeys).size() > 1) return false;
        if (Utils.diff(HashSet::new, thatReplicaKeys, thisReplicaKeys).size() > 1) return false;

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

    public final static class VoterNode {
        private final ReplicaKey voterKey;
        private final Map<String, InetSocketAddress> listeners;
        private final SupportedVersionRange supportedKRaftVersion;

        VoterNode(
            ReplicaKey voterKey,
            Map<String, InetSocketAddress> listeners,
            SupportedVersionRange supportedKRaftVersion
        ) {
            this.voterKey = voterKey;
            this.listeners = listeners;
            this.supportedKRaftVersion = supportedKRaftVersion;
        }

        public ReplicaKey voterKey() {
            return voterKey;
        }

        Map<String, InetSocketAddress> listeners() {
            return listeners;
        }

        SupportedVersionRange supportedKRaftVersion() {
            return supportedKRaftVersion;
        }


        Optional<InetSocketAddress> address(String listener) {
            return Optional.ofNullable(listeners.get(listener));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoterNode that = (VoterNode) o;

            if (!Objects.equals(voterKey, that.voterKey)) return false;
            if (!Objects.equals(supportedKRaftVersion, that.supportedKRaftVersion)) return false;
            if (!Objects.equals(listeners, that.listeners)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(voterKey, listeners, supportedKRaftVersion);
        }

        @Override
        public String toString() {
            return String.format(
                "VoterNode(voterKey=%s, listeners=%s, supportedKRaftVersion=%s)",
                voterKey,
                listeners,
                supportedKRaftVersion
            );
        }
    }

    /**
     * Converts a {@code VotersRecord} to a {@code VoterSet}.
     *
     * @param voters the set of voters control record
     * @return the voter set
     */
    public static VoterSet fromVotersRecord(VotersRecord voters) {
        HashMap<Integer, VoterNode> voterNodes = new HashMap<>(voters.voters().size());
        for (VotersRecord.Voter voter: voters.voters()) {
            final Optional<Uuid> directoryId;
            if (!voter.voterDirectoryId().equals(Uuid.ZERO_UUID)) {
                directoryId = Optional.of(voter.voterDirectoryId());
            } else {
                directoryId = Optional.empty();
            }

            Map<String, InetSocketAddress> listeners = new HashMap<>(voter.endpoints().size());
            for (VotersRecord.Endpoint endpoint : voter.endpoints()) {
                listeners.put(endpoint.name(), InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port()));
            }

            voterNodes.put(
                voter.voterId(),
                new VoterNode(
                    ReplicaKey.of(voter.voterId(), directoryId),
                    listeners,
                    new SupportedVersionRange(
                        voter.kRaftVersionFeature().minSupportedVersion(),
                        voter.kRaftVersionFeature().maxSupportedVersion()
                    )
                )
            );
        }

        return new VoterSet(voterNodes);
    }

    /**
     * Creates a voter set from a map of socket addresses.
     *
     * @param listener the listener name for all of the endpoints
     * @param voters the socket addresses by voter id
     * @return the voter set
     */
    public static VoterSet fromInetSocketAddresses(String listener, Map<Integer, InetSocketAddress> voters) {
        Map<Integer, VoterNode> voterNodes = voters
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new VoterNode(
                        ReplicaKey.of(entry.getKey(), Optional.empty()),
                        Collections.singletonMap(listener, entry.getValue()),
                        new SupportedVersionRange((short) 0, (short) 0)
                    )
                )
            );

        return new VoterSet(voterNodes);
    }
}
