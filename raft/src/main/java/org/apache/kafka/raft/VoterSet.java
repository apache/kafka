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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

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
import java.util.stream.Stream;

/**
 * A type for representing the set of voters for a topic partition.
 *
 * It encapsulates static information like a voter's endpoint and their supported kraft.version.
 *
 * It provides functionality for converting to and from {@code VotersRecord} and for converting
 * from the static configuration.
 */
public final class VoterSet {
    private final Map<Integer, VoterNode> voters;

    private VoterSet(Map<Integer, VoterNode> voters) {
        this.voters = voters;
    }

    /**
     * Returns the node information for all the given voter ids and listener.
     *
     * @param voterIds the ids of the voters
     * @param listenerName the name of the listener
     * @return the node information for all of the voter ids
     * @throws IllegalArgumentException if there are missing endpoints
     */
    public Set<Node> voterNodes(Stream<Integer> voterIds, ListenerName listenerName) {
        return voterIds
            .map(voterId ->
                voterNode(voterId, listenerName).orElseThrow(() ->
                    new IllegalArgumentException(
                        String.format(
                            "Unable to find endpoint for voter %d and listener %s in %s",
                            voterId,
                            listenerName,
                            voters
                        )
                    )
                )
            )
            .collect(Collectors.toSet());
    }

    /**
     * Returns the node information for a given voter id and listener.
     *
     * @param voterId the id of the voter
     * @param listenerName the name of the listener
     * @return the node information if it exists, otherwise {@code Optional.empty()}
     */
    public Optional<Node> voterNode(int voterId, ListenerName listenerName) {
        return Optional.ofNullable(voters.get(voterId))
            .flatMap(voterNode -> voterNode.address(listenerName))
            .map(address -> new Node(voterId, address.getHostString(), address.getPort()));
    }

    /**
     * Return true if the provided voter node is a voter and would cause a change in the voter set.
     *
     * @param updatedVoterNode the updated voter node
     * @return true if the updated voter node is different than the node in the voter set; otherwise false.
     */
    public boolean voterNodeNeedsUpdate(VoterNode updatedVoterNode) {
        return Optional.ofNullable(voters.get(updatedVoterNode.voterKey().id()))
            .map(
                node -> node.isVoter(updatedVoterNode.voterKey()) &&
                        !node.equals(updatedVoterNode)
            )
            .orElse(false);
    }

    /**
     * Returns if the node is a voter in the set of voters.
     *
     * If the voter set includes the directory id, the {@code replicaKey} directory id must match the
     * directory id specified by the voter set.
     *
     * If the voter set doesn't include the directory id ({@code Optional.empty()}), a node is in
     * the voter set as long as the node id matches. The directory id is not checked.
     *
     * @param replicaKey the node's id and directory id
     * @return true if the node is a voter in the voter set, otherwise false
     */
    public boolean isVoter(ReplicaKey replicaKey) {
        return Optional.ofNullable(voters.get(replicaKey.id()))
            .map(node -> node.isVoter(replicaKey))
            .orElse(false);
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
     * Returns all of the voters.
     */
    public Set<ReplicaKey> voterKeys() {
        return voters
            .values()
            .stream()
            .map(VoterNode::voterKey)
            .collect(Collectors.toSet());
    }

    /**
     * Returns all of the voters.
     */
    public Set<VoterNode> voterNodes() {
        return new HashSet<>(voters.values());
    }

    /**
     * Returns all of the endpoints for a voter id.
     *
     * {@code Endpoints.empty()} is returned if the id is not a voter.
     *
     * @param voterId the id of the voter
     * @return the endpoints for the voter
     */
    public Endpoints listeners(int voterId) {
        return Optional.ofNullable(voters.get(voterId))
            .map(VoterNode::listeners)
            .orElse(Endpoints.empty());
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
     * A voter can be removed from the voter set if its id and directory id match and there
     * are more than one voter in the set of voters.
     *
     * @param voterKey the voter key
     * @return a new voter set if the voter was removed, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> removeVoter(ReplicaKey voterKey) {
        VoterNode oldVoter = voters.get(voterKey.id());
        if (oldVoter != null &&
            Objects.equals(oldVoter.voterKey(), voterKey) &&
            voters.size() > 1
        ) {
            HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
            newVoters.remove(voterKey.id());

            return Optional.of(new VoterSet(newVoters));
        }

        return Optional.empty();
    }

    /**
     * Updates a voter in the voter set.
     *
     * @param voter the updated voter
     * @return a new voter set if the voter was updated, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> updateVoter(VoterNode voter) {
        VoterNode oldVoter = voters.get(voter.voterKey().id());
        if (oldVoter != null && oldVoter.isVoter(voter.voterKey())) {
            HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
            newVoters.put(voter.voterKey().id(), voter);

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
                .votersRecordEndpoints();

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
        Set<ReplicaKey> thisReplicaKeys = voterKeys();
        Set<ReplicaKey> thatReplicaKeys = that.voterKeys();

        if (Utils.diff(HashSet::new, thisReplicaKeys, thatReplicaKeys).size() > 1) return false;
        return Utils.diff(HashSet::new, thatReplicaKeys, thisReplicaKeys).size() <= 1;
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

    public static final class VoterNode {
        private final ReplicaKey voterKey;
        private final Endpoints listeners;
        private final SupportedVersionRange supportedKRaftVersion;

        VoterNode(
            ReplicaKey voterKey,
            Endpoints listeners,
            SupportedVersionRange supportedKRaftVersion
        ) {
            this.voterKey = voterKey;
            this.listeners = listeners;
            this.supportedKRaftVersion = supportedKRaftVersion;
        }

        public ReplicaKey voterKey() {
            return voterKey;
        }

        /**
         * Returns if the provided replica key matches this voter node.
         *
         * If the voter node includes the directory id, the {@code replicaKey} directory id must
         * match the directory id specified by the voter set.
         *
         * If the voter node doesn't include the directory id ({@code Optional.empty()}), a replica
         * is the voter as long as the node id matches. The directory id is not checked.
         *
         * @param replicaKey the replica key
         * @return true if the replica key is the voter, otherwise false
         */
        public boolean isVoter(ReplicaKey replicaKey) {
            if (voterKey.id() != replicaKey.id()) return false;

            if (voterKey.directoryId().isPresent()) {
                return voterKey.directoryId().equals(replicaKey.directoryId());
            } else {
                // configured voter set doesn't include a directory id so it is a voter as long as
                // the ids match
                return true;
            }
        }

        /**
         * Returns the listeners of the voter node
         */
        public Endpoints listeners() {
            return listeners;
        }

        SupportedVersionRange supportedKRaftVersion() {
            return supportedKRaftVersion;
        }


        Optional<InetSocketAddress> address(ListenerName listener) {
            return listeners.address(listener);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoterNode that = (VoterNode) o;

            if (!Objects.equals(voterKey, that.voterKey)) return false;
            if (!Objects.equals(supportedKRaftVersion, that.supportedKRaftVersion)) return false;
            return Objects.equals(listeners, that.listeners);
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

        public static VoterNode of(
            ReplicaKey voterKey,
            Endpoints listeners,
            SupportedVersionRange supportedKRaftVersion
        ) {
            return new VoterNode(voterKey, listeners, supportedKRaftVersion);
        }
    }

    private static final VoterSet EMPTY = new VoterSet(Collections.emptyMap());
    public static VoterSet empty() {
        return EMPTY;
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
            voterNodes.put(
                voter.voterId(),
                new VoterNode(
                    ReplicaKey.of(voter.voterId(), voter.voterDirectoryId()),
                    Endpoints.fromVotersRecordEndpoints(voter.endpoints()),
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
    public static VoterSet fromInetSocketAddresses(ListenerName listener, Map<Integer, InetSocketAddress> voters) {
        Map<Integer, VoterNode> voterNodes = voters
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new VoterNode(
                        ReplicaKey.of(entry.getKey(), Uuid.ZERO_UUID),
                        Endpoints.fromInetSocketAddresses(Collections.singletonMap(listener, entry.getValue())),
                        new SupportedVersionRange((short) 0, (short) 0)
                    )
                )
            );

        return new VoterSet(voterNodes);
    }

    public static VoterSet fromMap(Map<Integer, VoterNode> voters) {
        return new VoterSet(new HashMap<>(voters));
    }
}
