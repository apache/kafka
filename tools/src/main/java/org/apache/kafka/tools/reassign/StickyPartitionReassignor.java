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
package org.apache.kafka.tools.reassign;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashSet;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;


public class StickyPartitionReassignor {

    private final Map<Integer, Integer> brokerToAssignments;

    private final TreeSet<BrokerMetadata> sortedBrokersByAssignments;

    private final Map<ReplicaId, Replica> replicas;
    private final Set<Partition> partitions;

    private final Map<BrokerMetadata, Map<BrokerMetadata, TreeSet<MoveFrom>>> possibleMoves;

    private final Set<Optional<String>> racks;


    public StickyPartitionReassignor(Map<TopicPartition, List<Integer>> currentAssignments, List<BrokerMetadata> brokerMetadatas) {
        this.possibleMoves = new HashMap<>();
        this.brokerToAssignments = new HashMap<>();
        this.sortedBrokersByAssignments = new TreeSet<>(new BrokerReplicaCountComparator(this.brokerToAssignments));

        this.racks = new HashSet<>();
        final Map<Integer, BrokerMetadata> brokers = new HashMap<>();

        int rackAwareBrokers = 0;
        int rackUnawareBrokers = 0;

        for (final BrokerMetadata brokerMetadata : brokerMetadatas) {
            if (brokerMetadata.rack.isPresent()) {
                rackAwareBrokers++;
            } else {
                rackUnawareBrokers++;
            }
            this.racks.add(brokerMetadata.rack);
            brokers.put(brokerMetadata.id, brokerMetadata);
        }

        if (rackAwareBrokers > 0 && rackUnawareBrokers > 0)
            throw new RuntimeException("Not all brokers have rack information for replica rack aware assignment.");

        this.replicas = new HashMap<>();
        this.partitions = new HashSet<>();

        this.initReplicas(currentAssignments, brokers);

        this.sortedBrokersByAssignments.addAll(brokerMetadatas);

        for (final Map.Entry<ReplicaId, Replica> entry : this.replicas.entrySet()) {
            final Replica replica = entry.getValue();
            this.computeAndUpdatePossibleMoves(replica);
        }
    }

    private static MoveFrom getMoveFrom(TreeSet<MoveFrom> toBrokerMoves, BrokerMetadata fromBroker, BrokerMetadata toBroker) {
        final MoveFrom moveFrom = toBrokerMoves.first();
        if (moveFrom == null)
            throw new RuntimeException(String.format("No available moves from %s to %s", fromBroker, toBroker));
        if (!moveFrom.replica.currentBroker.equals(fromBroker))
            throw new RuntimeException(String.format("Chosen from-broker %s do not match move %s", fromBroker, moveFrom));
        if (!moveFrom.to.equals(toBroker))
            throw new RuntimeException(String.format("Chosen to-broker %s do not match move %s", toBroker, moveFrom));
        return moveFrom;
    }

    private void initReplicas(Map<TopicPartition, List<Integer>> currentAssignments, Map<Integer, BrokerMetadata> brokers) {
        for (final Map.Entry<TopicPartition, List<Integer>> entry : currentAssignments.entrySet()) {
            final List<Integer> replicaAssignments = entry.getValue();
            final TopicPartition topicPartition = entry.getKey();

            final Partition partition = new Partition(topicPartition);
            this.partitions.add(partition);

            for (int i = 0; i < replicaAssignments.size(); i++) {
                final Integer currentNodeId = replicaAssignments.get(i);
                if (currentNodeId == null)
                    throw new RuntimeException(String.format("Replica assignment for %s %s was null", topicPartition, i));
                final BrokerMetadata currentBroker = brokers.get(currentNodeId);
                if (currentBroker == null)
                    throw new RuntimeException(String.format("Replica assignment for %s %s references unknown broker %s", topicPartition, i, currentNodeId));

                this.brokerToAssignments.compute(currentNodeId, (key, value) -> value == null ? 1 : value + 1);

                final ReplicaId replicaId = new ReplicaId(partition, i);
                final Replica replica = new Replica(replicaId, currentBroker);

                if (!partition.replicas.add(replica))
                    throw new RuntimeException(String.format("Replica %s was already added to partition %s", replica.id, partition));

                partition.initPartitionCounts(replica);

                if (this.replicas.put(replicaId, replica) != null)
                    throw new RuntimeException(String.format("Replica %s was already present", replicaId));
            }
        }
    }

    private long computeCurrentBrokerScore(Replica replica) {
        long brokerScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            final int replicasOnBroker = replica.id.partition.replicaCountPerBroker.getOrDefault(broker, 0);
            brokerScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return brokerScore;
    }

    private long computeNewBrokerScore(Replica replica, BrokerMetadata to) {
        long brokerScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            int replicasOnBroker = replica.id.partition.replicaCountPerBroker.getOrDefault(broker, 0);
            if (broker.equals(replica.currentBroker)) replicasOnBroker--;
            if (broker.equals(to)) replicasOnBroker++;
            brokerScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return brokerScore;
    }

    private long computeCurrentRackScore(Replica replica) {
        long rackScore = 0;
        for (final Optional<String> rack : this.racks) {
            final int replicasOnRack = replica.id.partition.replicaCountPerRack.getOrDefault(rack, 0);
            rackScore += (long) Math.pow(replicasOnRack, 2);
        }
        return rackScore;
    }

    private long computeNewRackScore(Replica replica, BrokerMetadata to) {
        final Optional<String> currentRack = replica.currentBroker.rack;
        final Optional<String> newRack = to.rack;
        long rackScore = 0;
        for (final Optional<String> rack : this.racks) {
            int replicasOnRack = replica.id.partition.replicaCountPerRack.getOrDefault(rack, 0);
            if (rack.equals(currentRack)) replicasOnRack--;
            if (rack.equals(newRack)) replicasOnRack++;

            rackScore += (long) Math.pow(replicasOnRack, 2);
        }
        return rackScore;
    }

    private Set<MoveFrom> getPossibleMovesRackAwareness(Replica replica) {
        final Set<MoveFrom> possibleMoves = new TreeSet<>(new MovableFromScoreComparator());

        final long currentRackScore = this.computeCurrentRackScore(replica);
        final long currentBrokerScore = this.computeCurrentBrokerScore(replica);

        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            if (replica.currentBroker.equals(broker)) continue;

            final long newBrokerScore = this.computeNewBrokerScore(replica, broker);
            final long brokerImprovementScore = newBrokerScore - currentBrokerScore;

            if (brokerImprovementScore > 0)
                continue;

            final long newRackScore = this.computeNewRackScore(replica, broker);
            final long rackImprovementScore = newRackScore - currentRackScore;

            if (rackImprovementScore <= 0)
                possibleMoves.add(new MoveFrom(replica, broker, rackImprovementScore, brokerImprovementScore));
        }

        return possibleMoves;
    }

    private void adjustGlobalPossibleMoves(BrokerMetadata fromBroker, List<MoveFrom> toRemove, List<MoveFrom> toAdd) {
        Map<BrokerMetadata, TreeSet<MoveFrom>> fromBrokerMoves = this.possibleMoves.get(fromBroker);

        for (final MoveFrom move : toRemove) {
            if (fromBrokerMoves == null)
                throw new RuntimeException(String.format("Current broker %s has no available moves to another broker", fromBroker));
            final Set<MoveFrom> previousMoves = fromBrokerMoves.get(move.to);
            if (previousMoves == null)
                throw new RuntimeException(String.format("New broker %s has no available moves from current broker %s", move.to, fromBroker));

            if (!previousMoves.remove(move))
                throw new RuntimeException(String.format("Move for replica %s was not contained in move set from %s to %s", move.replica, fromBroker, move.to));

            if (previousMoves.isEmpty()) fromBrokerMoves.remove(move.to);
        }

        for (final MoveFrom move : toAdd) {
            if (fromBrokerMoves == null) {
                fromBrokerMoves = new HashMap<>();
                this.possibleMoves.put(fromBroker, fromBrokerMoves);
            }

            final Set<MoveFrom> previousMoves = fromBrokerMoves.computeIfAbsent(move.to, k -> new TreeSet<>(new MovableFromScoreComparator()));
            previousMoves.add(move);
        }

        if (fromBrokerMoves != null && fromBrokerMoves.isEmpty()) this.possibleMoves.remove(fromBroker);
    }

    private void updatePossibleMoves(Replica changedReplica, Set<MoveFrom> newReplicaPossibleMoves) {
        final Map<MoveFrom, MoveFrom> oldMoves = new HashMap<>();
        if (changedReplica.possibleMoves != null) {
            for (final MoveFrom move : changedReplica.possibleMoves) {
                oldMoves.put(move, move);
            }
        }
        final Map<MoveFrom, MoveFrom> newMoves = new HashMap<>();
        if (newReplicaPossibleMoves != null) {
            for (final MoveFrom move : newReplicaPossibleMoves) {
                newMoves.put(move, move);
            }
        }

        changedReplica.possibleMoves = newReplicaPossibleMoves;

        final List<MoveFrom> toRemove = new ArrayList<>();
        final List<MoveFrom> toAdd = new ArrayList<>();

        for (final Map.Entry<MoveFrom, MoveFrom> entry : newMoves.entrySet()) {
            final MoveFrom newMove = entry.getKey();
            final MoveFrom oldMove = oldMoves.get(newMove);

            if (oldMove == null) {
                toAdd.add(entry.getValue());
                continue;
            }

            oldMoves.remove(oldMove);

            if (oldMove.replica.moved != newMove.replica.moved || oldMove.rackImprovementScore != newMove.rackImprovementScore || oldMove.brokerImprovementScore != newMove.brokerImprovementScore) {
                toRemove.add(oldMove);
                toAdd.add(entry.getValue());
            }
        }

        for (final Map.Entry<MoveFrom, MoveFrom> entry : oldMoves.entrySet()) {
            toRemove.add(entry.getValue());
        }

        this.adjustGlobalPossibleMoves(changedReplica.currentBroker, toRemove, toAdd);
    }

    private void computeAndUpdatePossibleMoves(Replica replica) {
        final Set<MoveFrom> newPossibleMoves = this.getPossibleMovesRackAwareness(replica);

        this.updatePossibleMoves(replica, newPossibleMoves);
    }

    private void move(Move move) {
        this.sortedBrokersByAssignments.remove(move.from);
        this.sortedBrokersByAssignments.remove(move.to);

        this.brokerToAssignments.compute(move.from.id, (key, value) -> value == null || value <= 1 ? null : value - 1);
        this.brokerToAssignments.compute(move.to.id, (key, value) -> value == null ? 1 : value + 1);

        this.sortedBrokersByAssignments.add(move.from);
        this.sortedBrokersByAssignments.add(move.to);

        move.replica.id.partition.updatePartitionCounts(move);

        this.updatePossibleMoves(move.replica, null);

        move.replica.moved = true;
        move.replica.currentBroker = move.to;

        this.computeAndUpdatePossibleMoves(move.replica);

        for (final Replica replica : move.replica.id.partition.replicas) {
            if (replica.id.equals(move.replica.id)) continue;
            this.computeAndUpdatePossibleMoves(replica);
        }
    }

    private Move getBestMove() {
        final TreeSet<Move> bestMoves = new TreeSet<>(new MovableScoreComparator());

        final long currentEquilibriumScore = this.computeCurrentEquilibriumScore();

        final Iterator<BrokerMetadata> fromBrokerIterator = this.sortedBrokersByAssignments.descendingIterator();

        while (fromBrokerIterator.hasNext()) {
            final BrokerMetadata fromBroker = fromBrokerIterator.next();
            final Map<BrokerMetadata, TreeSet<MoveFrom>> fromBrokerMoves = this.possibleMoves.get(fromBroker);

            if (fromBrokerMoves == null) continue;

            final Iterator<BrokerMetadata> toBrokerIterator = this.sortedBrokersByAssignments.iterator();
            boolean consideredToBroker = false;

            while (toBrokerIterator.hasNext()) {
                final BrokerMetadata toBroker = toBrokerIterator.next();
                if (fromBroker.equals(toBroker)) continue;

                final long newEquilibriumScore = this.computeNewEquilibriumScore(fromBroker, toBroker);
                final long equilibriumImprovementScore = newEquilibriumScore - currentEquilibriumScore;

                if (equilibriumImprovementScore > 0) break;
                consideredToBroker = true;

                final TreeSet<MoveFrom> toBrokerMoves = fromBrokerMoves.get(toBroker);
                if (toBrokerMoves == null) continue;

                final MoveFrom moveFrom = getMoveFrom(toBrokerMoves, fromBroker, toBroker);

                // No move if no score changes
                if (moveFrom.brokerImprovementScore < 0 || moveFrom.rackImprovementScore < 0 || equilibriumImprovementScore < 0)
                    bestMoves.add(new Move(moveFrom.replica, fromBroker, toBroker, equilibriumImprovementScore, moveFrom.rackImprovementScore, moveFrom.brokerImprovementScore));
            }

            if (!consideredToBroker) break;
        }

        if (bestMoves.isEmpty()) return null;

        return bestMoves.first();
    }

    public Map<TopicPartition, List<Integer>> reassign() {
        do {
            final Move bestMove = this.getBestMove();
            if (bestMove == null) break;

            this.move(bestMove);
        } while (true);

        final Map<TopicPartition, List<Integer>> newState = new HashMap<>();

        for (final Partition partition : this.partitions) {
            final TopicPartition topicPartition = partition.id;
            final List<Integer> replicaAssignments = new ArrayList<>(partition.replicas.size());

            if (newState.put(topicPartition, replicaAssignments) != null)
                throw new RuntimeException(String.format("New state for partition %s already created", partition));

            for (int i = 0; i < partition.replicas.size(); i++) {
                final Replica replica = this.replicas.get(new ReplicaId(partition, i));
                if (replica == null)
                    throw new RuntimeException(String.format("Replica %s does not exist for partition %s", i, partition));
                replicaAssignments.add(replica.currentBroker.id);
            }
        }

        return newState;
    }

    private long computeCurrentEquilibriumScore() {
        long equilibriumScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            final int replicasOnBroker = this.brokerToAssignments.getOrDefault(broker.id, 0);
            equilibriumScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return equilibriumScore;
    }

    private long computeNewEquilibriumScore(BrokerMetadata from, BrokerMetadata to) {
        long equilibriumScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            int replicasOnBroker = this.brokerToAssignments.getOrDefault(broker.id, 0);
            if (broker.id == from.id) replicasOnBroker--;
            if (broker.id == to.id) replicasOnBroker++;

            equilibriumScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return equilibriumScore;
    }

    private record BrokerReplicaCountComparator(
            Map<Integer, Integer> assignments) implements Comparator<BrokerMetadata>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(BrokerMetadata o1, BrokerMetadata o2) {
            int ret = this.assignments.getOrDefault(o1.id, 0).compareTo(this.assignments.getOrDefault(o2.id, 0));
            if (ret != 0) return ret;
            return o1.id - o2.id;
        }
    }

    private static class MovableFromScoreComparator implements Comparator<MoveFrom>, Serializable {
        private static final long serialVersionUID = 1L;

        private int compareMovedAndImproved(MoveFrom o1, MoveFrom o2) {
            final int o1MovedAndImproved = o1.didImprove() && o1.replica.moved ? 0 : 1;
            final int o2MovedAndImproved = o2.didImprove() && o2.replica.moved ? 0 : 1;
            return o1MovedAndImproved - o2MovedAndImproved;
        }

        private int compareLongScores(long o1, long o2) {
            long ret = o1 - o2;
            if (ret < 0) return -1;
            if (ret > 0) return 1;
            return 0;
        }

        @Override
        public int compare(MoveFrom o1, MoveFrom o2) {
            int ret = compareMovedAndImproved(o1, o2);
            if (ret != 0) return ret;
            ret = compareLongScores(o1.rackImprovementScore, o2.rackImprovementScore);
            if (ret != 0) return ret;
            ret = compareLongScores(o1.brokerImprovementScore, o2.brokerImprovementScore);
            if (ret != 0) return ret;
            final int o1Moved = o1.replica.moved ? 0 : 1;
            final int o2Moved = o2.replica.moved ? 0 : 1;
            ret = o1Moved - o2Moved;
            if (ret != 0) return ret;
            ret = o1.to.id - o2.to.id;
            if (ret != 0) return ret;
            return o1.replica.compareTo(o2.replica);
        }
    }

    private static class MovableScoreComparator implements Comparator<Move>, Serializable {
        private static final long serialVersionUID = 1L;

        private int compareMovedAndImproved(Move o1, Move o2) {
            final int o1MovedAndImproved = o1.didImprove() && o1.replica.moved ? 0 : 1;
            final int o2MovedAndImproved = o2.didImprove() && o2.replica.moved ? 0 : 1;
            return o1MovedAndImproved - o2MovedAndImproved;
        }

        private int compareLongScores(long o1, long o2) {
            long ret = o1 - o2;
            if (ret < 0) return -1;
            if (ret > 0) return 1;
            return 0;
        }

        @Override
        public int compare(Move o1, Move o2) {
            int ret = compareMovedAndImproved(o1, o2);
            if (ret != 0) return ret;
            ret = compareLongScores(o1.equilibriumImprovementScore, o2.equilibriumImprovementScore);
            if (ret != 0) return ret;
            ret = compareLongScores(o1.rackImprovementScore, o2.rackImprovementScore);
            if (ret != 0) return ret;
            ret = compareLongScores(o1.brokerImprovementScore, o2.brokerImprovementScore);
            if (ret != 0) return ret;
            final int o1Moved = o1.replica.moved ? 0 : 1;
            final int o2Moved = o2.replica.moved ? 0 : 1;
            ret = o1Moved - o2Moved;
            if (ret != 0) return ret;
            ret = o1.to.id - o2.to.id;
            if (ret != 0) return ret;
            return o1.replica.compareTo(o2.replica);
        }
    }

    private record ReplicaId(Partition partition, int id) implements Comparable<ReplicaId> {
        @Override
        public String toString() {
            return "ReplicaId{" + "partition=" + partition + ", id=" + id + '}';
        }

        @Override
        public int compareTo(ReplicaId o) {
            int ret = this.partition.compareTo(o.partition);
            if (ret != 0) return ret;
            return this.id - o.id;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaId replicaId = (ReplicaId) o;
            return id == replicaId.id && Objects.equals(partition, replicaId.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, id);
        }
    }

    private static class Partition implements Comparable<Partition> {
        final TopicPartition id;

        final Set<Replica> replicas;

        final Map<Optional<String>, Integer> replicaCountPerRack;
        final Map<BrokerMetadata, Integer> replicaCountPerBroker;

        public Partition(TopicPartition id) {
            this.id = id;
            this.replicas = new HashSet<>();
            this.replicaCountPerRack = new HashMap<>();
            this.replicaCountPerBroker = new HashMap<>();
        }

        public void initPartitionCounts(Replica replica) {
            if (!replica.id.partition.equals(this))
                throw new RuntimeException(String.format("Replica %s does not belong to partition %s", replica.id, this));

            final BrokerMetadata newBroker = replica.currentBroker;
            final Optional<String> newRack = newBroker.rack;

            this.replicaCountPerRack.compute(newRack, (key, value) -> value == null ? 1 : value + 1);

            this.replicaCountPerBroker.compute(newBroker, (key, value) -> value == null ? 1 : value + 1);
        }

        public void updatePartitionCounts(Move move) {
            if (!move.replica.id.partition.equals(this))
                throw new RuntimeException(String.format("Replica %s does not belong to partition %s", move.replica.id, this));

            final BrokerMetadata previousBroker = move.from;
            final Optional<String> previousRack = previousBroker.rack;
            final BrokerMetadata newBroker = move.to;
            final Optional<String> newRack = newBroker.rack;

            this.replicaCountPerRack.compute(previousRack, (key, value) -> value == null || value <= 1 ? null : value - 1);
            this.replicaCountPerRack.compute(newRack, (key, value) -> value == null ? 1 : value + 1);

            this.replicaCountPerBroker.compute(previousBroker, (key, value) -> value == null || value <= 1 ? null : value - 1);
            this.replicaCountPerBroker.compute(newBroker, (key, value) -> value == null ? 1 : value + 1);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Partition partition = (Partition) o;
            return Objects.equals(id, partition.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(Partition o) {
            int ret = this.id.topic().compareTo(o.id.topic());
            if (ret != 0) return ret;
            return this.id.partition() - o.id.partition();
        }
    }

    private static class Replica implements Comparable<Replica> {

        final ReplicaId id;

        BrokerMetadata currentBroker;

        Set<MoveFrom> possibleMoves;
        boolean moved;

        public Replica(ReplicaId id, BrokerMetadata currentBroker) {
            this.id = id;
            this.currentBroker = currentBroker;
            this.moved = false;
            this.possibleMoves = null;
        }

        @Override
        public String toString() {
            return "Replica{" + "id=" + id + ", currentBroker=" + currentBroker + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Replica replica = (Replica) o;
            return Objects.equals(id, replica.id);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id);
        }

        @Override
        public int compareTo(Replica o) {
            return this.id.compareTo(o.id);
        }
    }

    private record MoveFrom(Replica replica, BrokerMetadata to, long rackImprovementScore,
                            long brokerImprovementScore) {
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            MoveFrom moveFrom = (MoveFrom) o;
            return Objects.equals(replica, moveFrom.replica) && Objects.equals(to, moveFrom.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replica, to);
        }

        public boolean didImprove() {
            return rackImprovementScore < 0 || brokerImprovementScore < 0;
        }
    }

    private record Move(Replica replica, BrokerMetadata from, BrokerMetadata to, long equilibriumImprovementScore,
                        long rackImprovementScore, long brokerImprovementScore) {

        public boolean didImprove() {
            return equilibriumImprovementScore < 0 || rackImprovementScore < 0 || brokerImprovementScore < 0;
        }
    }
}
