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
import java.math.BigInteger;
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
    private final boolean rackAware;


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
        this.rackAware = rackAwareBrokers > 0;

        this.replicas = new HashMap<>();
        this.partitions = new HashSet<>();

        this.initReplicas(currentAssignments, brokers);

        this.sortedBrokersByAssignments.addAll(brokerMetadatas);

        for (final Map.Entry<ReplicaId, Replica> entry : this.replicas.entrySet()) {
            final Replica replica = entry.getValue();
            this.computeAndUpdatePossibleMoves(replica);
        }
    }

    private static MoveFrom getBestMoveFrom(TreeSet<MoveFrom> toBrokerMoves, BrokerMetadata fromBroker, BrokerMetadata toBroker) {
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

    private long computeCurrentPartitionBrokerScore(Replica replica) {
        // The count per broker can only grow as large as replicationFactor
        // Using long arithmetic by squaring each count per broker should
        // be safe
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        long brokerScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            final int replicasOnBroker = replica.id.partition.replicaCountPerBroker.getOrDefault(broker, 0);
            brokerScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return brokerScore;
    }

    private long computeNewPartitionBrokerScore(Replica replica, BrokerMetadata to) {
        // The count per broker can only grow as large as replicationFactor
        // Using long arithmetic by squaring each count per broker should
        // be safe
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        long brokerScore = 0;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            int replicasOnBroker = replica.id.partition.replicaCountPerBroker.getOrDefault(broker, 0);
            // Assume that the move was executed and remove a replica from the from-broker count
            // and add a replica to the to-broker count
            if (broker.equals(replica.currentBroker)) replicasOnBroker--;
            if (broker.equals(to)) replicasOnBroker++;
            brokerScore += (long) Math.pow(replicasOnBroker, 2);
        }
        return brokerScore;
    }

    private long computeCurrentRackScore(Replica replica) {
        if (!this.rackAware) return 0L;

        // The count per broker can only grow as large as replicationFactor
        // Using long arithmetic by squaring each count per broker should
        // be safe
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        long rackScore = 0;
        for (final Optional<String> rack : this.racks) {
            final int replicasOnRack = replica.id.partition.replicaCountPerRack.getOrDefault(rack, 0);
            rackScore += (long) Math.pow(replicasOnRack, 2);
        }
        return rackScore;
    }

    private long computeNewRackScore(Replica replica, BrokerMetadata to) {
        if (!this.rackAware) return 0L;

        final Optional<String> currentRack = replica.currentBroker.rack;
        final Optional<String> newRack = to.rack;

        // The count per broker can only grow as large as replicationFactor
        // Using long arithmetic by squaring each count per broker should
        // be safe
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        long rackScore = 0;
        for (final Optional<String> rack : this.racks) {
            int replicasOnRack = replica.id.partition.replicaCountPerRack.getOrDefault(rack, 0);
            // Assume that the move was executed and remove a replica from the from-rack count
            // and add a replica to the to-rack count
            if (rack.equals(currentRack)) replicasOnRack--;
            if (rack.equals(newRack)) replicasOnRack++;

            rackScore += (long) Math.pow(replicasOnRack, 2);
        }
        return rackScore;
    }

    private Set<MoveFrom> getPossibleMovesForReplica(Replica replica) {
        final Set<MoveFrom> possibleMoves = new TreeSet<>(new MovableFromScoreComparator());

        final long currentRackScore = this.computeCurrentRackScore(replica);
        final long currentBrokerScore = this.computeCurrentPartitionBrokerScore(replica);

        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            if (replica.currentBroker.equals(broker)) continue;

            final long newBrokerScore = this.computeNewPartitionBrokerScore(replica, broker);
            final long brokerImprovementScore = newBrokerScore - currentBrokerScore;

            // Do not consider moves that make the broker distribution for the partition
            // worse than before
            if (brokerImprovementScore > 0) continue;

            final long newRackScore = this.computeNewRackScore(replica, broker);
            final long rackImprovementScore = newRackScore - currentRackScore;

            // Do not consider moves that make the rack distribution for the partition
            // worse than before
            if (rackImprovementScore > 0) continue;

            possibleMoves.add(new MoveFrom(replica, broker, rackImprovementScore, brokerImprovementScore));
        }

        return possibleMoves;
    }

    private void adjustGlobalPossibleMoves(BrokerMetadata fromBroker, List<MoveFrom> toRemove, List<MoveFrom> toAdd) {
        Map<BrokerMetadata, TreeSet<MoveFrom>> fromBrokerMoves = this.possibleMoves.get(fromBroker);

        // First remove all old possible moves from the global possible moves
        // Removing old moves first is required due to the invariants defined in
        // updatePossibleMoves
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
        // newReplicaPossibleMoves might contain the semantically same moves (same replica is moved to same broker)
        // but with changed improvement scores.
        // Since the global possibleMoves structures are ordered by improvement scores first, we might not be able
        // to find the old moves using the new moves

        // Construct an oldMoves map containing all previous possibleMoves of changedReplica using only the moves
        // identity (replica id and to-broker identity)
        final Map<MoveFrom, MoveFrom> oldMoves = new HashMap<>();
        if (changedReplica.possibleMoves != null) {
            for (final MoveFrom move : changedReplica.possibleMoves) {
                oldMoves.put(move, move);
            }
        }
        // Construct a newMoves map containing all new possibleMoves of changedReplica using only the moves
        // identity (replica id and to-broker identity)
        final Map<MoveFrom, MoveFrom> newMoves = new HashMap<>();
        if (newReplicaPossibleMoves != null) {
            for (final MoveFrom move : newReplicaPossibleMoves) {
                newMoves.put(move, move);
            }
        }

        changedReplica.possibleMoves = newReplicaPossibleMoves;

        // Contains all old moves that should be removed from the global possibleMoves
        final List<MoveFrom> toRemove = new ArrayList<>();
        // Contains all new moves that should be added to the global possibleMoves
        final List<MoveFrom> toAdd = new ArrayList<>();

        for (final Map.Entry<MoveFrom, MoveFrom> entry : newMoves.entrySet()) {
            final MoveFrom newMove = entry.getKey();
            final MoveFrom oldMove = oldMoves.get(newMove); // Get old move by identity

            // new move does not have a corresponding old move
            if (oldMove == null) {
                toAdd.add(entry.getValue());
                continue;
            }

            oldMoves.remove(oldMove);

            // If the old move and new move are the same by identity, check whether at least one of the scores changed
            // If so, make sure that the old move is first removed from the global possibleMoves and then the new move
            // is added
            if (oldMove.replica.moved != newMove.replica.moved || !Objects.equals(oldMove.rackImprovementScore, newMove.rackImprovementScore) || !Objects.equals(oldMove.brokerImprovementScore, newMove.brokerImprovementScore)) {
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
        final Set<MoveFrom> newPossibleMoves = this.getPossibleMovesForReplica(replica);

        this.updatePossibleMoves(replica, newPossibleMoves);
    }

    private void move(Move move) {
        // Remove broker nodes that take part in the move to update their assignment counts
        this.sortedBrokersByAssignments.remove(move.from);
        this.sortedBrokersByAssignments.remove(move.to);

        // Update assignment counts to state after move
        this.brokerToAssignments.compute(move.from.id, (key, value) -> value == null || value <= 1 ? null : value - 1);
        this.brokerToAssignments.compute(move.to.id, (key, value) -> value == null ? 1 : value + 1);

        // Re-add broker nodes
        this.sortedBrokersByAssignments.add(move.from);
        this.sortedBrokersByAssignments.add(move.to);

        // Update internal partition maps to state after move
        move.replica.id.partition.updatePartitionCounts(move);

        this.updatePossibleMoves(move.replica, null);

        // Update replica to state after move
        move.replica.moved = true;
        move.replica.currentBroker = move.to;

        this.computeAndUpdatePossibleMoves(move.replica);

        // Re-evaluate possible moves for each sibling replica
        // This is required since the partitions internal state has been changed,
        // so the improvement scores for each current possible move for each
        // sibling replica might have changed to
        for (final Replica replica : move.replica.id.partition.replicas) {
            if (replica.id.equals(move.replica.id)) continue;
            this.computeAndUpdatePossibleMoves(replica);
        }
    }

    private Move getBestMove() {
        final Comparator<Move> comparator = new MovableScoreComparator();
        Move bestMove = null;

        final BigInteger currentGlobalBrokerScore = this.computeCurrentGlobalBrokerScore();

        // Iterate through all brokers in descending order by replica assignment count
        // (broker with the most assignments first, broker with the least assignments last)
        final Iterator<BrokerMetadata> fromBrokerIterator = this.sortedBrokersByAssignments.descendingIterator();

        while (fromBrokerIterator.hasNext()) {
            final BrokerMetadata fromBroker = fromBrokerIterator.next();
            final Map<BrokerMetadata, TreeSet<MoveFrom>> fromBrokerMoves = this.possibleMoves.get(fromBroker);

            if (fromBrokerMoves == null) continue;

            // Iterate through all brokers in ascending order by replica assignment count
            // (broker with the least assignments first, broker with the most assignments last)
            final Iterator<BrokerMetadata> toBrokerIterator = this.sortedBrokersByAssignments.iterator();
            boolean globalBrokerDistributionCouldImprove = false;

            while (toBrokerIterator.hasNext()) {
                final BrokerMetadata toBroker = toBrokerIterator.next();
                if (fromBroker.equals(toBroker)) continue;

                final BigInteger newGlobalBrokerScore = this.computeNewGlobalBrokerScore(fromBroker, toBroker);
                final BigInteger globalBrokerImprovement = newGlobalBrokerScore.subtract(currentGlobalBrokerScore);
                final int globalBrokerImprovementToZero = globalBrokerImprovement.compareTo(BigInteger.ZERO);

                // Do not consider moves to brokers that would make the global broker distribution score worse
                if (globalBrokerImprovementToZero > 0) break;
                globalBrokerDistributionCouldImprove = true;

                final TreeSet<MoveFrom> toBrokerMoves = fromBrokerMoves.get(toBroker);
                if (toBrokerMoves == null) continue;

                // Only get the best move, since all other moves would have a lower priority in the global best
                // moves set anyway
                final MoveFrom moveFrom = getBestMoveFrom(toBrokerMoves, fromBroker, toBroker);

                // No move if no score changes
                if (moveFrom.brokerImprovementScore < 0 || moveFrom.rackImprovementScore < 0 || globalBrokerImprovementToZero < 0) {
                    final Move bestMoveForBrokerCombination = new Move(moveFrom.replica, fromBroker, toBroker, globalBrokerImprovement, moveFrom.rackImprovementScore, moveFrom.brokerImprovementScore);

                    // Use the best move that moves a replica from fromBroker to toBroker if we do not have a
                    // best move yet or if the move is better than the current best move
                    if (bestMove == null || comparator.compare(bestMoveForBrokerCombination, bestMove) < 0)
                        bestMove = bestMoveForBrokerCombination;
                }
            }

            // Since the fromBrokerIterator and toBrokerIterator are ordered (descending by replica count, ascending by replica count)
            // we can be sure that we will not find another from-broker once we checked a from-broker for which there is no other broker
            // for which moving a partition from the from-broker to the broker would improve the global broker distribution
            if (!globalBrokerDistributionCouldImprove) break;
        }

        return bestMove;
    }

    public Map<TopicPartition, List<Integer>> reassign() {
        do {
            // Choose a move to execute for as long as there exists a move that improves
            // the replica assignment
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

    private BigInteger computeCurrentGlobalBrokerScore() {
        // The count per broker can grow as large as the number of replicas in
        // the cluster.
        // Using long arithmetic as for partitionBrokerScore or rackScore
        // might not be safe for the globalBrokerScore.
        // Therefore, BigIntegers are used instead
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        BigInteger brokerScore = BigInteger.ZERO;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            final long replicasOnBroker = (long) this.brokerToAssignments.getOrDefault(broker.id, 0);
            final BigInteger thisScore = BigInteger.valueOf(replicasOnBroker).pow(2);
            brokerScore = brokerScore.add(thisScore);
        }
        return brokerScore;
    }

    private BigInteger computeNewGlobalBrokerScore(BrokerMetadata from, BrokerMetadata to) {
        // The count per broker can grow as large as the number of replicas in
        // the cluster.
        // Using long arithmetic as for partitionBrokerScore or rackScore
        // might not be safe for the globalBrokerScore.
        // Therefore, BigIntegers are used instead
        // Mathematically using just the square of the count is as good a
        // metric for ranking the distribution scores as the squared
        // residuals
        BigInteger brokerScore = BigInteger.ZERO;
        for (final BrokerMetadata broker : this.sortedBrokersByAssignments) {
            int replicasOnBroker = this.brokerToAssignments.getOrDefault(broker.id, 0);
            // Assume that the move was executed and remove a replica from the from-rack count
            // and add a replica to the to-rack count
            if (broker.id == from.id) replicasOnBroker--;
            if (broker.id == to.id) replicasOnBroker++;

            final BigInteger thisScore = BigInteger.valueOf(replicasOnBroker).pow(2);
            brokerScore = brokerScore.add(thisScore);
        }
        return brokerScore;
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

        @Override
        public int compare(MoveFrom o1, MoveFrom o2) {
            // Prefer already moved replicas that improve at least one score over unmoved replicas
            int ret = compareMovedAndImproved(o1, o2);
            if (ret != 0) return ret;
            ret = o1.rackImprovementScore.compareTo(o2.rackImprovementScore);
            if (ret != 0) return ret;
            ret = o1.brokerImprovementScore.compareTo(o2.brokerImprovementScore);
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

        @Override
        public int compare(Move o1, Move o2) {
            // Prefer already moved replicas that improve at least one score over unmoved replicas
            int ret = compareMovedAndImproved(o1, o2);
            if (ret != 0) return ret;
            ret = o1.equilibriumImprovementScore.compareTo(o2.equilibriumImprovementScore);
            if (ret != 0) return ret;
            ret = o1.rackImprovementScore.compareTo(o2.rackImprovementScore);
            if (ret != 0) return ret;
            ret = o1.brokerImprovementScore.compareTo(o2.brokerImprovementScore);
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

    private record MoveFrom(Replica replica, BrokerMetadata to, Long rackImprovementScore,
                            Long brokerImprovementScore) {
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

    private record Move(Replica replica, BrokerMetadata from, BrokerMetadata to, BigInteger equilibriumImprovementScore,
                        Long rackImprovementScore, Long brokerImprovementScore) {

        public boolean didImprove() {
            return equilibriumImprovementScore.compareTo(BigInteger.ZERO) < 0 || rackImprovementScore < 0 || brokerImprovementScore < 0;
        }
    }
}
