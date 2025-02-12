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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;


public class StickyPartitionReassignorUnitTest {

    private static List<BrokerMetadata> brokersFromBrokerLength(int brokersLength) {
        final List<BrokerMetadata> brokers = new ArrayList<>();
        for (int j = 0; j < brokersLength; j++) {
            brokers.add(new BrokerMetadata(j, Optional.empty()));
        }
        return brokers;
    }

    private static List<String> racksFromRacksLength(int racksLength) {
        final List<String> racks = new ArrayList<>();
        for (int i = 0; i < racksLength; i++) {
            racks.add(i, Integer.toString(i));
        }
        return racks;
    }

    private static Arbitrary<List<BrokerMetadata>> assignBrokersToRacks(List<String> racks, List<BrokerMetadata> brokers) {
        if (racks.size() > brokers.size())
            throw new RuntimeException("Can not represent each rack with available brokers");
        // Fast track
        if (racks.size() == brokers.size()) {
            return Arbitraries.shuffle(racks).map(shuffledRacks -> {
                final List<BrokerMetadata> newBrokers = new ArrayList<>();
                for (int i = 0; i < brokers.size(); i++) {
                    final String rack = racks.get(i);
                    final BrokerMetadata broker = brokers.get(i);
                    newBrokers.add(new BrokerMetadata(broker.id, Optional.of(rack)));
                }
                return newBrokers;
            });
        }

        final Arbitrary<String> rackArbitrary = Arbitraries.of(racks);

        return Arbitraries.shuffle(racks).flatMap(shuffledRacks -> {
            final List<Arbitrary<BrokerMetadata>> brokerArbitraries = new ArrayList<>();
            // Make sure that each rack is assigned to at least one broker
            for (int i = 0; i < shuffledRacks.size(); i++) {
                final String rack = shuffledRacks.get(i);
                final BrokerMetadata broker = brokers.get(i);
                brokerArbitraries.add(Arbitraries.just(new BrokerMetadata(broker.id, Optional.of(rack))));
            }
            // Assign all other brokers a randomly selected rack
            for (int i = shuffledRacks.size(); i < brokers.size(); i++) {
                final BrokerMetadata broker = brokers.get(i);
                brokerArbitraries.add(rackArbitrary.map(rack -> new BrokerMetadata(broker.id, Optional.of(rack))));
            }

            return Combinators.combine(brokerArbitraries).as(newBrokers -> newBrokers);
        });
    }

    private static Arbitrary<List<BrokerMetadata>> brokerMetadataNoRackAwareness() {
        return Arbitraries.integers().between(2, 20).map(StickyPartitionReassignorUnitTest::brokersFromBrokerLength);
    }

    private static Arbitrary<List<BrokerMetadata>> brokerMetadatas() {
        return Arbitraries.integers().between(2, 15).map(StickyPartitionReassignorUnitTest::racksFromRacksLength).flatMap(racks -> Arbitraries.integers().between(racks.size(), 20).map(StickyPartitionReassignorUnitTest::brokersFromBrokerLength).flatMap(brokers -> assignBrokersToRacks(racks, brokers)));
    }

    private static Arbitrary<List<BrokerMetadata>> brokerMetadatasEquallyDistributedRacks() {
        return Arbitraries.integers().between(2, 5).flatMap(racksLength -> Arbitraries.integers().between(1, 4).map(brokersPerRack -> {
            final int brokersLength = racksLength * brokersPerRack;
            final List<BrokerMetadata> brokers = new ArrayList<>();

            for (int i = 0; i < brokersLength; i++) {
                final int rack = i % racksLength;
                brokers.add(new BrokerMetadata(i, Optional.of(Integer.toString(rack))));
            }

            return brokers;
        }));
    }

    private static Arbitrary<List<Integer>> replicasFromBrokers(List<BrokerMetadata> brokers, int replicationFactor) {
        if (brokers.size() < replicationFactor)
            throw new RuntimeException("Replication factor is larger than available brokers");
        // Fast track
        if (brokers.size() == replicationFactor) {
            final List<Integer> replicas = new ArrayList<>();
            for (final BrokerMetadata broker : brokers) {
                replicas.add(broker.id);
            }
            return Arbitraries.shuffle(replicas);
        }

        return Arbitraries.shuffle(brokers).flatMap(shuffledBrokers -> {
            final List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < replicationFactor; i++) {
                final BrokerMetadata broker = shuffledBrokers.get(i);
                replicas.add(broker.id);
            }
            return Arbitraries.shuffle(replicas);
        });
    }

    private static Arbitrary<List<List<Integer>>> generatePartitionReplicasFixedReplicationFactor(List<BrokerMetadata> brokers, int replicationFactor) {
        return replicasFromBrokers(brokers, replicationFactor).list().ofMinSize(1).ofMaxSize(16);
    }

    private static Arbitrary<List<List<Integer>>> generatePartitionReplicas(List<BrokerMetadata> brokers) {
        if (brokers.isEmpty()) throw new RuntimeException("No brokers to assign partitions to");
        return Arbitraries.integers().between(1, brokers.size()).flatMap(replicationFactor -> generatePartitionReplicasFixedReplicationFactor(brokers, replicationFactor));
    }

    private static Arbitrary<Map<TopicPartition, List<Integer>>> generateAssignments(List<BrokerMetadata> brokers) {
        return generatePartitionReplicas(brokers).list().ofMinSize(1).ofMaxSize(100).map(topics -> {
            final Map<TopicPartition, List<Integer>> map = new HashMap<>();

            for (int t = 0; t < topics.size(); t++) {
                // Names do not matter to the reassignment, so we are just stringifying the index
                final String topic = Integer.toString(t);
                final List<List<Integer>> partitions = topics.get(t);

                for (int p = 0; p < partitions.size(); p++) {
                    final TopicPartition topicPartition = new TopicPartition(topic, p);
                    final List<Integer> replicas = new ArrayList<>(partitions.get(p));
                    map.put(topicPartition, replicas);
                }
            }

            return map;
        });
    }

    private static Arbitrary<ClusterMetadata> metadataForBrokers(Arbitrary<List<BrokerMetadata>> brokerArbitrary) {
        return brokerArbitrary.flatMap(brokers -> generateAssignments(brokers).map(assignments -> new ClusterMetadata(brokers, assignments)));
    }

    private static Map<Integer, BrokerMetadata> brokerMap(List<BrokerMetadata> brokers) {
        final Map<Integer, BrokerMetadata> brokerMap = new HashMap<>();
        for (final BrokerMetadata broker : brokers) {
            brokerMap.put(broker.id, broker);
        }
        return brokerMap;
    }

    private static Map<Integer, Integer> replicaCountPerBroker(Map<TopicPartition, List<Integer>> assignments) {
        final Map<Integer, Integer> replicaCountPerBroker = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<Integer>> entry : assignments.entrySet()) {
            for (final Integer nodeId : entry.getValue()) {
                replicaCountPerBroker.compute(nodeId, (key, value) -> value == null ? 1 : value + 1);
            }
        }
        return replicaCountPerBroker;
    }

    private static Set<Optional<String>> racks(List<BrokerMetadata> brokers) {
        final Set<Optional<String>> racks = new HashSet<>();
        for (final BrokerMetadata broker : brokers) {
            racks.add(broker.rack);
        }
        return racks;
    }

    private static long topicPartitionRackScore(Map<Integer, BrokerMetadata> brokers, Set<Optional<String>> racks, List<Integer> assignments) {
        final Map<Optional<String>, Integer> countPerRacks = new HashMap<>();
        for (final Integer nodeId : assignments) {
            final BrokerMetadata broker = brokers.get(nodeId);
            countPerRacks.compute(broker.rack, (key, value) -> value == null ? 1 : value + 1);
        }

        long score = 0;
        for (final Optional<String> rack : racks) {
            final Integer countForRack = countPerRacks.getOrDefault(rack, 0);
            score += (long) Math.pow(countForRack, 2);
        }
        return score;
    }

    private static long topicPartitionBrokerScore(Map<Integer, BrokerMetadata> brokers, List<Integer> assignments) {
        final Map<Integer, Integer> countPerBroker = new HashMap<>();
        for (final Integer nodeId : assignments) {
            countPerBroker.compute(nodeId, (key, value) -> value == null ? 1 : value + 1);
        }

        long score = 0;
        for (final Map.Entry<Integer, BrokerMetadata> entry : brokers.entrySet()) {
            final Integer countForBroker = countPerBroker.getOrDefault(entry.getKey(), 0);
            score += (long) Math.pow(countForBroker, 2);
        }
        return score;
    }

    private static BigInteger globalBrokerScore(Map<Integer, BrokerMetadata> brokers, Map<TopicPartition, List<Integer>> assignments) {
        final Map<Integer, Integer> countPerBroker = replicaCountPerBroker(assignments);

        BigInteger score = BigInteger.ZERO;
        for (final Map.Entry<Integer, BrokerMetadata> entry : brokers.entrySet()) {
            final long countForBroker = (long) countPerBroker.getOrDefault(entry.getKey(), 0);
            final BigInteger brokerScore = BigInteger.valueOf(countForBroker).pow(2);
            score = score.add(brokerScore);
        }
        return score;
    }

    private static int countMoves(Map<TopicPartition, List<Integer>> previous, Map<TopicPartition, List<Integer>> current) {
        int moves = 0;
        for (final Map.Entry<TopicPartition, List<Integer>> previousEntry : previous.entrySet()) {
            final List<Integer> previousReplicas = previousEntry.getValue();
            final List<Integer> currentReplicas = current.get(previousEntry.getKey());
            if (currentReplicas == null || previousReplicas.size() != currentReplicas.size())
                throw new RuntimeException();

            for (int i = 0; i < previousReplicas.size(); i++) {
                final Integer previousNodeId = previousReplicas.get(i);
                final Integer currentNodeId = currentReplicas.get(i);
                if (previousNodeId == null || currentNodeId == null) throw new RuntimeException();
                if (!previousNodeId.equals(currentNodeId)) moves++;
            }
        }
        return moves;
    }

    @Provide
    private Arbitrary<ClusterMetadata> metadataRackAware() {
        return metadataForBrokers(brokerMetadatas());
    }

    @Provide
    private Arbitrary<ClusterMetadata> metadataRackAwareBrokersEquallyDistributed() {
        return metadataForBrokers(brokerMetadatasEquallyDistributedRacks());
    }

    @Provide
    private Arbitrary<ClusterMetadata> metadataNoRackAwareness() {
        return metadataForBrokers(brokerMetadataNoRackAwareness());
    }

    @Provide
    private Arbitrary<ClusterMetadata> anyMetadata() {
        return Arbitraries.oneOf(metadataNoRackAwareness(), metadataRackAware());
    }


    @Property(tries = 10_000)
    void topicPartitionRackDistributionIsNotWorseThanBefore(@ForAll("metadataRackAware") ClusterMetadata metadata) {
        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        final Map<Integer, BrokerMetadata> brokers = brokerMap(metadata.brokers);
        final Set<Optional<String>> racks = racks(metadata.brokers);

        Assertions.assertEquals(metadata.assignments.size(), newPartitionAssignments.size());

        for (final Map.Entry<TopicPartition, List<Integer>> currentEntry : metadata.assignments.entrySet()) {
            final TopicPartition topicPartition = currentEntry.getKey();
            final List<Integer> currentAssignments = currentEntry.getValue();
            final List<Integer> newAssignments = newPartitionAssignments.get(topicPartition);

            Assertions.assertNotNull(newAssignments);
            Assertions.assertEquals(currentAssignments.size(), newAssignments.size());

            final long currentRackScore = topicPartitionRackScore(brokers, racks, currentAssignments);
            final long newRackScore = topicPartitionRackScore(brokers, racks, newAssignments);

            Assertions.assertTrue(newRackScore <= currentRackScore);
        }
    }

    @Property(tries = 10_000)
    void topicPartitionBrokerDistributionIsNotWorseThanBefore(@ForAll("anyMetadata") ClusterMetadata metadata) {
        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        final Map<Integer, BrokerMetadata> brokers = brokerMap(metadata.brokers);

        assertThat(newPartitionAssignments.size(), is(equalTo(metadata.assignments.size())));

        for (final Map.Entry<TopicPartition, List<Integer>> currentEntry : metadata.assignments.entrySet()) {
            final TopicPartition topicPartition = currentEntry.getKey();
            final List<Integer> currentAssignments = currentEntry.getValue();
            final List<Integer> newAssignments = newPartitionAssignments.get(topicPartition);

            assertThat(newAssignments, is(notNullValue()));
            assertThat(newAssignments.size(), is(equalTo(currentAssignments.size())));

            final long currentBrokerScore = topicPartitionBrokerScore(brokers, currentAssignments);
            final long newBrokerScore = topicPartitionBrokerScore(brokers, newAssignments);

            assertThat(newBrokerScore, is(lessThanOrEqualTo(currentBrokerScore)));
        }
    }

    @Property(tries = 10_000)
    void globalBrokerDistributionIsNotWorseThanBefore(@ForAll("anyMetadata") ClusterMetadata metadata) {
        final Map<Integer, BrokerMetadata> brokers = brokerMap(metadata.brokers);
        final BigInteger currentDistributionScore = globalBrokerScore(brokers, metadata.assignments);

        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        assertThat(newPartitionAssignments.size(), is(equalTo(metadata.assignments.size())));

        final BigInteger newDistributionScore = globalBrokerScore(brokers, newPartitionAssignments);

        assertThat(newDistributionScore, is(lessThanOrEqualTo(currentDistributionScore)));
    }


    @Property(tries = 10_000)
    void reachesEquilibriumIfBrokersAreEquallySpreadAcrossRacks(@ForAll("metadataRackAwareBrokersEquallyDistributed") ClusterMetadata metadata) {
        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        final Map<Integer, Integer> newCountPerBroker = replicaCountPerBroker(newPartitionAssignments);
        Integer newMinCount = null;
        Integer newMaxCount = null;
        for (final BrokerMetadata broker : metadata.brokers) {
            final int brokerReplicaCount = newCountPerBroker.getOrDefault(broker.id, 0);
            if (newMinCount == null || newMinCount > brokerReplicaCount) newMinCount = brokerReplicaCount;
            if (newMaxCount == null || newMaxCount < brokerReplicaCount) newMaxCount = brokerReplicaCount;
        }

        assertThat(newPartitionAssignments.size(), is(equalTo(metadata.assignments.size())));

        Assertions.assertNotNull(newMaxCount);
        Assertions.assertNotNull(newMinCount);

        assertThat(newMinCount + 1, is(greaterThanOrEqualTo(newMaxCount)));
    }

    @Property(tries = 10_000)
    void reachesEquilibriumIfRacksAreNotSet(@ForAll("metadataNoRackAwareness") ClusterMetadata metadata) {
        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        final Map<Integer, Integer> newCountPerBroker = replicaCountPerBroker(newPartitionAssignments);
        Integer newMinCount = null;
        Integer newMaxCount = null;
        for (final BrokerMetadata broker : metadata.brokers) {
            final int brokerReplicaCount = newCountPerBroker.getOrDefault(broker.id, 0);
            if (newMinCount == null || newMinCount > brokerReplicaCount) newMinCount = brokerReplicaCount;
            if (newMaxCount == null || newMaxCount < brokerReplicaCount) newMaxCount = brokerReplicaCount;
        }

        assertThat(newPartitionAssignments.size(), is(equalTo(metadata.assignments.size())));

        assertThat(newMaxCount, is(notNullValue()));
        assertThat(newMinCount, is(notNullValue()));

        assertThat(newMinCount + 1, is(greaterThanOrEqualTo(newMaxCount)));
    }

    @Property(tries = 10_000)
    void reachesBetterGlobalBrokerDistributionOrRequiresAtLeastSameMoveCountForSameGlobalBrokerDistribution(@ForAll("anyMetadata") ClusterMetadata metadata) {
        final Map<Integer, BrokerMetadata> brokers = brokerMap(metadata.brokers);

        final Map<TopicPartition, List<Integer>> oldPartitionAssignments = ReassignPartitionsCommand.calculateAssignment(metadata.assignments, metadata.brokers);
        final int oldMovesCount = countMoves(metadata.assignments, oldPartitionAssignments);
        final BigInteger oldDistributionScore = globalBrokerScore(brokers, oldPartitionAssignments);

        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();
        final int newMovesCount = countMoves(metadata.assignments, newPartitionAssignments);
        final BigInteger newDistributionScore = globalBrokerScore(brokers, newPartitionAssignments);

        assertThat(newPartitionAssignments.size(), is(equalTo(metadata.assignments.size())));

        final int globalBrokerScoreComparison = newDistributionScore.compareTo(oldDistributionScore);

        if (globalBrokerScoreComparison == 0) {
            // If the distributions are the same, make sure that at most as many moves are used as with the old algorithm
            assertThat(newMovesCount, is(lessThanOrEqualTo(oldMovesCount)));
        } else {
            assertThat(globalBrokerScoreComparison, is(lessThan(0)));
        }
    }

    private record ClusterMetadata(List<BrokerMetadata> brokers, Map<TopicPartition, List<Integer>> assignments) {
    }
}
