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
import net.jqwik.api.Provide;
import net.jqwik.api.Property;
import net.jqwik.api.ForAll;
import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;


public class StickyPartitionReassignorUnitTest {
    private Arbitrary<List<BrokerMetadata>> brokerMetadataNoRackAwareness() {
        return Arbitraries.integers().between(2, 20).map(brokersLength -> {
            final List<BrokerMetadata> brokers = new ArrayList<>();
            for (int j = 0; j < brokersLength; j++) {
                brokers.add(new BrokerMetadata(j, Optional.empty()));
            }
            return brokers;
        });
    }

    private Arbitrary<List<BrokerMetadata>> brokerMetadatas() {
        return Arbitraries.integers().between(2, 15).flatMap(racksLength -> {
            final List<Optional<String>> racks = new ArrayList<>();

            for (int i = 0; i < racksLength; i++) {
                racks.add(i, Optional.of(Integer.toString(i)));
            }

            return Arbitraries.integers().between(racksLength, 20).flatMap(brokersLength -> {
                final List<BrokerMetadata> brokers = new ArrayList<>();

                for (int j = 0; j < racksLength; j++) {
                    brokers.add(new BrokerMetadata(j, racks.get(j)));
                }

                return Arbitraries.of(racks).list().ofSize(brokersLength - racksLength).map(racksForOtherBrokers -> {
                    final List<BrokerMetadata> allBrokers = new ArrayList<>(brokers);
                    for (int x = 0; x < racksForOtherBrokers.size(); x++) {
                        final int myId = brokersLength + x;
                        final Optional<String> rack = racksForOtherBrokers.get(x);
                        allBrokers.add(new BrokerMetadata(myId, rack));
                    }
                    return allBrokers;
                });
            });
        });
    }

    private Arbitrary<List<BrokerMetadata>> brokerMetadatasEquallyDistributedRacks() {
        return Arbitraries.integers().between(2, 15).flatMap(racksLength ->
            Arbitraries.integers().between(1,4).flatMap(brokersPerRack -> {
                final int brokersLength = racksLength * brokersPerRack;
                final List<Optional<String>> racksForBrokers = new ArrayList<>();

                for (int i = 0; i < brokersLength; i++) {
                    final int rack = i % brokersPerRack;
                    racksForBrokers.add(Optional.of(Integer.toString(rack)));
                }

                return Arbitraries.shuffle(racksForBrokers).map(racks -> {
                    final List<BrokerMetadata> brokers = new ArrayList<>();
                    int i = 0;
                    for (final Optional<String> rack : racks) {
                        brokers.add(new BrokerMetadata(i, rack));
                        i++;
                    }
                    return brokers;
                });
            }));
    }

    private Arbitrary<ClusterMetadata> metadataForBrokers(Arbitrary<List<BrokerMetadata>> brokerArbitrary) {
        return brokerArbitrary.flatMap(brokerMetadata -> {
            final int maxReplicationFactor = brokerMetadata.size();
            final List<Integer> nodeIds = brokerMetadata.stream().map(b -> b.id).toList();


            return Arbitraries.integers().between(1, 100).flatMap(topicNum -> Arbitraries.integers().between(1, maxReplicationFactor).flatMap(replicationFactor -> Arbitraries.integers().between(1, 25).flatMap(numPartitions -> Arbitraries.subsetOf(nodeIds).ofSize(replicationFactor).flatMap(replicas -> Arbitraries.shuffle(new ArrayList<>(replicas))).list().ofSize(numPartitions)).list().ofSize(topicNum))).map(topicPartitions -> {
                final Map<TopicPartition, List<Integer>> map = new HashMap<>();

                for (int t = 0; t < topicPartitions.size(); t++) {
                    final String topic = Integer.toString(t);
                    final List<List<Integer>> partitions = topicPartitions.get(t);

                    Integer size = null;

                    for (int p = 0; p < partitions.size(); p++) {
                        final TopicPartition topicPartition = new TopicPartition(topic, p);
                        final List<Integer> replicas = new ArrayList<>(partitions.get(p));
                        if (size == null) size = replicas.size();
                        else if (size != replicas.size())
                            throw new RuntimeException(String.format("Heeeelp %s %s", size, replicas.size()));
                        map.put(topicPartition, replicas);
                    }
                }

                return new ClusterMetadata(brokerMetadata, map);
            });
        });
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

    Map<Integer, BrokerMetadata> brokerMap(List<BrokerMetadata> brokers) {
        final Map<Integer, BrokerMetadata> brokerMap = new HashMap<>();
        for (final BrokerMetadata broker : brokers) {
            brokerMap.put(broker.id, broker);
        }
        return brokerMap;
    }

    Map<Integer, Integer> replicaCountPerBroker(Map<TopicPartition, List<Integer>> assignments) {
        final Map<Integer, Integer> replicaCountPerBroker = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<Integer>> entry : assignments.entrySet()) {
            for (final Integer nodeId : entry.getValue()) {
                replicaCountPerBroker.compute(nodeId, (key, value) -> value == null ? 1 : value + 1);
            }
        }
        return replicaCountPerBroker;
    }

    Set<Optional<String>> racks(List<BrokerMetadata> brokers) {
        final Set<Optional<String>> racks = new HashSet<>();
        for (final BrokerMetadata broker : brokers) {
            racks.add(broker.rack);
        }
        return racks;
    }

    long rackScore(Map<Integer, BrokerMetadata> brokers, Set<Optional<String>> racks, List<Integer> assignments) {
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

    long brokerScore(Map<Integer, BrokerMetadata> brokers, List<Integer> assignments) {
        final Map<Integer, Integer> countPerBroker = new HashMap<>();
        for (final Integer nodeId : assignments) {
            final BrokerMetadata broker = brokers.get(nodeId);
            countPerBroker.compute(nodeId, (key, value) -> value == null ? 1 : value + 1);
        }

        long score = 0;
        for (final Map.Entry<Integer, BrokerMetadata> entry : brokers.entrySet()) {
            final Integer countForBroker = countPerBroker.getOrDefault(entry.getKey(), 0);
            score += (long) Math.pow(countForBroker, 2);
        }
        return score;
    }

    int countMoves(Map<TopicPartition, List<Integer>> previous, Map<TopicPartition, List<Integer>> current) {
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

    @Property
    void rackDistributionForEachTopicPartitionIsSameOrBetterThanBefore(@ForAll("metadata") ClusterMetadata metadata) {
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

            final long currentRackScore = rackScore(brokers, racks, currentAssignments);
            final long newRackScore = rackScore(brokers, racks, newAssignments);

            Assertions.assertTrue(newRackScore <= currentRackScore);
        }
    }

    @Property
    void replicaDistributionAcrossBrokersIsSameOrBetterThanBefore(@ForAll("anyMetadata") ClusterMetadata metadata) {
        final Map<Integer, BrokerMetadata> brokers = brokerMap(metadata.brokers);

        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        Assertions.assertEquals(metadata.assignments.size(), newPartitionAssignments.size());

        for (final Map.Entry<TopicPartition, List<Integer>> currentEntry : metadata.assignments.entrySet()) {
            final TopicPartition topicPartition = currentEntry.getKey();
            final List<Integer> currentAssignments = currentEntry.getValue();
            final List<Integer> newAssignments = newPartitionAssignments.get(topicPartition);

            Assertions.assertNotNull(newAssignments);
            Assertions.assertEquals(currentAssignments.size(), newAssignments.size());

            final long currentBrokerScore = brokerScore(brokers, currentAssignments);
            final long newBrokerScore = brokerScore(brokers, newAssignments);

            Assertions.assertTrue(newBrokerScore <= currentBrokerScore);
        }
    }


    @Property
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

        Assertions.assertEquals(metadata.assignments.size(), newPartitionAssignments.size());

        Assertions.assertNotNull(newMaxCount);
        Assertions.assertNotNull(newMinCount);

        Assertions.assertTrue(newMinCount + 1 >= newMaxCount);
    }

    @Property
    void replicaDistributionAcrossBrokersPerPartitionIsSameOrBetterThanBefore(@ForAll("anyMetadata") ClusterMetadata metadata) {
        final Map<Integer, Integer> currentCountPerBroker = replicaCountPerBroker(metadata.assignments);
        long currentDistributionScore = 0;
        for (final BrokerMetadata broker : metadata.brokers) {
            currentDistributionScore += (long) Math.pow(currentCountPerBroker.getOrDefault(broker.id, 0), 2);
        }

        final StickyPartitionReassignor assignor = new StickyPartitionReassignor(metadata.assignments, metadata.brokers);
        final Map<TopicPartition, List<Integer>> newPartitionAssignments = assignor.reassign();

        final Map<Integer, Integer> newCountPerBroker = replicaCountPerBroker(newPartitionAssignments);
        long newDistributionScore = 0;
        for (final BrokerMetadata broker : metadata.brokers) {
            newDistributionScore += (long) Math.pow(newCountPerBroker.getOrDefault(broker.id, 0), 2);
        }
        Assertions.assertEquals(metadata.assignments.size(), newPartitionAssignments.size());

        Assertions.assertTrue(newDistributionScore <= currentDistributionScore);
    }

    @Property
    void createsPerfectDistributionIfNotRackAware(@ForAll("metadataNoRackAwareness") ClusterMetadata metadata) {
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

        Assertions.assertEquals(metadata.assignments.size(), newPartitionAssignments.size());

        Assertions.assertNotNull(newMaxCount);
        Assertions.assertNotNull(newMinCount);

        Assertions.assertTrue(newMinCount + 1 >= newMaxCount);
    }

    private record ClusterMetadata(List<BrokerMetadata> brokers,
                                          Map<TopicPartition, List<Integer>> assignments) {
    }
}
