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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer.BrokerList;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer.RackList;
import org.apache.kafka.server.util.MockRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class StripedReplicaPlacerTest {
    /**
     * Test that the BrokerList class works as expected.
     */
    @Test
    public void testBrokerList() {
        assertEquals(0, BrokerList.EMPTY.size());
        assertEquals(-1, BrokerList.EMPTY.next(1));
        BrokerList brokers = new BrokerList().add(0).add(1).add(2).add(3);
        assertEquals(4, brokers.size());
        assertEquals(0, brokers.next(0));
        assertEquals(1, brokers.next(0));
        assertEquals(2, brokers.next(0));
        assertEquals(3, brokers.next(0));
        assertEquals(-1, brokers.next(0));
        assertEquals(-1, brokers.next(0));
        assertEquals(1, brokers.next(1));
        assertEquals(2, brokers.next(1));
        assertEquals(3, brokers.next(1));
        assertEquals(0, brokers.next(1));
        assertEquals(-1, brokers.next(1));
    }

    /**
     * Test that we perform striped replica placement as expected, and don't use the
     * fenced replica if we don't have to.
     */
    @Test
    public void testAvoidFencedReplicaIfPossibleOnSingleRack() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), true),
            new UsableBroker(0, Optional.empty(), false),
            new UsableBroker(4, Optional.empty(), false),
            new UsableBroker(2, Optional.empty(), false)).iterator());
        assertEquals(5, rackList.numTotalBrokers());
        assertEquals(4, rackList.numUnfencedBrokers());
        assertEquals(Collections.singletonList(Optional.empty()), rackList.rackNames());
        assertThrows(InvalidReplicationFactorException.class, () -> rackList.place(0));
        assertThrows(InvalidReplicationFactorException.class, () -> rackList.place(-1));
        assertEquals(Arrays.asList(3, 4, 0, 2), rackList.place(4));
        assertEquals(Arrays.asList(4, 0, 2, 3), rackList.place(4));
        assertEquals(Arrays.asList(0, 2, 3, 4), rackList.place(4));
        assertEquals(Arrays.asList(2, 3, 4, 0), rackList.place(4));
        assertEquals(Arrays.asList(0, 4, 3, 2), rackList.place(4));
    }

    private TopicAssignment place(
        ReplicaPlacer placer,
        int startPartition,
        int numPartitions,
        short replicationFactor,
        List<UsableBroker> brokers,
        List<PartitionAssignment> existingPartitionAssignments
    ) {
        PlacementSpec placementSpec = new PlacementSpec("topic", startPartition,
            numPartitions,
            replicationFactor);
        ClusterDescriber cluster = new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return brokers.iterator();
            }
            @Override
            public List<PartitionAssignment> replicasForTopicName(String topicName) {
                return existingPartitionAssignments;
            }
        };
        return placer.place(placementSpec, cluster);
    }

    private TopicAssignment place(
        ReplicaPlacer placer,
        int startPartition,
        int numPartitions,
        short replicationFactor,
        List<UsableBroker> brokers
    ) {
        return place(placer, startPartition, numPartitions, replicationFactor, brokers, new ArrayList<>());
    }

    /**
     * Test that we perform striped replica placement as expected for a multi-partition topic
     * on a single unfenced broker
     */
    @Test
    public void testMultiPartitionTopicPlacementOnSingleUnfencedBroker() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals(new TopicAssignment(Arrays.asList(new PartitionAssignment(Arrays.asList(0)),
                new PartitionAssignment(Arrays.asList(0)),
                new PartitionAssignment(Arrays.asList(0)))),
                place(placer, 0, 3, (short) 1, Arrays.asList(
                        new UsableBroker(0, Optional.empty(), false),
                        new UsableBroker(1, Optional.empty(), true))));
    }

    /**
     * Test that we will place on the fenced replica if we need to.
     */
    @Test
    public void testPlacementOnFencedReplicaOnSingleRack() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), true),
            new UsableBroker(2, Optional.empty(), false)).iterator());
        assertEquals(3, rackList.numTotalBrokers());
        assertEquals(2, rackList.numUnfencedBrokers());
        assertEquals(Collections.singletonList(Optional.empty()), rackList.rackNames());
        assertEquals(Arrays.asList(3, 2, 1), rackList.place(3));
        assertEquals(Arrays.asList(2, 3, 1), rackList.place(3));
        assertEquals(Arrays.asList(3, 2, 1), rackList.place(3));
        assertEquals(Arrays.asList(2, 3, 1), rackList.place(3));
    }

    @Test
    public void testRackListWithMultipleRacks() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
            new UsableBroker(11, Optional.of("1"), false),
            new UsableBroker(10, Optional.of("1"), false),
            new UsableBroker(30, Optional.of("3"), false),
            new UsableBroker(31, Optional.of("3"), false),
            new UsableBroker(21, Optional.of("2"), false),
            new UsableBroker(20, Optional.of("2"), true)).iterator());
        assertEquals(6, rackList.numTotalBrokers());
        assertEquals(5, rackList.numUnfencedBrokers());
        assertEquals(Arrays.asList(Optional.of("1"), Optional.of("2"), Optional.of("3")), rackList.rackNames());
        assertEquals(Arrays.asList(11, 21, 31, 10), rackList.place(4));
        assertEquals(Arrays.asList(21, 30, 10, 20), rackList.place(4));
        assertEquals(Arrays.asList(31, 11, 21, 30), rackList.place(4));
    }

    @Test
    public void testPlaceShiftsInitialOffsetsRackUnaware() {
        List<UsableBroker> usableBrokers = Arrays.asList(
            new UsableBroker(0, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), false),
            new UsableBroker(2, Optional.empty(), false),
            new UsableBroker(3, Optional.empty(), false),
            new UsableBroker(4, Optional.empty(), false),
            new UsableBroker(5, Optional.empty(), false),
            new UsableBroker(6, Optional.empty(), false),
            new UsableBroker(7, Optional.empty(), false),
            new UsableBroker(8, Optional.empty(), false),
            new UsableBroker(9, Optional.empty(), false),
            new UsableBroker(10, Optional.empty(), false),
            new UsableBroker(11, Optional.empty(), false));

        List<PartitionAssignment> partitionAssignments = place(new StripedReplicaPlacer(new MockRandom()), 0, 2, (short) 3, usableBrokers).assignments();
        assertEquals(Arrays.asList(7, 8, 9), partitionAssignments.get(0).replicas());
        assertEquals(Arrays.asList(8, 9, 10), partitionAssignments.get(1).replicas());

        // Add two partitions
        int startPartitionId = partitionAssignments.size();
        List<PartitionAssignment> nextTwoPartitionAssignments = place(
                new StripedReplicaPlacer(new MockRandom()), startPartitionId, 2, (short) 3, usableBrokers, partitionAssignments).assignments();
        assertEquals(Arrays.asList(9, 10, 11), nextTwoPartitionAssignments.get(0).replicas());
        assertEquals(Arrays.asList(10, 11, 0), nextTwoPartitionAssignments.get(1).replicas());

        // Add four more partitions and make sure we handle starting from index 0 correctly.
        List<PartitionAssignment> currPartitionAssignments = new ArrayList<>(partitionAssignments);
        for (PartitionAssignment partitionAssignment : nextTwoPartitionAssignments) {
            currPartitionAssignments.add(partitionAssignment);
        }
        List<PartitionAssignment> nextFourPartitionAssignments = place(
                new StripedReplicaPlacer(new MockRandom()), startPartitionId, 4, (short) 3, usableBrokers, currPartitionAssignments).assignments();
        assertEquals(Arrays.asList(11, 0, 1), nextFourPartitionAssignments.get(0).replicas());
        assertEquals(Arrays.asList(0, 1, 2), nextFourPartitionAssignments.get(1).replicas());
        assertEquals(Arrays.asList(1, 2, 3), nextFourPartitionAssignments.get(2).replicas());
        assertEquals(Arrays.asList(2, 3, 4), nextFourPartitionAssignments.get(3).replicas());
    }

    @Test
    public void testPlaceShiftsInitialOffsetsRackAware() {
        List<UsableBroker> usableBrokers = Arrays.asList(
            new UsableBroker(0, Optional.of("1"), false),
            new UsableBroker(1, Optional.of("1"), false),
            new UsableBroker(2, Optional.of("1"), false),
            new UsableBroker(3, Optional.of("1"), false),
            new UsableBroker(4, Optional.of("2"), false),
            new UsableBroker(5, Optional.of("2"), false),
            new UsableBroker(6, Optional.of("2"), false),
            new UsableBroker(7, Optional.of("2"), false),
            new UsableBroker(8, Optional.of("3"), false),
            new UsableBroker(9, Optional.of("3"), false),
            new UsableBroker(10, Optional.of("3"), false),
            new UsableBroker(11, Optional.of("3"), false));

        List<PartitionAssignment> partitionAssignments = place(new StripedReplicaPlacer(new MockRandom()), 0, 2, (short) 3, usableBrokers).assignments();
        assertEquals(Arrays.asList(6, 8, 2), partitionAssignments.get(0).replicas());
        assertEquals(Arrays.asList(9, 3, 7), partitionAssignments.get(1).replicas());
        Set<Integer> assignedBrokers = new HashSet<>();

        assignedBrokers.addAll(partitionAssignments.get(0).replicas());
        assignedBrokers.addAll(partitionAssignments.get(1).replicas());

        // Add partitions
        int startPartitionId = partitionAssignments.size();
        List<PartitionAssignment> newPartitionAssignments = place(
            new StripedReplicaPlacer(new MockRandom()), startPartitionId, 2, (short) 3, usableBrokers, partitionAssignments).assignments();
        assertEquals(Arrays.asList(0, 4, 10), newPartitionAssignments.get(0).replicas());
        assertEquals(Arrays.asList(5, 11, 1), newPartitionAssignments.get(1).replicas());
        assignedBrokers.addAll(newPartitionAssignments.get(0).replicas());
        assignedBrokers.addAll(newPartitionAssignments.get(1).replicas());

        assertEquals(usableBrokers.size(), assignedBrokers.size());
    }

    @Test
    public void testPlaceShiftsInitialOffsetsRackAwareAndUsesUnusedRacks() {
        List<UsableBroker> usableBrokers = Arrays.asList(
            new UsableBroker(0, Optional.of("1"), false),
            new UsableBroker(1, Optional.of("2"), false),
            new UsableBroker(2, Optional.of("3"), false),
            new UsableBroker(3, Optional.of("4"), false));

        List<PartitionAssignment> partitionAssignments = place(new StripedReplicaPlacer(new MockRandom()), 0, 1, (short) 2, usableBrokers).assignments();
        assertEquals(Arrays.asList(0, 1), partitionAssignments.get(0).replicas());

        List<PartitionAssignment> fullPartitionAssignments = new ArrayList<>(partitionAssignments);

        // Add partitions
        int startPartitionId = partitionAssignments.size();
        partitionAssignments = place(
            new StripedReplicaPlacer(new MockRandom()), startPartitionId, 1, (short) 2, usableBrokers, partitionAssignments).assignments();
        assertEquals(Arrays.asList(1, 2), partitionAssignments.get(0).replicas());

        fullPartitionAssignments.addAll(partitionAssignments);

        partitionAssignments = place(
            new StripedReplicaPlacer(new MockRandom()), startPartitionId, 1, (short) 2, usableBrokers, partitionAssignments).assignments();
        assertEquals(Arrays.asList(2, 3), partitionAssignments.get(0).replicas());

        fullPartitionAssignments.addAll(partitionAssignments);

        partitionAssignments = place(
            new StripedReplicaPlacer(new MockRandom()), startPartitionId, 1, (short) 2, usableBrokers, partitionAssignments).assignments();
        assertEquals(Arrays.asList(3, 0), partitionAssignments.get(0).replicas());
    }

    @Test
    public void testRackListWithInvalidRacks() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
            new UsableBroker(11, Optional.of("1"), false),
            new UsableBroker(10, Optional.of("1"), false),
            new UsableBroker(30, Optional.of("3"), true),
            new UsableBroker(31, Optional.of("3"), true),
            new UsableBroker(20, Optional.of("2"), true),
            new UsableBroker(21, Optional.of("2"), true),
            new UsableBroker(41, Optional.of("4"), false),
            new UsableBroker(40, Optional.of("4"), true)).iterator());
        assertEquals(8, rackList.numTotalBrokers());
        assertEquals(3, rackList.numUnfencedBrokers());
        assertEquals(Arrays.asList(Optional.of("1"),
            Optional.of("2"),
            Optional.of("3"),
            Optional.of("4")), rackList.rackNames());
        assertEquals(Arrays.asList(41, 11, 21, 30), rackList.place(4));
        assertEquals(Arrays.asList(10, 20, 31, 41), rackList.place(4));
        assertEquals(Arrays.asList(41, 21, 30, 11), rackList.place(4));
    }

    @Test
    public void testTryGetPreviousBrokerOffset() {
        Map<Integer, Optional<String>> brokerIdToRack = new HashMap<>();
        brokerIdToRack.put(0, Optional.of("1"));
        brokerIdToRack.put(1, Optional.of("1"));
        brokerIdToRack.put(2, Optional.of("1"));
        brokerIdToRack.put(3, Optional.of("1"));
        brokerIdToRack.put(4, Optional.of("2"));
        brokerIdToRack.put(5, Optional.of("2"));
        brokerIdToRack.put(6, Optional.of("2"));
        brokerIdToRack.put(7, Optional.of("2"));
        brokerIdToRack.put(8, Optional.of("3"));
        brokerIdToRack.put(9, Optional.of("3"));
        brokerIdToRack.put(10, Optional.of("3"));
        brokerIdToRack.put(11, Optional.of("3"));

        List<Integer> unfencedBrokersRack1 = Arrays.asList(0, 1, 2, 3);
        List<Integer> unfencedBrokersRack2 = Arrays.asList(4, 5, 6, 7);
        List<Integer> unfencedBrokersRack3 = Arrays.asList(8, 9, 10, 11);

        // No existing partitions case.
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack,
                new ArrayList<>(), Optional.of("1"), unfencedBrokersRack1));

        // Case where existing partitions have broker from rack in last partition.
        assertEquals(Optional.of(1),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(0, 1, 2)),
                new PartitionAssignment(Arrays.asList(1, 2, 0))
            ), Optional.of("1"), unfencedBrokersRack1));

        // Case where existing partitions have broker from rack in last partition but brokers are fenced.
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(0, 1, 2)),
                new PartitionAssignment(Arrays.asList(1, 2, 0))
            ), Optional.of("1"), new ArrayList<>()));

        // Case where existing partitions have broker from rack in third to last partition.
        assertEquals(Optional.of(1),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(0, 1, 2)),
                new PartitionAssignment(Arrays.asList(1, 2, 0)),
                new PartitionAssignment(Arrays.asList(4, 5, 6)),
                new PartitionAssignment(Arrays.asList(5, 6, 7))
            ), Optional.of("1"), unfencedBrokersRack1));

        // Case where existing partitions have broker from rack in last partition but not first replica
        // in that partition.
        assertEquals(Optional.of(1),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(4, 5, 1))
            ), Optional.of("1"), unfencedBrokersRack1));

        // Case where existing partitions have broker from rack in last partition but not first replica
        // in that partition.
        assertEquals(Optional.of(1),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(1, 2, 0)),
                new PartitionAssignment(Arrays.asList(0, 5, 6)),
                new PartitionAssignment(Arrays.asList(0, 1, 2))
            ), Optional.of("2"), unfencedBrokersRack2));

        // Case where existing partitions do not have broker from rack.
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousBrokerOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(0, 1, 2)),
                new PartitionAssignment(Arrays.asList(1, 2, 0)),
                new PartitionAssignment(Arrays.asList(4, 5, 6)),
                new PartitionAssignment(Arrays.asList(5, 6, 7))
            ), Optional.of("3"), unfencedBrokersRack3));
    }

    @Test
    public void testTryGetPreviousRackOffset() {
        Map<Integer, Optional<String>> brokerIdToRack = new HashMap<>();
        brokerIdToRack.put(0, Optional.of("1"));
        brokerIdToRack.put(1, Optional.of("1"));
        brokerIdToRack.put(2, Optional.of("1"));
        brokerIdToRack.put(3, Optional.of("1"));
        brokerIdToRack.put(4, Optional.of("2"));
        brokerIdToRack.put(5, Optional.of("2"));
        brokerIdToRack.put(6, Optional.of("2"));
        brokerIdToRack.put(7, Optional.of("2"));
        brokerIdToRack.put(8, Optional.of("3"));
        brokerIdToRack.put(9, Optional.of("3"));
        brokerIdToRack.put(10, Optional.of("3"));
        brokerIdToRack.put(11, Optional.of("3"));

        List<Optional<String>> racks = Arrays.asList(Optional.of("1"), Optional.of("2"), Optional.of("3"));

        // No existing partitions case.
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousRackOffset(brokerIdToRack,
                new ArrayList<>(), racks));

        // No rack case
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousRackOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(6, 8, 2)),
                new PartitionAssignment(Arrays.asList(9, 3, 7))
            ), Arrays.asList(Optional.empty())));

        // Case where existing partitions have previous rack
        assertEquals(Optional.of(2),
            RackList.tryGetPreviousRackOffset(brokerIdToRack, Arrays.asList(
                new PartitionAssignment(Arrays.asList(6, 8, 2)),
                new PartitionAssignment(Arrays.asList(9, 3, 7))
            ), racks));

        // Case where last leader rack does not exist in brokerIdToRack Map.
        assertEquals(Optional.empty(),
            RackList.tryGetPreviousRackOffset(new HashMap<>(), Arrays.asList(
                new PartitionAssignment(Arrays.asList(6, 8, 2)),
                new PartitionAssignment(Arrays.asList(9, 3, 7))
            ), racks));

    }

    @Test
    public void testAllBrokersFenced() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("All brokers are currently fenced.",
            assertThrows(InvalidReplicationFactorException.class,
                () -> place(placer, 0, 1, (short) 1, Arrays.asList(
                    new UsableBroker(11, Optional.of("1"), true),
                    new UsableBroker(10, Optional.of("1"), true)))).getMessage());
    }

    @Test
    public void testNotEnoughBrokers() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("The target replication factor of 3 cannot be reached because only " +
            "2 broker(s) are registered.",
            assertThrows(InvalidReplicationFactorException.class,
                () -> place(placer, 0, 1, (short) 3, Arrays.asList(
                    new UsableBroker(11, Optional.of("1"), false),
                    new UsableBroker(10, Optional.of("1"), false)))).getMessage());
    }

    @Test
    public void testNonPositiveReplicationFactor() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals("Invalid replication factor 0: the replication factor must be positive.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> place(placer, 0, 1, (short) 0, Arrays.asList(
                                new UsableBroker(11, Optional.of("1"), false),
                                new UsableBroker(10, Optional.of("1"), false)))).getMessage());
    }

    @Test
    public void testSuccessfulPlacement() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        assertEquals(new TopicAssignment(Arrays.asList(new PartitionAssignment(Arrays.asList(2, 3, 0)),
                new PartitionAssignment(Arrays.asList(3, 0, 1)),
                new PartitionAssignment(Arrays.asList(0, 1, 2)),
                new PartitionAssignment(Arrays.asList(1, 2, 3)),
                new PartitionAssignment(Arrays.asList(1, 0, 2)))),
            place(placer, 0, 5, (short) 3, Arrays.asList(
                new UsableBroker(0, Optional.empty(), false),
                new UsableBroker(3, Optional.empty(), false),
                new UsableBroker(2, Optional.empty(), false),
                new UsableBroker(1, Optional.empty(), false))));
    }

    @Test
    public void testEvenDistribution() {
        MockRandom random = new MockRandom();
        StripedReplicaPlacer placer = new StripedReplicaPlacer(random);
        TopicAssignment topicAssignment = place(placer, 0, 200, (short) 2, Arrays.asList(
            new UsableBroker(0, Optional.empty(), false),
            new UsableBroker(1, Optional.empty(), false),
            new UsableBroker(2, Optional.empty(), false),
            new UsableBroker(3, Optional.empty(), false)));
        Map<List<Integer>, Integer> counts = new HashMap<>();
        for (PartitionAssignment partitionAssignment : topicAssignment.assignments()) {
            counts.put(partitionAssignment.replicas(), counts.getOrDefault(partitionAssignment.replicas(), 0) + 1);
        }
        assertEquals(14, counts.get(Arrays.asList(0, 1)));
        assertEquals(22, counts.get(Arrays.asList(0, 2)));
        assertEquals(14, counts.get(Arrays.asList(0, 3)));
        assertEquals(17, counts.get(Arrays.asList(1, 0)));
        assertEquals(17, counts.get(Arrays.asList(1, 2)));
        assertEquals(16, counts.get(Arrays.asList(1, 3)));
        assertEquals(13, counts.get(Arrays.asList(2, 0)));
        assertEquals(17, counts.get(Arrays.asList(2, 1)));
        assertEquals(20, counts.get(Arrays.asList(2, 3)));
        assertEquals(20, counts.get(Arrays.asList(3, 0)));
        assertEquals(19, counts.get(Arrays.asList(3, 1)));
        assertEquals(11, counts.get(Arrays.asList(3, 2)));
    }

    @Test
    public void testRackListAllBrokersFenced() {
        // ensure we can place N replicas on a rack when the rack has less than N brokers
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
                new UsableBroker(0, Optional.empty(), true),
                new UsableBroker(1, Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), true)).iterator());
        assertEquals(3, rackList.numTotalBrokers());
        assertEquals(0, rackList.numUnfencedBrokers());
        assertEquals(Collections.singletonList(Optional.empty()), rackList.rackNames());
        assertEquals("All brokers are currently fenced.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(3)).getMessage());
    }

    @Test
    public void testRackListNotEnoughBrokers() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
                new UsableBroker(11, Optional.of("1"), false),
                new UsableBroker(10, Optional.of("1"), false)).iterator());
        assertEquals("The target replication factor of 3 cannot be reached because only " +
                        "2 broker(s) are registered.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(3)).getMessage());
    }

    @Test
    public void testRackListNonPositiveReplicationFactor() {
        MockRandom random = new MockRandom();
        RackList rackList = new RackList(random, Arrays.asList(
                new UsableBroker(11, Optional.of("1"), false),
                new UsableBroker(10, Optional.of("1"), false)).iterator());
        assertEquals("Invalid replication factor -1: the replication factor must be positive.",
                assertThrows(InvalidReplicationFactorException.class,
                        () -> rackList.place(-1)).getMessage());
    }
}
