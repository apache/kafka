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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PodReplicaPlacerTest {

    @Test
    public void testDefaultPlaceWithEmptyCluster() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //place on all brokers with empty pods
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, Collections.EMPTY_LIST);
        Assertions.assertEquals(0, assignment.assignments().size());
    }

    @Test
    public void testDefaultPlaceWithEmptyPods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //place on all brokers with empty pods
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, List.of(
                        new UsableBroker(1, Optional.empty(), Optional.empty(), true),
                        new UsableBroker(2, Optional.empty(), Optional.empty(), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testAddPartitionDefaultPlaceWithEmptyPods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //place on all brokers with empty pods
        TopicAssignment assignment = place(placer, 4, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), Optional.empty(), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testDefaultPlaceWithTwoPods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //there is no broker with empty pod, fallback to all brokers
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("a"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("b"), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testAddPartitionDefaultPlaceWithTwoPods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //there is no broker with empty pod, fallback to all brokers
        TopicAssignment assignment = place(placer, 4, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("a"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("b"), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testDefaultPlaceWithPartialPods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer());
        //there is one broker with empty pod, place all replica to the empty pod
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("a"), true),
                new UsableBroker(2, Optional.empty(), Optional.empty(), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testPodIsolationPlaceWithEmptyPod() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("pod0", i -> i == 0));
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), Optional.empty(), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }


    @Test
    public void testAddPartitionPodIsolationPlaceWithEmptyPod() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("pod0", i -> i == 4));
        TopicAssignment assignment = place(placer, 4, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), Optional.empty(), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testPodIsolationPlaceWithPartialPod() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("pod0", i -> i == 0));
        TopicAssignment assignment = place(placer, 0, 4, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.empty(), true),
                new UsableBroker(2, Optional.empty(), Optional.of("pod0"), true)));
        Assertions.assertEquals(4, assignment.assignments().size());
        Assertions.assertEquals(2, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(3).replicas().get(0));
    }

    @Test
    public void testPodIsolationPlaceWithMultiplePods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("pod0", i -> i == 0));
        TopicAssignment assignment = place(placer, 0, 8, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("pod1"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("pod2"), true),
                new UsableBroker(3, Optional.empty(), Optional.of("pod3"), true),
                new UsableBroker(4, Optional.empty(), Optional.of("pod0"), true)));
        Assertions.assertEquals(8, assignment.assignments().size());
        Assertions.assertEquals(4, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(3).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(4).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(5).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(6).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(7).replicas().get(0));
    }

    @Test
    public void testAddPartitionPodIsolationPlaceWithMultiplePods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("pod0", i -> i == 8));
        TopicAssignment assignment = place(placer, 8, 8, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("pod1"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("pod2"), true),
                new UsableBroker(3, Optional.empty(), Optional.of("pod3"), true),
                new UsableBroker(4, Optional.empty(), Optional.of("pod0"), true)));
        Assertions.assertEquals(8, assignment.assignments().size());
        Assertions.assertEquals(4, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(3).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(4).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(5).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(6).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(7).replicas().get(0));
    }

    @Test
    public void testInvalidPodIsolationPlaceWithMultiplePods() {
        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), Collections.singletonMap("foo", i -> i == 0));
        TopicAssignment assignment = place(placer, 0, 8, (short) 1, List.of(
                new UsableBroker(1, Optional.empty(), Optional.of("pod1"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("pod2"), true),
                new UsableBroker(3, Optional.empty(), Optional.of("pod3"), true),
                new UsableBroker(4, Optional.empty(), Optional.of("pod0"), true)));
        Assertions.assertEquals(8, assignment.assignments().size());
        Assertions.assertEquals(1, assignment.assignments().get(0).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(1).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(2).replicas().get(0));
        Assertions.assertEquals(4, assignment.assignments().get(3).replicas().get(0));
        Assertions.assertEquals(1, assignment.assignments().get(4).replicas().get(0));
        Assertions.assertEquals(2, assignment.assignments().get(5).replicas().get(0));
        Assertions.assertEquals(3, assignment.assignments().get(6).replicas().get(0));
        Assertions.assertEquals(4, assignment.assignments().get(7).replicas().get(0));
    }

    @Test
    public void testOverlappingPodIsolationRulesShouldFail() {
        Map<String, java.util.function.Predicate<Integer>> rules = new HashMap<>();
        rules.put("pod0", i -> i == 0);
        rules.put("pod1", i -> i < 5);  // Also matches partition 0

        PodReplicaPlacer placer = new PodReplicaPlacer(new MockReplicaPlacer(), rules);

        IllegalStateException exception = Assertions.assertThrows(
            IllegalStateException.class,
            () -> place(placer, 0, 1, (short) 1, Arrays.asList(
                new UsableBroker(1, Optional.empty(), Optional.of("pod0"), true),
                new UsableBroker(2, Optional.empty(), Optional.of("pod1"), true)
            ))
        );

        Assertions.assertTrue(exception.getMessage().contains("Partition 0"));
        Assertions.assertTrue(exception.getMessage().contains("pod0"));
        Assertions.assertTrue(exception.getMessage().contains("pod1"));
        Assertions.assertTrue(exception.getMessage().contains("multiple pod isolation rules"));
    }

    @Test
    public void testPodIsolationWithMockedReplicaPlacer() {
        ReplicaPlacer mockReplicaPlacer = Mockito.mock(ReplicaPlacer.class);
        ArgumentCaptor<ClusterDescriber> clusterCaptor = ArgumentCaptor.forClass(ClusterDescriber.class);
        ArgumentCaptor<PlacementSpec> placementSpecCaptor = ArgumentCaptor.forClass(PlacementSpec.class);
        Mockito.when(mockReplicaPlacer.place(Mockito.any(PlacementSpec.class), Mockito.any(ClusterDescriber.class))).thenReturn(new TopicAssignment(Collections.EMPTY_LIST));
        PodReplicaPlacer placer = new PodReplicaPlacer(mockReplicaPlacer, Collections.singletonMap("pod0", i -> i == 0));
        place(placer, 0, 8, (short) 1, List.of(
                new UsableBroker(1, Optional.of("rack-1"), Optional.of("pod0"), false),
                new UsableBroker(2, Optional.of("rack-2"), Optional.of("pod0"), false),
                new UsableBroker(3, Optional.of("rack-3"), Optional.of("pod0"), false),
                new UsableBroker(4, Optional.of("rack-1"), Optional.of("pod1"), false),
                new UsableBroker(5, Optional.of("rack-2"), Optional.of("pod2"), false),
                new UsableBroker(6, Optional.of("rack-3"), Optional.empty(), false)));
        Mockito.verify(mockReplicaPlacer, Mockito.times(2)).place(placementSpecCaptor.capture(), clusterCaptor.capture());
        List<PlacementSpec> placementSpecs = placementSpecCaptor.getAllValues();
        List<ClusterDescriber> clusters = clusterCaptor.getAllValues();
        Assertions.assertEquals(2, placementSpecs.size());
        Assertions.assertEquals(2, clusters.size());
        Assertions.assertEquals(new PlacementSpec(0, 1, (short) 1), placementSpecs.get(0));
        Assertions.assertEquals(new PlacementSpec(1, 7, (short) 1), placementSpecs.get(1));
        Assertions.assertEquals(Set.of(1, 2, 3, 6), toList(clusters.get(0).usableBrokers())
                .stream()
                .map(UsableBroker::id)
                .collect(Collectors.toSet()));
        Assertions.assertEquals(Set.of(4, 5, 6), toList(clusters.get(1).usableBrokers())
                .stream()
                .map(UsableBroker::id)
                .collect(Collectors.toSet()));
    }

    @Test
    public void testAddPartitionPodIsolationWithMockedReplicaPlacer() {
        ReplicaPlacer mockReplicaPlacer = Mockito.mock(ReplicaPlacer.class);
        ArgumentCaptor<ClusterDescriber> clusterCaptor = ArgumentCaptor.forClass(ClusterDescriber.class);
        ArgumentCaptor<PlacementSpec> placementSpecCaptor = ArgumentCaptor.forClass(PlacementSpec.class);
        Mockito.when(mockReplicaPlacer.place(Mockito.any(PlacementSpec.class), Mockito.any(ClusterDescriber.class))).thenReturn(new TopicAssignment(Collections.EMPTY_LIST));
        PodReplicaPlacer placer = new PodReplicaPlacer(mockReplicaPlacer, Collections.singletonMap("pod0", i -> i == 8));
        place(placer, 8, 8, (short) 1, List.of(
                new UsableBroker(1, Optional.of("rack-1"), Optional.of("pod0"), false),
                new UsableBroker(2, Optional.of("rack-2"), Optional.of("pod0"), false),
                new UsableBroker(3, Optional.of("rack-3"), Optional.of("pod0"), false),
                new UsableBroker(4, Optional.of("rack-1"), Optional.of("pod1"), false),
                new UsableBroker(5, Optional.of("rack-2"), Optional.of("pod2"), false),
                new UsableBroker(6, Optional.of("rack-3"), Optional.empty(), false)));
        Mockito.verify(mockReplicaPlacer, Mockito.times(2)).place(placementSpecCaptor.capture(), clusterCaptor.capture());
        List<PlacementSpec> placementSpecs = placementSpecCaptor.getAllValues();
        List<ClusterDescriber> clusters = clusterCaptor.getAllValues();
        Assertions.assertEquals(2, placementSpecs.size());
        Assertions.assertEquals(2, clusters.size());
        Assertions.assertEquals(new PlacementSpec(8, 1, (short) 1), placementSpecs.get(0));
        Assertions.assertEquals(new PlacementSpec(9, 7, (short) 1), placementSpecs.get(1));
        Assertions.assertEquals(Set.of(1, 2, 3, 6), toList(clusters.get(0).usableBrokers())
                .stream()
                .map(UsableBroker::id)
                .collect(Collectors.toSet()));
        Assertions.assertEquals(Set.of(4, 5, 6), toList(clusters.get(1).usableBrokers())
                .stream()
                .map(UsableBroker::id)
                .collect(Collectors.toSet()));
    }

    private <C> List<C> toList(Iterator<C> iterator) {
        List<C> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    /**
     * Place partition round-robin
     */
    private class MockReplicaPlacer implements ReplicaPlacer {
        @Override
        public TopicAssignment place(PlacementSpec placement, ClusterDescriber cluster) throws InvalidReplicationFactorException {
            Iterator<UsableBroker> iterator = cluster.usableBrokers();
            List<PartitionAssignment> partitionAssignments = new ArrayList<>();
            if (!iterator.hasNext()) {
                return new TopicAssignment(partitionAssignments);
            }
            for (int i = 0; i < placement.numPartitions(); i++) {
                if (!iterator.hasNext()) {
                    iterator = cluster.usableBrokers();
                }
                List<Integer> replicas = new ArrayList<>();
                replicas.add(iterator.next().id());
                partitionAssignments.add(new PartitionAssignment(replicas, cluster));
            }

            return new TopicAssignment(partitionAssignments);
        }
    }

    private TopicAssignment place(
            ReplicaPlacer placer,
            int startPartition,
            int numPartitions,
            short replicationFactor,
            List<UsableBroker> brokers
    ) {
        PlacementSpec placementSpec = new PlacementSpec(startPartition,
                numPartitions,
                replicationFactor);
        return placer.place(placementSpec, new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return brokers.iterator();
            }

            @Override
            public Uuid defaultDir(int brokerId) {
                return DirectoryId.MIGRATING;
            }
        });
    }
}
