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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The class is a delegator of ReplicaPlacer to achieve pod isolation by placing partitions by broker pod
 * {@code podPartitionMap} defined all pods that need isolate certain partition into
 * for a partition that matches with any pod defined in {@code podPartitionMap}, the partition will be placed on that specific pod
 * otherwise the partition will be placed on all the other pods not defined in podPartitionMap
 *
 * special cases:
 * - A broker with empty pod will be treated as any pod
 * - A pod defined in podPartitionMap but not match any broker pod will be ignored
 */
public class PodReplicaPlacer implements ReplicaPlacer {
    private final ReplicaPlacer impl;
    private final Map<String, Predicate<Integer>> podPartitionMap;

    public PodReplicaPlacer(ReplicaPlacer impl, Map<String, Predicate<Integer>> podPartitionMap) {
        this.impl = impl;
        this.podPartitionMap = podPartitionMap;
    }

    public PodReplicaPlacer(ReplicaPlacer impl) {
        this(impl, Collections.emptyMap());
    }

    @Override
    public TopicAssignment place(PlacementSpec placement, ClusterDescriber cluster) throws InvalidReplicationFactorException {
        List<ClusterPlacement> clusterPlacements = calculateClusterPlacements(placement, cluster);
        List<PartitionAssignment> partitionAssignments = new ArrayList<>();
        for (ClusterPlacement clusterPlacement : clusterPlacements) {
            partitionAssignments.addAll(impl.place(clusterPlacement.placement, clusterPlacement.cluster).assignments());
        }
        return new TopicAssignment(partitionAssignments);
    }

    /**
     * calculate placement for each partition by broker pod to achieve partition isolation by pod
     * @param placement placement
     * @param cluster cluster
     * @return list of placement and cluster
     */
    private List<ClusterPlacement> calculateClusterPlacements(PlacementSpec placement, ClusterDescriber cluster) {
        List<ClusterPlacement> result = new ArrayList<>();
        Map<Set<String>, ClusterDescriber> podClusterCache = initCache(cluster);
        Set<String> allPods = podClusterCache.keySet()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        int lastStartPartition = -1;
        ClusterDescriber lastCluster = null;
        for (int i = 0; i < placement.numPartitions(); ++i) {
            int partition = placement.startPartition() + i;
            Set<String> pods = partitionToPods(partition, allPods);
            ClusterDescriber podCluster = clusterOfPods(pods, cluster, podClusterCache);
            if (lastStartPartition == -1 || lastCluster != podCluster) {
                if (lastStartPartition != -1) {
                    result.add(new ClusterPlacement(lastCluster, new PlacementSpec(lastStartPartition, partition - lastStartPartition, placement.numReplicas())));
                }
                lastStartPartition = partition;
                lastCluster = podCluster;
            }
        }
        if (lastCluster != null) {
            result.add(new ClusterPlacement(lastCluster, new PlacementSpec(lastStartPartition, placement.numPartitions()  + placement.startPartition() - lastStartPartition, placement.numReplicas())));
        }
        return result;
    }

    /**
     * evaluates pods with given partition
     * <p>
     * - if the partition matches any isolation rule of a pod, return the pod
     * - otherwise, return other pods
     * </p>
     * @param partition the partition to map from
     * @param pods pods of the whole cluster
     * @return pods of the partition
     */
    private Set<String> partitionToPods(int partition, Set<String> pods) {
        for (String pod : pods) {
            if (podPartitionMap.getOrDefault(pod, x -> false).test(partition))
                return Collections.singleton(pod);
        }
        // for partitions not match any pod, distribute to all other pods
        return pods.stream()
                .filter(pod -> !podPartitionMap.containsKey(pod))
                .collect(Collectors.toSet());
    }

    /**
     * Gets brokers for given pod set
     * @param pods list of pod
     * @param defaultCluster the full broker list
     * @param cache the initial pods to brokers map
     * @return brokers of given pods
     */
    private ClusterDescriber clusterOfPods(Set<String> pods, ClusterDescriber defaultCluster, Map<Set<String>, ClusterDescriber> cache) {
        if (cache.containsKey(pods)) {
            return cache.get(pods);
        }
        // if pods is empty but there is no broker with empty pod, fallback to full cluster
        if (pods.isEmpty()) {
            return defaultCluster;
        }

        List<ClusterDescriber> clusters = pods.stream()
                .map(Collections::singleton)
                .map(cache::get)
                .toList();

        Set<UsableBroker> brokers = new LinkedHashSet<>();
        for (ClusterDescriber cluster : clusters) {
            Iterator<UsableBroker> iter = cluster.usableBrokers();
            while (iter.hasNext()) {
                brokers.add(iter.next());
            }
        }

        ClusterDescriber result = new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return brokers.iterator();
            }

            @Override
            public Uuid defaultDir(int brokerId) {
                return defaultCluster.defaultDir(brokerId);
            }
        };

        cache.put(pods, result);
        return result;
    }

    /**
     * initialize mapping from pod set to brokers
     * @param cluster indicates the full broker set
     * @return pod set to brokers map
     */
    private Map<Set<String>, ClusterDescriber> initCache(ClusterDescriber cluster) {
        Map<Set<String>, Set<UsableBroker>> podBrokers = new HashMap<>();
        Iterator<UsableBroker> iter = cluster.usableBrokers();
        while (iter.hasNext()) {
            UsableBroker broker = iter.next();
            Optional<String> pod = broker.pod();
            podBrokers.computeIfAbsent(pod.map(Collections::singleton).orElse(Collections.emptySet()), k -> new LinkedHashSet<>()).add(broker);
        }

        // broker without pod can be treated as any pod
        Set<UsableBroker> podLessBrokers = podBrokers.get(Collections.emptySet());
        if (podLessBrokers != null) {
            for (Map.Entry<Set<String>, Set<UsableBroker>> entry : podBrokers.entrySet()) {
                if (!entry.getKey().isEmpty()) {
                    entry.getValue().addAll(podLessBrokers);
                }
            }
        }
        return podBrokers.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, // Keep the original key
                entry -> new ClusterDescriber() {
                    @Override
                    public Iterator<UsableBroker> usableBrokers() {
                        return entry.getValue().iterator();
                    }

                    @Override
                    public Uuid defaultDir(int brokerId) {
                        return cluster.defaultDir(brokerId);
                    }
                })
        );
    }

    private record ClusterPlacement(ClusterDescriber cluster, PlacementSpec placement) {
    }

}
