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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.MetadataResponse;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
public class MetadataCache {
    private final String clusterId;
    private final Map<Integer, Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, MetadataResponseData.MetadataResponsePartition> metadataByPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  Map<Integer, Node> nodes,
                  Map<TopicPartition, MetadataResponseData.MetadataResponsePartition> metadataByPartition,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, metadataByPartition, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    private MetadataCache(String clusterId,
                          Map<Integer, Node> nodes,
                          Map<TopicPartition, MetadataResponseData.MetadataResponsePartition> metadataByPartition,
                          Set<String> unauthorizedTopics,
                          Set<String> invalidTopics,
                          Set<String> internalTopics,
                          Node controller,
                          Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;

        this.metadataByPartition = metadataByPartition;

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Optional<MetadataResponseData.MetadataResponsePartition> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    /**
     * Merges the metadata cache's contents with the provided metadata, returning a new metadata cache. The provided
     * metadata is presumed to be more recent than the cache's metadata, and therefore all overlapping metadata will
     * be overridden.
     *
     * @param newClusterId the new cluster Id
     * @param newNodes the new set of nodes
     * @param addPartitions partitions to add
     * @param addUnauthorizedTopics unauthorized topics to add
     * @param addInternalTopics internal topics to add
     * @param newController the new controller node
     * @param retainTopic returns whether a topic's metadata should be retained
     * @return the merged metadata cache
     */
    MetadataCache mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Map<TopicPartition, MetadataResponseData.MetadataResponsePartition> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            BiPredicate<String, Boolean> retainTopic) {

        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));

        Map<TopicPartition, MetadataResponseData.MetadataResponsePartition> newMetadataByPartition = new HashMap<>(addPartitions.size());
        for (Map.Entry<TopicPartition, MetadataResponseData.MetadataResponsePartition> entry : addPartitions.entrySet()) {
            newMetadataByPartition.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<TopicPartition, MetadataResponseData.MetadataResponsePartition> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);

        return new MetadataCache(newClusterId, newNodes, newMetadataByPartition, newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController);
    }

    /**
     * Copies {@code baseSet} and adds all non-existent elements in {@code fillSet} such that {@code predicate} is true.
     * In other words, all elements of {@code baseSet} will be contained in the result, with additional non-overlapping
     * elements in {@code fillSet} where the predicate is true.
     *
     * @param baseSet the base elements for the resulting set
     * @param fillSet elements to be filled into the resulting set
     * @param predicate tested against the fill set to determine whether elements should be added to the base set
     */
    private static <T> Set<T> fillSet(Set<T> baseSet, Set<T> fillSet, Predicate<T> predicate) {
        Set<T> result = new HashSet<>(baseSet);
        for (T element : fillSet) {
            if (predicate.test(element)) {
                result.add(element);
            }
        }
        return result;
    }

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.entrySet()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata.getKey().topic(), metadata.getValue(), nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller);
    }

    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }
        return new MetadataCache(null, nodes, Collections.emptyMap(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}
