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

import java.util.OptionalInt;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;

import java.net.InetSocketAddress;
import java.util.Collection;
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
 * An internal immutable snapshot of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 * Prefer to extend MetadataSnapshot's API for internal client usage Vs the public {@link Cluster}
 */
public class MetadataSnapshot {
    private final String clusterId;
    private final Map<Integer, Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionMetadata> metadataByPartition;
    private final Map<String, Uuid> topicIds;
    private final Map<Uuid, String> topicNames;
    private Cluster clusterInstance;

    public MetadataSnapshot(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Map<String, Uuid> topicIds) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, topicIds, null);
    }

    // Visible for testing
    public MetadataSnapshot(String clusterId,
        Map<Integer, Node> nodes,
        Collection<PartitionMetadata> partitions,
        Set<String> unauthorizedTopics,
        Set<String> invalidTopics,
        Set<String> internalTopics,
        Node controller,
        Map<String, Uuid> topicIds,
        Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = Collections.unmodifiableMap(nodes);
        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
        this.topicIds = Collections.unmodifiableMap(topicIds);
        this.topicNames = Collections.unmodifiableMap(
            topicIds.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey))
        );

        Map<TopicPartition, PartitionMetadata> tmpMetadataByPartition = new HashMap<>(partitions.size());
        for (PartitionMetadata p : partitions) {
            tmpMetadataByPartition.put(p.topicPartition, p);
        }
        this.metadataByPartition = Collections.unmodifiableMap(tmpMetadataByPartition);

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Optional<PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition));
    }

    Map<String, Uuid> topicIds() {
        return topicIds;
    }

    Map<Uuid, String> topicNames() {
        return topicNames;
    }

    Optional<Node> nodeById(int id) {
        return Optional.ofNullable(nodes.get(id));
    }

    public Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    /**
     * Get leader-epoch for partition.
     *
     * @param tp partition
     * @return leader-epoch if known, else return OptionalInt.empty()
     */
    public OptionalInt leaderEpochFor(TopicPartition tp) {
        PartitionMetadata partitionMetadata = metadataByPartition.get(tp);
        if (partitionMetadata == null || !partitionMetadata.leaderEpoch.isPresent()) {
            return OptionalInt.empty();
        } else {
            return OptionalInt.of(partitionMetadata.leaderEpoch.get());
        }
    }

    ClusterResource clusterResource() {
        return new ClusterResource(clusterId);
    }

    /**
     * Merges the metadata snapshot's contents with the provided metadata, returning a new metadata snapshot. The provided
     * metadata is presumed to be more recent than the snapshot's metadata, and therefore all overlapping metadata will
     * be overridden.
     *
     * @param newClusterId the new cluster Id
     * @param newNodes the new set of nodes
     * @param addPartitions partitions to add
     * @param addUnauthorizedTopics unauthorized topics to add
     * @param addInternalTopics internal topics to add
     * @param newController the new controller node
     * @param addTopicIds the mapping from topic name to topic ID, for topics in addPartitions
     * @param retainTopic returns whether a pre-existing topic's metadata should be retained
     * @return the merged metadata snapshot
     */
    MetadataSnapshot mergeWith(String newClusterId,
                            Map<Integer, Node> newNodes,
                            Collection<PartitionMetadata> addPartitions,
                            Set<String> addUnauthorizedTopics,
                            Set<String> addInvalidTopics,
                            Set<String> addInternalTopics,
                            Node newController,
                            Map<String, Uuid> addTopicIds,
                            BiPredicate<String, Boolean> retainTopic) {

        Predicate<String> shouldRetainTopic = topic -> retainTopic.test(topic, internalTopics.contains(topic));

        Map<TopicPartition, PartitionMetadata> newMetadataByPartition = new HashMap<>(addPartitions.size());

        // We want the most recent topic ID. We start with the previous ID stored for retained topics and then
        // update with newest information from the MetadataResponse. We always take the latest state, removing existing
        // topic IDs if the latest state contains the topic name but not a topic ID.
        Map<String, Uuid> newTopicIds = this.topicIds.entrySet().stream()
                .filter(entry -> shouldRetainTopic.test(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        for (PartitionMetadata partition : addPartitions) {
            newMetadataByPartition.put(partition.topicPartition, partition);
            Uuid id = addTopicIds.get(partition.topic());
            if (id != null)
                newTopicIds.put(partition.topic(), id);
            else
                // Remove if the latest metadata does not have a topic ID
                newTopicIds.remove(partition.topic());
        }
        for (Map.Entry<TopicPartition, PartitionMetadata> entry : metadataByPartition.entrySet()) {
            if (shouldRetainTopic.test(entry.getKey().topic())) {
                newMetadataByPartition.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        Set<String> newUnauthorizedTopics = fillSet(addUnauthorizedTopics, unauthorizedTopics, shouldRetainTopic);
        Set<String> newInvalidTopics = fillSet(addInvalidTopics, invalidTopics, shouldRetainTopic);
        Set<String> newInternalTopics = fillSet(addInternalTopics, internalTopics, shouldRetainTopic);

        return new MetadataSnapshot(newClusterId, newNodes, newMetadataByPartition.values(), newUnauthorizedTopics,
                newInvalidTopics, newInternalTopics, newController, newTopicIds);
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
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes.values(), partitionInfos, unauthorizedTopics,
                invalidTopics, internalTopics, controller, topicIds);
    }

    static MetadataSnapshot bootstrap(List<InetSocketAddress> addresses) {
        Map<Integer, Node> nodes = new HashMap<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses) {
            nodes.put(nodeId, new Node(nodeId, address.getHostString(), address.getPort()));
            nodeId--;
        }
        return new MetadataSnapshot(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Collections.emptyMap(), Cluster.bootstrap(addresses));
    }

    static MetadataSnapshot empty() {
        return new MetadataSnapshot(null, Collections.emptyMap(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap(), Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataSnapshot{" +
                "clusterId='" + clusterId + '\'' +
                ", nodes=" + nodes +
                ", partitions=" + metadataByPartition.values() +
                ", controller=" + controller +
                '}';
    }

}
