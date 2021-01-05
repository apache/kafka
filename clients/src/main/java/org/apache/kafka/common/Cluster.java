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
package org.apache.kafka.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * An immutable representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 */
public final class Cluster {

    private final boolean isBootstrapConfigured;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;
    private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
    private final Map<Integer, List<PartitionInfo>> partitionsByNode;
    private final Map<Integer, Node> nodesById;
    private final ClusterResource clusterResource;
    private final Map<String, Uuid> topicIds;

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null, Collections.emptyMap());
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller, Collections.emptyMap());
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, Collections.emptyMap());
    }

    /**
     * Create a new cluster with the given id, nodes, partitions and topicIds
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller,
                   Map<String, Uuid> topicIds) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, topicIds);
    }

    private Cluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> invalidTopics,
                    Set<String> internalTopics,
                    Node controller,
                    Map<String, Uuid> topicIds) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        this.clusterResource = new ClusterResource(clusterId);
        // make a randomized, unmodifiable copy of the nodes
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);

        // Index the nodes for quick lookup
        Map<Integer, Node> tmpNodesById = new HashMap<>();
        Map<Integer, List<PartitionInfo>> tmpPartitionsByNode = new HashMap<>(nodes.size());
        for (Node node : nodes) {
            tmpNodesById.put(node.id(), node);
            // Populate the map here to make it easy to add the partitions per node efficiently when iterating over
            // the partitions
            tmpPartitionsByNode.put(node.id(), new ArrayList<>());
        }
        this.nodesById = Collections.unmodifiableMap(tmpNodesById);

        // index the partition infos by topic, topic+partition, and node
        // note that this code is performance sensitive if there are a large number of partitions so we are careful
        // to avoid unnecessary work
        Map<TopicPartition, PartitionInfo> tmpPartitionsByTopicPartition = new HashMap<>(partitions.size());
        Map<String, List<PartitionInfo>> tmpPartitionsByTopic = new HashMap<>();
        for (PartitionInfo p : partitions) {
            tmpPartitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
            tmpPartitionsByTopic.computeIfAbsent(p.topic(), topic -> new ArrayList<>()).add(p);

            // The leader may not be known
            if (p.leader() == null || p.leader().isEmpty())
                continue;

            // If it is known, its node information should be available
            List<PartitionInfo> partitionsForNode = Objects.requireNonNull(tmpPartitionsByNode.get(p.leader().id()));
            partitionsForNode.add(p);
        }

        // Update the values of `tmpPartitionsByNode` to contain unmodifiable lists
        for (Map.Entry<Integer, List<PartitionInfo>> entry : tmpPartitionsByNode.entrySet()) {
            tmpPartitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }

        // Populate `tmpAvailablePartitionsByTopic` and update the values of `tmpPartitionsByTopic` to contain
        // unmodifiable lists
        Map<String, List<PartitionInfo>> tmpAvailablePartitionsByTopic = new HashMap<>(tmpPartitionsByTopic.size());
        for (Map.Entry<String, List<PartitionInfo>> entry : tmpPartitionsByTopic.entrySet()) {
            String topic = entry.getKey();
            List<PartitionInfo> partitionsForTopic = Collections.unmodifiableList(entry.getValue());
            tmpPartitionsByTopic.put(topic, partitionsForTopic);
            // Optimise for the common case where all partitions are available
            boolean foundUnavailablePartition = partitionsForTopic.stream().anyMatch(p -> p.leader() == null);
            List<PartitionInfo> availablePartitionsForTopic;
            if (foundUnavailablePartition) {
                availablePartitionsForTopic = new ArrayList<>(partitionsForTopic.size());
                for (PartitionInfo p : partitionsForTopic) {
                    if (p.leader() != null)
                        availablePartitionsForTopic.add(p);
                }
                availablePartitionsForTopic = Collections.unmodifiableList(availablePartitionsForTopic);
            } else {
                availablePartitionsForTopic = partitionsForTopic;
            }
            tmpAvailablePartitionsByTopic.put(topic, availablePartitionsForTopic);
        }

        this.partitionsByTopicPartition = Collections.unmodifiableMap(tmpPartitionsByTopicPartition);
        this.partitionsByTopic = Collections.unmodifiableMap(tmpPartitionsByTopic);
        this.availablePartitionsByTopic = Collections.unmodifiableMap(tmpAvailablePartitionsByTopic);
        this.partitionsByNode = Collections.unmodifiableMap(tmpPartitionsByNode);
        this.topicIds = Collections.unmodifiableMap(topicIds);

        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
            Collections.emptySet(), null);
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new Cluster(null, true, nodes, new ArrayList<>(0),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        combinedPartitions.putAll(partitions);
        return new Cluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
                new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.invalidTopics),
                new HashSet<>(this.internalTopics), this.controller);
    }

    /**
     * @return The known set of nodes
     */
    public List<Node> nodes() {
        return this.nodes;
    }

    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the node by node id if the replica for the given partition is online
     * @param partition
     * @param id
     * @return the node
     */
    public Optional<Node> nodeIfOnline(TopicPartition partition, int id) {
        Node node = nodeById(id);
        if (node != null && !Arrays.asList(partition(partition).offlineReplicas()).contains(node)) {
            return Optional.of(node);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return info.leader();
    }

    /**
     * Get the metadata for the specified partition
     * @param topicPartition The topic and partition to fetch info for
     * @return The metadata about the given topic and partition, or null if none is found
     */
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return partitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * Get the number of partitions for the given topic.
     * @param topic The topic to get the number of partitions for
     * @return The number of partitions or null if there is no corresponding metadata
     */
    public Integer partitionCountForTopic(String topic) {
        List<PartitionInfo> partitions = this.partitionsByTopic.get(topic);
        return partitions == null ? null : partitions.size();
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return availablePartitionsByTopic.getOrDefault(topic, Collections.emptyList());
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return partitionsByNode.getOrDefault(nodeId, Collections.emptyList());
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    public Set<String> topics() {
        return partitionsByTopic.keySet();
    }

    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }

    public Set<String> internalTopics() {
        return internalTopics;
    }

    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    public ClusterResource clusterResource() {
        return clusterResource;
    }

    public Node controller() {
        return controller;
    }

    public Collection<Uuid> topicIds() {
        return topicIds.values();
    }

    public Uuid topicId(String topic) {
        return topicIds.getOrDefault(topic, Uuid.ZERO_UUID);
    }

    @Override
    public String toString() {
        return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
            ", partitions = " + this.partitionsByTopicPartition.values() + ", controller = " + controller + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return isBootstrapConfigured == cluster.isBootstrapConfigured &&
                Objects.equals(nodes, cluster.nodes) &&
                Objects.equals(unauthorizedTopics, cluster.unauthorizedTopics) &&
                Objects.equals(invalidTopics, cluster.invalidTopics) &&
                Objects.equals(internalTopics, cluster.internalTopics) &&
                Objects.equals(controller, cluster.controller) &&
                Objects.equals(partitionsByTopicPartition, cluster.partitionsByTopicPartition) &&
                Objects.equals(clusterResource, cluster.clusterResource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isBootstrapConfigured, nodes, unauthorizedTopics, invalidTopics, internalTopics, controller,
                partitionsByTopicPartition, clusterResource);
    }
}
