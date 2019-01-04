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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultCluster implements Cluster, MutableCluster {

    private final boolean isBootstrapConfigured;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<Integer, Node> nodesById;
    private final ClusterResource clusterResource;

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public DefaultCluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, null);
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public DefaultCluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, Collections.emptySet(), internalTopics, controller);
    }

    /**
     * Create a new cluster with the given id, nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public DefaultCluster(String clusterId,
                   Collection<Node> nodes,
                   Collection<PartitionInfo> partitions,
                   Set<String> unauthorizedTopics,
                   Set<String> invalidTopics,
                   Set<String> internalTopics,
                   Node controller) {
        this(clusterId, false, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    private DefaultCluster(String clusterId,
                    boolean isBootstrapConfigured,
                    Collection<Node> nodes,
                    Collection<PartitionInfo> partitions,
                    Set<String> unauthorizedTopics,
                    Set<String> invalidTopics,
                    Set<String> internalTopics,
                    Node controller) {
        this.isBootstrapConfigured = isBootstrapConfigured;
        this.clusterResource = new ClusterResource(clusterId);
        // make a randomized, unmodifiable copy of the nodes
        List<Node> copy = new ArrayList<>(nodes);
        Collections.shuffle(copy);
        this.nodes = Collections.unmodifiableList(copy);
        this.nodesById = new HashMap<>();
        for (Node node : nodes)
            this.nodesById.put(node.id(), node);


        // index the partitions by topic/partition for quick lookup
        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfo p : partitions) {
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        }

        this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
        this.invalidTopics = Collections.unmodifiableSet(invalidTopics);
        this.internalTopics = Collections.unmodifiableSet(internalTopics);
        this.controller = controller;
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static DefaultCluster empty() {
        return new DefaultCluster(null, new ArrayList<>(0), new ArrayList<>(0), Collections.emptySet(),
                Collections.emptySet(), null);
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static DefaultCluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new DefaultCluster(null, true, nodes, new ArrayList<>(0),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
    }

    /**
     * Return a copy of this cluster combined with `partitions`.
     */
    @Override
    public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions) {
        Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
        combinedPartitions.putAll(partitions);
        return new DefaultCluster(clusterResource.clusterId(), this.nodes, combinedPartitions.values(),
                new HashSet<>(this.unauthorizedTopics), new HashSet<>(this.invalidTopics),
                new HashSet<>(this.internalTopics), this.controller);
    }

    /**
     * @return The known set of nodes
     */
    @Override
    public List<Node> nodes() {
        return this.nodes;
    }

    /**
     * Get the node by the node id (or null if no such node exists)
     * @param id The id of the node
     * @return The node, or null if no such node exists
     */
    @Override
    public Node nodeById(int id) {
        return this.nodesById.get(id);
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    @Override
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
    @Override
    public PartitionInfo partition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.get(topicPartition);
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    @Override
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return partitionsByTopicPartition.values().stream()
                .filter(info -> info.topic().equals(topic))
                .collect(Collectors.toList());
    }

    /**
     * Get the number of partitions for the given topic
     * @param topic The topic to get the number of partitions for
     * @return The number of partitions or null if there is no corresponding metadata
     */
    @Override
    public int partitionCountForTopic(String topic) {
        return partitionsForTopic(topic).size();
    }

    /**
     * Get the list of available partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    @Override
    public List<PartitionInfo> availablePartitionsForTopic(String topic) {
        return partitionsByTopicPartition.values().stream()
                .filter(info -> info.topic().equals(topic))
                .filter(info -> Objects.nonNull(info.leader()))
                .collect(Collectors.toList());
    }

    /**
     * Get the list of partitions whose leader is this node
     * @param nodeId The node id
     * @return A list of partitions
     */
    @Override
    public List<PartitionInfo> partitionsForNode(int nodeId) {
        return partitionsByTopicPartition.values().stream()
                .filter(info -> Objects.nonNull(info.leader()) && info.leader().id() == nodeId)
                .collect(Collectors.toList());
    }

    /**
     * Remove a partition from this cache.
     * @param topicPartition
     * @return
     */
    @Override
    public boolean removePartition(TopicPartition topicPartition) {
        return partitionsByTopicPartition.remove(topicPartition) != null;
    }

    /**
     * Get all topics.
     * @return a set of all topics
     */
    @Override
    public Set<String> topics() {
        return this.partitionsByTopicPartition.keySet().stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }

    @Override
    public Set<String> invalidTopics() {
        return invalidTopics;
    }

    @Override
    public Set<String> internalTopics() {
        return internalTopics;
    }

    @Override
    public boolean isBootstrapConfigured() {
        return isBootstrapConfigured;
    }

    @Override
    public ClusterResource clusterResource() {
        return clusterResource;
    }

    @Override
    public Node controller() {
        return controller;
    }

    @Override
    public String toString() {
        return "Cluster(id = " + clusterResource.clusterId() + ", nodes = " + this.nodes +
                ", partitions = " + this.partitionsByTopicPartition.values() + ", controller = " + controller + ")";
    }
}
