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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
class MetadataCache {
    private final String clusterId;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfo> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfo> partitions,
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

        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfo p : partitions) {
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        }

        if (clusterInstance == null) {
            updateClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    synchronized boolean removePartition(TopicPartition topicPartition) {
        if (partitionsByTopicPartition.remove(topicPartition) != null) {
            updateClusterView();
            return true;
        } else {
            return false;
        }
    }

    private void updateClusterView() {
        this.clusterInstance = new Cluster(clusterId, nodes, partitionsByTopicPartition.values(), unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyList(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "cluster=" + cluster() +
                '}';
    }
}
