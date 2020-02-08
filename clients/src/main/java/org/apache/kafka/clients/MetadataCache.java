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
import org.apache.kafka.common.requests.MetadataResponse;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private final Map<TopicPartition, MetadataResponse.PartitionMetadata> metadataByPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  Map<Integer, Node> nodes,
                  Collection<MetadataResponse.PartitionMetadata> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    private MetadataCache(String clusterId,
                          Map<Integer, Node> nodes,
                          Collection<MetadataResponse.PartitionMetadata> partitions,
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

        this.metadataByPartition = new HashMap<>(partitions.size());
        for (MetadataResponse.PartitionMetadata p : partitions) {
            this.metadataByPartition.put(p.topicPartition, p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    Optional<MetadataResponse.PartitionMetadata> partitionMetadata(TopicPartition topicPartition) {
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

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(metadata -> MetadataResponse.toPartitionInfo(metadata, nodes))
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
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyMap(), Collections.emptyList(),
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
