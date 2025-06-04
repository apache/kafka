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
package org.apache.kafka.metadata;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.FinalizedFeatures;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface MetadataCache extends ConfigRepository {

    /**
     * Return topic metadata for a given set of topics and listener. See KafkaApis#handleTopicMetadataRequest for details
     * on the use of the two boolean flags.
     *
     * @param topics                      The set of topics.
     * @param listenerName                The listener name.
     * @param errorUnavailableEndpoints   If true, we return an error on unavailable brokers. This is used to support
     *                                    MetadataResponse version 0.
     * @param errorUnavailableListeners   If true, return LEADER_NOT_AVAILABLE if the listener is not found on the leader.
     *                                    This is used for MetadataResponse versions 0-5.
     * @return                            A collection of topic metadata.
     */
    List<MetadataResponseData.MetadataResponseTopic> getTopicMetadata(
        Set<String> topics,
        ListenerName listenerName,
        boolean errorUnavailableEndpoints,
        boolean errorUnavailableListeners);

    Set<String> getAllTopics();

    boolean hasAliveBroker(int brokerId);

    Optional<Long> getAliveBrokerEpoch(int brokerId);

    boolean isBrokerFenced(int brokerId);

    boolean isBrokerShuttingDown(int brokerId);

    Uuid getTopicId(String topicName);

    Optional<String> getTopicName(Uuid topicId);

    Optional<Node> getAliveBrokerNode(int brokerId, ListenerName listenerName);

    List<Node> getAliveBrokerNodes(ListenerName listenerName);

    List<Node> getBrokerNodes(ListenerName listenerName);

    Optional<LeaderAndIsr> getLeaderAndIsr(String topic, int partitionId);

    /**
     * Return the number of partitions in the given topic, or None if the given topic does not exist.
     */
    Optional<Integer> numPartitions(String topic);

    Map<Uuid, String> topicIdsToNames();

    Map<String, Uuid> topicNamesToIds();

    /**
     * Get a partition leader's endpoint
     *
     * @return  If the leader is known, and the listener name is available, return Some(node). If the leader is known,
     *          but the listener is unavailable, return Some(Node.NO_NODE). Otherwise, if the leader is not known,
     *          return None
     */
    Optional<Node> getPartitionLeaderEndpoint(String topic, int partitionId, ListenerName listenerName);

    Map<Integer, Node> getPartitionReplicaEndpoints(TopicPartition tp, ListenerName listenerName);

    boolean contains(String topic);

    boolean contains(TopicPartition tp);

    MetadataVersion metadataVersion();

    Optional<Integer> getRandomAliveBrokerId();

    FinalizedFeatures features();

    DescribeClientQuotasResponseData describeClientQuotas(DescribeClientQuotasRequestData request);

    DescribeUserScramCredentialsResponseData describeScramCredentials(DescribeUserScramCredentialsRequestData request);

    /**
     * Get the topic metadata for the given topics.
     *
     * The quota is used to limit the number of partitions to return. The NextTopicPartition field points to the first
     * partition can't be returned due the limit.
     * If a topic can't return any partition due to quota limit reached, this topic will not be included in the response.
     *
     * Note, the topics should be sorted in alphabetical order. The topics in the DescribeTopicPartitionsResponseData
     * will also be sorted in alphabetical order.
     *
     * @param topics                        The iterator of topics and their corresponding first partition id to fetch.
     * @param listenerName                  The listener name.
     * @param topicPartitionStartIndex      The start partition index for the first topic
     * @param maximumNumberOfPartitions     The max number of partitions to return.
     * @param ignoreTopicsWithExceptions    Whether ignore the topics with exception.
     */
    DescribeTopicPartitionsResponseData describeTopicResponse(
        Iterator<String> topics,
        ListenerName listenerName,
        Function<String, Integer> topicPartitionStartIndex,
        int maximumNumberOfPartitions,
        boolean ignoreTopicsWithExceptions);

    static Cluster toCluster(String clusterId, MetadataImage image) {
        Map<Integer, List<Node>> brokerToNodes = new HashMap<>();
        image.cluster().brokers().values().stream()
            .filter(broker -> !broker.fenced())
            .forEach(broker -> brokerToNodes.put(broker.id(), broker.nodes()));

        List<PartitionInfo> partitionInfos = new ArrayList<>();
        Set<String> internalTopics = new HashSet<>();

        image.topics().topicsByName().values().forEach(topic -> {
            topic.partitions().forEach((partitionId, partition) -> {
                List<Node> nodes = brokerToNodes.get(partition.leader);
                if (nodes != null) {
                    nodes.forEach(node -> {
                        partitionInfos.add(new PartitionInfo(
                            topic.name(),
                            partitionId,
                            node,
                            toArray(partition.replicas, brokerToNodes),
                            toArray(partition.isr, brokerToNodes),
                            getOfflineReplicas(image, partition).stream()
                                .map(brokerToNodes::get)
                                .flatMap(Collection::stream)
                                .toArray(Node[]::new)
                        ));
                    });
                    if (Topic.isInternal(topic.name())) {
                        internalTopics.add(topic.name());
                    }
                }
            });
        });

        Node controllerNode = Optional.ofNullable(brokerToNodes.get(getRandomAliveBroker(image).orElse(-1)))
            .map(nodes -> nodes.get(0))
            .orElse(Node.noNode());

        return new Cluster(
            clusterId,
            brokerToNodes.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
            partitionInfos,
            Collections.emptySet(),
            internalTopics,
            controllerNode
        );
    }

    private static Node[] toArray(int[] replicas, Map<Integer, List<Node>> brokerToNodes) {
        return Arrays.stream(replicas)
            .mapToObj(brokerToNodes::get)
            .flatMap(Collection::stream)
            .toArray(Node[]::new);
    }

    private static List<Integer> getOfflineReplicas(MetadataImage image, PartitionRegistration partition) {
        List<Integer> offlineReplicas = new ArrayList<>();
        for (int brokerId : partition.replicas) {
            BrokerRegistration broker = image.cluster().broker(brokerId);
            if (broker == null || isReplicaOffline(partition, broker)) {
                offlineReplicas.add(brokerId);
            }
        }
        return offlineReplicas;
    }

    private static boolean isReplicaOffline(PartitionRegistration partition, BrokerRegistration broker) {
        return broker.fenced() || !broker.hasOnlineDir(partition.directory(broker.id()));
    }

    private static Optional<Integer> getRandomAliveBroker(MetadataImage image) {
        List<Integer> aliveBrokers = image.cluster().brokers().values().stream()
            .filter(broker -> !broker.fenced())
            .map(BrokerRegistration::id)
            .toList();
        if (aliveBrokers.isEmpty()) return Optional.empty();
        return Optional.of(aliveBrokers.get(ThreadLocalRandom.current().nextInt(aliveBrokers.size())));
    }
}