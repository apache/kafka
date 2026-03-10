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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DescribeClientQuotasRequestData;
import org.apache.kafka.common.message.DescribeClientQuotasResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.Cursor;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.server.common.FinalizedFeatures;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KRaftMetadataCache implements MetadataCache {
    private final Logger log;
    private final Supplier<KRaftVersion> kraftVersionSupplier;

    // This is the cache state. Every MetadataImage instance is immutable, and updates
    // replace this value with a completely new one. This means reads (which are not under
    // any lock) need to grab the value of this variable once, and retain that read copy for
    // the duration of their operation. Multiple reads of this value risk getting different
    // image values.
    private volatile MetadataImage currentImage = MetadataImage.EMPTY;

    public KRaftMetadataCache(int brokerId, Supplier<KRaftVersion> kraftVersionSupplier) {
        this.kraftVersionSupplier = kraftVersionSupplier;
        this.log = new LogContext("[MetadataCache brokerId=" + brokerId + "] ").logger(KRaftMetadataCache.class);
    }

    /**
     * Filter the alive replicas. It returns all brokers when filterUnavailableEndpoints is false.
     * Otherwise, it filters the brokers that are fenced or do not have the listener.
     * <p>
     * This method is the main hotspot when it comes to the performance of metadata requests,
     * we should be careful about adding additional logic here.
     * @param image                      The metadata image.
     * @param brokers                    The list of brokers to filter.
     * @param listenerName               The listener name.
     * @param filterUnavailableEndpoints Whether to filter the unavailable endpoints. This field is to support v0 MetadataResponse.
     */
    private List<Integer> maybeFilterAliveReplicas(
        MetadataImage image,
        int[] brokers,
        ListenerName listenerName,
        boolean filterUnavailableEndpoints
    ) {
        if (!filterUnavailableEndpoints) return Replicas.toList(brokers);
        List<Integer> res = new ArrayList<>(brokers.length);
        for (int brokerId : brokers) {
            BrokerRegistration broker = image.cluster().broker(brokerId);
            if (broker != null && !broker.fenced() && broker.listeners().containsKey(listenerName.value())) {
                res.add(brokerId);
            }
        }
        return res;
    }

    public MetadataImage currentImage() {
        return currentImage;
    }

    /**
     * Get the partition metadata for the given topic and listener. If errorUnavailableEndpoints is true,
     * it uses all brokers in the partitions. Otherwise, it filters the unavailable endpoints.
     * If errorUnavailableListeners is true, it returns LISTENER_NOT_FOUND if the listener is missing on the broker.
     * Otherwise, it returns LEADER_NOT_AVAILABLE for broker unavailable.
     *
     * @param image                     The metadata image.
     * @param topicName                 The name of the topic.
     * @param listenerName              The listener name.
     * @param errorUnavailableEndpoints Whether to filter the unavailable endpoints. This field is to support v0 MetadataResponse.
     * @param errorUnavailableListeners Whether to return LISTENER_NOT_FOUND or LEADER_NOT_AVAILABLE.
     */
    private List<MetadataResponsePartition> partitionMetadata(
        MetadataImage image,
        String topicName,
        ListenerName listenerName,
        boolean errorUnavailableEndpoints,
        boolean errorUnavailableListeners
    ) {
        TopicImage topicImage = image.topics().getTopic(topicName);
        if (topicImage == null) return List.of();
        return topicImage.partitions().entrySet().stream().map(entry -> {
            int partitionId = entry.getKey();
            PartitionRegistration partition = entry.getValue();
            List<Integer> filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas, listenerName, errorUnavailableEndpoints);
            List<Integer> filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName, errorUnavailableEndpoints);
            List<Integer> offlineReplicas = getOfflineReplicas(image, partition, listenerName);
            Optional<Node> maybeLeader = getAliveEndpoint(image, partition.leader, listenerName);
            Errors error;
            if (maybeLeader.isEmpty()) {
                if (!image.cluster().brokers().containsKey(partition.leader)) {
                    log.debug("Error while fetching metadata for {}-{}: leader not available", topicName, partitionId);
                    error = Errors.LEADER_NOT_AVAILABLE;
                } else {
                    log.debug("Error while fetching metadata for {}-{}: listener {} not found on leader {}", topicName, partitionId, listenerName, partition.leader);
                    error = errorUnavailableListeners ? Errors.LISTENER_NOT_FOUND : Errors.LEADER_NOT_AVAILABLE;
                }
                return new MetadataResponsePartition()
                    .setErrorCode(error.code())
                    .setPartitionIndex(partitionId)
                    .setLeaderId(MetadataResponse.NO_LEADER_ID)
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas);
            } else {
                if (filteredReplicas.size() < partition.replicas.length) {
                    log.debug("Error while fetching metadata for {}-{}: replica information not available for following brokers {}", topicName, partitionId, Arrays.stream(partition.replicas).filter(b -> !filteredReplicas.contains(b)).mapToObj(String::valueOf).collect(Collectors.joining(",")));
                    error = Errors.REPLICA_NOT_AVAILABLE;
                } else if (filteredIsr.size() < partition.isr.length) {
                    log.debug("Error while fetching metadata for {}-{}: in sync replica information not available for following brokers {}", topicName, partitionId, Arrays.stream(partition.isr).filter(b -> !filteredIsr.contains(b)).mapToObj(String::valueOf).collect(Collectors.joining(",")));
                    error = Errors.REPLICA_NOT_AVAILABLE;
                } else {
                    error = Errors.NONE;
                }
                return new MetadataResponsePartition()
                    .setErrorCode(error.code())
                    .setPartitionIndex(partitionId)
                    .setLeaderId(maybeLeader.get().id())
                    .setLeaderEpoch(partition.leaderEpoch)
                    .setReplicaNodes(filteredReplicas)
                    .setIsrNodes(filteredIsr)
                    .setOfflineReplicas(offlineReplicas);
            }
        }).toList();
    }

    /**
     * Return topic partition metadata for the given topic, listener and index range. Also, return the next partition
     * index that is not included in the result.
     *
     * @param image                       The metadata image
     * @param topicName                   The name of the topic.
     * @param listenerName                The listener name.
     * @param startIndex                  The smallest index of the partitions to be included in the result.
     *
     * @return                            A collection of topic partition metadata and next partition index (-1 means
     *                                    no next partition).
     */
    private Entry<Optional<List<DescribeTopicPartitionsResponsePartition>>, Integer> partitionMetadataForDescribeTopicResponse(
        MetadataImage image,
        String topicName,
        ListenerName listenerName,
        int startIndex,
        int maxCount
    ) {
        TopicImage topic = image.topics().getTopic(topicName);
        if (topic == null) return Map.entry(Optional.empty(), -1);
        List<DescribeTopicPartitionsResponsePartition> result = new ArrayList<>();
        final Set<Integer> partitions = topic.partitions().keySet();
        final int upperIndex = Math.min(topic.partitions().size(), startIndex + maxCount);
        for (int partitionId = startIndex; partitionId < upperIndex; partitionId++) {
            PartitionRegistration partition = topic.partitions().get(partitionId);
            if (partition == null) {
                log.warn("The partition {} does not exist for {}", partitionId, topicName);
                continue;
            }
            List<Integer> filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas, listenerName, false);
            List<Integer> filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName, false);
            List<Integer> offlineReplicas = getOfflineReplicas(image, partition, listenerName);
            Optional<Node> maybeLeader = getAliveEndpoint(image, partition.leader, listenerName);
            result.add(new DescribeTopicPartitionsResponsePartition()
                .setPartitionIndex(partitionId)
                .setLeaderId(maybeLeader.map(Node::id).orElse(MetadataResponse.NO_LEADER_ID))
                .setLeaderEpoch(partition.leaderEpoch)
                .setReplicaNodes(filteredReplicas)
                .setIsrNodes(filteredIsr)
                .setOfflineReplicas(offlineReplicas)
                .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                .setLastKnownElr(Replicas.toList(partition.lastKnownElr)));
        }
        return Map.entry(Optional.of(result), (upperIndex < partitions.size()) ? upperIndex : -1);
    }

    private List<Integer> getOfflineReplicas(MetadataImage image, PartitionRegistration partition, ListenerName listenerName) {
        List<Integer> offlineReplicas = new ArrayList<>(0);
        for (int brokerId : partition.replicas) {
            BrokerRegistration broker = image.cluster().broker(brokerId);
            if (broker == null || isReplicaOffline(partition, listenerName, broker)) {
                offlineReplicas.add(brokerId);
            }
        }
        return offlineReplicas;
    }

    private boolean isReplicaOffline(PartitionRegistration partition, ListenerName listenerName, BrokerRegistration broker) {
        return broker.fenced() || !broker.listeners().containsKey(listenerName.value()) || isReplicaInOfflineDir(broker, partition);
    }

    private boolean isReplicaInOfflineDir(BrokerRegistration broker, PartitionRegistration partition) {
        return !broker.hasOnlineDir(partition.directory(broker.id()));
    }

    /**
     * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
     * be added dynamically, so a broker with a missing listener could be a transient error.
     *
     * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
     */
    private Optional<Node> getAliveEndpoint(MetadataImage image, int id, ListenerName listenerName) {
        return image.cluster().broker(id) == null ? Optional.empty() :
            image.cluster().broker(id).node(listenerName.value());
    }

    @Override
    public List<MetadataResponseTopic> getTopicMetadata(
        Set<String> topics,
        ListenerName listenerName,
        boolean errorUnavailableEndpoints,
        boolean errorUnavailableListeners
    ) {
        MetadataImage image = currentImage;
        return topics.stream().flatMap(topic -> {
            List<MetadataResponsePartition> partitions = partitionMetadata(image, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners);
            if (partitions.isEmpty()) return Stream.empty();
            return Stream.of(new MetadataResponseTopic()
                .setErrorCode(Errors.NONE.code())
                .setName(topic)
                .setTopicId(image.topics().getTopic(topic) == null ? Uuid.ZERO_UUID : image.topics().getTopic(topic).id())
                .setIsInternal(Topic.isInternal(topic))
                .setPartitions(partitions));
        }).toList();
    }

    @Override
    public DescribeTopicPartitionsResponseData describeTopicResponse(
        Iterator<String> topics,
        ListenerName listenerName,
        Function<String, Integer> topicPartitionStartIndex,
        int maximumNumberOfPartitions,
        boolean ignoreTopicsWithExceptions
    ) {
        MetadataImage image = currentImage;
        AtomicInteger remaining = new AtomicInteger(maximumNumberOfPartitions);
        DescribeTopicPartitionsResponseData result = new DescribeTopicPartitionsResponseData();
        while (topics.hasNext()) {
            String topicName = topics.next();
            if (remaining.get() > 0) {
                var partitionResponseEntry = partitionMetadataForDescribeTopicResponse(image, topicName, listenerName, topicPartitionStartIndex.apply(topicName), remaining.get());
                var partitionResponse = partitionResponseEntry.getKey();
                int nextPartition = partitionResponseEntry.getValue();
                if (partitionResponse.isPresent()) {
                    List<DescribeTopicPartitionsResponsePartition> partitions = partitionResponse.get();
                    DescribeTopicPartitionsResponseTopic response = new DescribeTopicPartitionsResponseTopic()
                        .setErrorCode(Errors.NONE.code())
                        .setName(topicName)
                        .setTopicId(Optional.ofNullable(image.topics().getTopic(topicName).id()).orElse(Uuid.ZERO_UUID))
                        .setIsInternal(Topic.isInternal(topicName))
                        .setPartitions(partitions);
                    result.topics().add(response);

                    if (nextPartition != -1) {
                        result.setNextCursor(new Cursor().setTopicName(topicName).setPartitionIndex(nextPartition));
                        break;
                    } else {
                        remaining.addAndGet(-partitions.size());
                    }
                } else if (!ignoreTopicsWithExceptions) {
                    Errors error;
                    try {
                        Topic.validate(topicName);
                        error = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                    } catch (InvalidTopicException e) {
                        error = Errors.INVALID_TOPIC_EXCEPTION;
                    }
                    result.topics().add(new DescribeTopicPartitionsResponseTopic()
                        .setErrorCode(error.code())
                        .setName(topicName)
                        .setTopicId(getTopicId(topicName))
                        .setIsInternal(Topic.isInternal(topicName)));
                }
            } else if (remaining.get() == 0) {
                // The cursor should point to the beginning of the current topic. All the partitions in the previous topic
                // should be fulfilled. Note that, if a partition is pointed in the NextTopicPartition, it does not mean
                // this topic exists.
                result.setNextCursor(new Cursor().setTopicName(topicName).setPartitionIndex(0));
                break;
            }
        }
        return result;
    }

    @Override
    public Set<String> getAllTopics() {
        return currentImage.topics().topicsByName().keySet();
    }

    @Override
    public Uuid getTopicId(String topicName) {
        MetadataImage image = currentImage;
        return image.topics().getTopic(topicName) == null ? Uuid.ZERO_UUID : image.topics().getTopic(topicName).id();
    }

    @Override
    public Optional<String> getTopicName(Uuid topicId) {
        return Optional.ofNullable(currentImage.topics().topicsById().get(topicId)).map(TopicImage::name);
    }

    @Override
    public boolean hasAliveBroker(int brokerId) {
        MetadataImage image = currentImage;
        return image.cluster().broker(brokerId) != null && !image.cluster().broker(brokerId).fenced();
    }

    @Override
    public boolean isBrokerFenced(int brokerId) {
        MetadataImage image = currentImage;
        return image.cluster().broker(brokerId) != null && image.cluster().broker(brokerId).fenced();
    }

    @Override
    public boolean isBrokerShuttingDown(int brokerId) {
        MetadataImage image = currentImage;
        return image.cluster().broker(brokerId) != null && image.cluster().broker(brokerId).inControlledShutdown();
    }

    @Override
    public Optional<Node> getAliveBrokerNode(int brokerId, ListenerName listenerName) {
        return Optional.ofNullable(currentImage.cluster().broker(brokerId))
            .filter(Predicate.not(BrokerRegistration::fenced))
            .flatMap(broker -> broker.node(listenerName.value()));
    }

    @Override
    public List<Node> getAliveBrokerNodes(ListenerName listenerName) {
        return currentImage.cluster().brokers().values().stream()
            .filter(Predicate.not(BrokerRegistration::fenced))
            .flatMap(broker -> broker.node(listenerName.value()).stream())
            .collect(Collectors.toList());
    }

    @Override
    public List<Node> getBrokerNodes(ListenerName listenerName) {
        return currentImage.cluster().brokers().values().stream()
            .flatMap(broker -> broker.node(listenerName.value()).stream())
            .collect(Collectors.toList());
    }

    @Override
    public Optional<LeaderAndIsr> getLeaderAndIsr(String topicName, int partitionId) {
        return Optional.ofNullable(currentImage.topics().getTopic(topicName))
            .flatMap(topic -> Optional.ofNullable(topic.partitions().get(partitionId)))
            .map(partition -> new LeaderAndIsr(
                partition.leader,
                partition.leaderEpoch,
                Arrays.stream(partition.isr).boxed().collect(Collectors.toList()),
                partition.leaderRecoveryState,
                partition.partitionEpoch
            ));
    }

    @Override
    public Optional<Integer> numPartitions(String topicName) {
        return Optional.ofNullable(currentImage.topics().getTopic(topicName)).map(topic -> topic.partitions().size());
    }

    @Override
    public Map<Uuid, String> topicIdsToNames() {
        return currentImage.topics().topicIdToNameView();
    }

    @Override
    public Map<String, Uuid> topicNamesToIds() {
        return currentImage.topics().topicNameToIdView();
    }

    /**
     * If the leader is not known, return None;
     * If the leader is known and corresponding node is available, return Some(node)
     * If the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
     */
    @Override
    public Optional<Node> getPartitionLeaderEndpoint(String topicName, int partitionId, ListenerName listenerName) {
        MetadataImage image = currentImage;
        return Optional.ofNullable(image.topics().getTopic(topicName))
            .flatMap(topic -> Optional.ofNullable(topic.partitions().get(partitionId)))
            .flatMap(partition -> Optional.ofNullable(image.cluster().broker(partition.leader))
                .map(broker -> broker.node(listenerName.value()).orElse(Node.noNode())));
    }

    @Override
    public Map<Integer, Node> getPartitionReplicaEndpoints(TopicPartition tp, ListenerName listenerName) {
        MetadataImage image = currentImage;
        TopicImage topic = image.topics().getTopic(tp.topic());
        if (topic == null) return Map.of();
        PartitionRegistration partition = topic.partitions().get(tp.partition());
        if (partition == null) return Map.of();
        Map<Integer, Node> result = new HashMap<>();
        for (int replicaId : partition.replicas) {
            BrokerRegistration broker = image.cluster().broker(replicaId);
            if (broker != null && !broker.fenced()) {
                broker.node(listenerName.value()).ifPresent(node -> {
                    if (!node.isEmpty()) {
                        result.put(replicaId, node);
                    }
                });
            }
        }
        return result;
    }

    @Override
    public Optional<Integer> getRandomAliveBrokerId() {
        List<Integer> aliveBrokers = currentImage.cluster().brokers().values().stream()
            .filter(Predicate.not(BrokerRegistration::fenced))
            .map(BrokerRegistration::id).toList();
        if (aliveBrokers.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(aliveBrokers.get(ThreadLocalRandom.current().nextInt(aliveBrokers.size())));
        }
    }

    @Override
    public Optional<Long> getAliveBrokerEpoch(int brokerId) {
        return Optional.ofNullable(currentImage.cluster().broker(brokerId))
            .filter(Predicate.not(BrokerRegistration::fenced))
            .map(BrokerRegistration::epoch);
    }

    @Override
    public boolean contains(String topicName) {
        return currentImage.topics().topicsByName().containsKey(topicName);
    }

    @Override
    public boolean contains(TopicPartition tp) {
        return Optional.ofNullable(currentImage.topics().getTopic(tp.topic()))
            .map(topic -> topic.partitions().containsKey(tp.partition()))
            .orElse(false);
    }

    public void setImage(MetadataImage newImage) {
        this.currentImage = newImage;
    }

    public MetadataImage getImage() {
        return currentImage;
    }

    @Override
    public Properties config(ConfigResource configResource) {
        return currentImage.configs().configProperties(configResource);
    }

    @Override
    public DescribeClientQuotasResponseData describeClientQuotas(DescribeClientQuotasRequestData request) {
        return currentImage.clientQuotas().describe(request);
    }

    @Override
    public DescribeUserScramCredentialsResponseData describeScramCredentials(DescribeUserScramCredentialsRequestData request) {
        return currentImage.scram().describe(request);
    }

    @Override
    public MetadataVersion metadataVersion() {
        return currentImage.features().metadataVersionOrThrow();
    }

    @Override
    public FinalizedFeatures features() {
        MetadataImage image = currentImage;
        Map<String, Short> finalizedFeatures = new HashMap<>(image.features().finalizedVersions());
        short kraftVersionLevel = kraftVersionSupplier.get().featureLevel();
        if (kraftVersionLevel > 0) {
            finalizedFeatures.put(KRaftVersion.FEATURE_NAME, kraftVersionLevel);
        }
        return new FinalizedFeatures(image.features().metadataVersionOrThrow(), finalizedFeatures, image.highestOffsetAndEpoch().offset());
    }
}
