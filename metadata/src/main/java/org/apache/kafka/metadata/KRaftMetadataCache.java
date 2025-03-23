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

import java.util.AbstractMap;
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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    // This method is the main hotspot when it comes to the performance of metadata requests,
    // we should be careful about adding additional logic here.
    // filterUnavailableEndpoints exists to support v0 MetadataResponses
    private List<Integer> maybeFilterAliveReplicas(MetadataImage image, int[] brokers, ListenerName listenerName, boolean filterUnavailableEndpoints) {
        if (!filterUnavailableEndpoints) {
            return Replicas.toList(brokers);
        } else {
            List<Integer> res = new ArrayList<>(brokers.length);
            for (int brokerId : brokers) {
                Optional.ofNullable(image.cluster().broker(brokerId)).ifPresent(b -> {
                    if (!b.fenced() && b.listeners().containsKey(listenerName.value())) {
                        res.add(brokerId);
                    }
                });
            }
            return res;
        }
    }

    public MetadataImage currentImage() {
        return currentImage;
    }

    // errorUnavailableEndpoints exists to support v0 MetadataResponses
    // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
    // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
    private Optional<Iterator<MetadataResponsePartition>> getPartitionMetadata(
        MetadataImage image,
        String topicName,
        ListenerName listenerName,
        boolean errorUnavailableEndpoints,
        boolean errorUnavailableListeners
    ) {
        return Optional.ofNullable(image.topics().getTopic(topicName)).map(topic -> topic.partitions().entrySet().stream().map(entry -> {
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
        }).iterator());
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
    private Entry<Optional<List<DescribeTopicPartitionsResponsePartition>>, Integer> getPartitionMetadataForDescribeTopicResponse(
        MetadataImage image,
        String topicName,
        ListenerName listenerName,
        int startIndex,
        int maxCount
    ) {
        TopicImage topic = image.topics().getTopic(topicName);
        if (topic == null) {
            return new AbstractMap.SimpleEntry<>(Optional.empty(), -1);
        } else {
            List<DescribeTopicPartitionsResponsePartition> result = new ArrayList<>();
            Set<Integer> partitions = topic.partitions().keySet();
            int upperIndex = Math.min(topic.partitions().size(), startIndex + maxCount);
            int nextIndex = (upperIndex < partitions.size()) ? upperIndex : -1;
            for (int partitionId = startIndex; partitionId < upperIndex; partitionId++) {
                PartitionRegistration partition = topic.partitions().get(partitionId);
                if (partition != null) {
                    List<Integer> filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas, listenerName, false);
                    List<Integer> filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName, false);
                    List<Integer> offlineReplicas = getOfflineReplicas(image, partition, listenerName);
                    Optional<Node> maybeLeader = getAliveEndpoint(image, partition.leader, listenerName);
                    if (maybeLeader.isEmpty()) {
                        result.add(new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(partitionId)
                            .setLeaderId(MetadataResponse.NO_LEADER_ID)
                            .setLeaderEpoch(partition.leaderEpoch)
                            .setReplicaNodes(filteredReplicas)
                            .setIsrNodes(filteredIsr)
                            .setOfflineReplicas(offlineReplicas)
                            .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                            .setLastKnownElr(Replicas.toList(partition.lastKnownElr)));
                    } else {
                        result.add(new DescribeTopicPartitionsResponsePartition()
                            .setPartitionIndex(partitionId)
                            .setLeaderId(maybeLeader.get().id())
                            .setLeaderEpoch(partition.leaderEpoch)
                            .setReplicaNodes(filteredReplicas)
                            .setIsrNodes(filteredIsr)
                            .setOfflineReplicas(offlineReplicas)
                            .setEligibleLeaderReplicas(Replicas.toList(partition.elr))
                            .setLastKnownElr(Replicas.toList(partition.lastKnownElr)));
                    }
                } else {
                    log.warn("The partition {} does not exist for {}", partitionId, topicName);
                }
            }
            return new AbstractMap.SimpleEntry<>(Optional.of(result), nextIndex);
        }
    }

    private List<Integer> getOfflineReplicas(MetadataImage image, PartitionRegistration partition, ListenerName listenerName) {
        List<Integer> offlineReplicas = new ArrayList<>(0);
        for (int brokerId : partition.replicas) {
            Optional.ofNullable(image.cluster().broker(brokerId)).ifPresentOrElse(broker -> {
                if (isReplicaOffline(partition, listenerName, broker)) {
                    offlineReplicas.add(brokerId);
                }
            }, () -> offlineReplicas.add(brokerId));
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
        return Optional.ofNullable(image.cluster().broker(id)).flatMap(b -> b.node(listenerName.value()));
    }

    // errorUnavailableEndpoints exists to support v0 MetadataResponses
    @Override
    public List<MetadataResponseTopic> getTopicMetadata(
        Set<String> topics,
        ListenerName listenerName,
        boolean errorUnavailableEndpoints,
        boolean errorUnavailableListeners
    ) {
        MetadataImage image = currentImage;
        return topics.stream().flatMap(topic ->
            getPartitionMetadata(image, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners)
                .stream()
                .map(partitionMetadata ->
                    new MetadataResponseTopic().setErrorCode(Errors.NONE.code())
                        .setName(topic)
                        .setTopicId(Optional.ofNullable(image.topics().getTopic(topic).id()).orElse(Uuid.ZERO_UUID))
                        .setIsInternal(Topic.isInternal(topic))
                        .setPartitions(StreamSupport.stream(Spliterators.spliteratorUnknownSize(partitionMetadata, Spliterator.ORDERED), false)
                            .collect(Collectors.toList())))
        ).collect(Collectors.toList());
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
                Entry<Optional<List<DescribeTopicPartitionsResponsePartition>>, Integer> partitionResponseEntry = getPartitionMetadataForDescribeTopicResponse(image, topicName, listenerName, topicPartitionStartIndex.apply(topicName), remaining.get());
                Optional<List<DescribeTopicPartitionsResponsePartition>> partitionResponse = partitionResponseEntry.getKey();
                int nextPartition = partitionResponseEntry.getValue();
                AtomicBoolean breakLoop = new AtomicBoolean(false);
                partitionResponse.ifPresent(partitions -> {
                    DescribeTopicPartitionsResponseTopic response = new DescribeTopicPartitionsResponseTopic()
                        .setErrorCode(Errors.NONE.code())
                        .setName(topicName)
                        .setTopicId(Optional.ofNullable(image.topics().getTopic(topicName).id()).orElse(Uuid.ZERO_UUID))
                        .setIsInternal(Topic.isInternal(topicName))
                        .setPartitions(partitions);
                    result.topics().add(response);

                    if (nextPartition != -1) {
                        result.setNextCursor(new Cursor().setTopicName(topicName).setPartitionIndex(nextPartition));
                        breakLoop.set(true);
                    }
                    remaining.addAndGet(-partitions.size());
                });

                if (breakLoop.get()) {
                    break;
                }

                if (!ignoreTopicsWithExceptions && partitionResponse.isEmpty()) {
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
        return Optional.ofNullable(currentImage.topics().topicsByName().get(topicName)).map(TopicImage::id).orElse(Uuid.ZERO_UUID);
    }

    @Override
    public Optional<String> getTopicName(Uuid topicId) {
        return Optional.ofNullable(currentImage.topics().topicsById().get(topicId)).map(TopicImage::name);
    }

    @Override
    public boolean hasAliveBroker(int brokerId) {
        return Optional.ofNullable(currentImage.cluster().broker(brokerId)).filter(b -> !b.fenced()).isPresent();
    }

    @Override
    public boolean isBrokerFenced(int brokerId) {
        return Optional.ofNullable(currentImage.cluster().broker(brokerId)).filter(BrokerRegistration::fenced).isPresent();
    }

    @Override
    public boolean isBrokerShuttingDown(int brokerId) {
        return Optional.ofNullable(currentImage.cluster().broker(brokerId)).filter(BrokerRegistration::inControlledShutdown).isPresent();
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
    public Optional<Node> getPartitionLeaderEndpoint(String topicName, int partitionId, ListenerName listenerName) {
        return Optional.ofNullable(currentImage.topics().getTopic(topicName))
            .flatMap(topic -> Optional.ofNullable(topic.partitions().get(partitionId)))
            .flatMap(partition -> Optional.ofNullable(currentImage.cluster().broker(partition.leader))
                .map(broker -> broker.node(listenerName.value()).orElse(Node.noNode())));
    }

    @Override
    public Map<Integer, Node> getPartitionReplicaEndpoints(TopicPartition tp, ListenerName listenerName) {
        Map<Integer, Node> result = new HashMap<>();
        Optional.ofNullable(currentImage.topics().getTopic(tp.topic()))
            .flatMap(topic -> Optional.ofNullable(topic.partitions().get(tp.partition())))
            .ifPresent(partition -> {
                for (int replicaId : partition.replicas) {
                    BrokerRegistration broker = currentImage.cluster().broker(replicaId);
                    if (broker != null && !broker.fenced()) {
                        broker.node(listenerName.value()).ifPresent(node -> {
                            if (!node.isEmpty()) {
                                result.put(replicaId, node);
                            }
                        });
                    }
                }
            });
        return result;
    }

    @Override
    public Optional<Integer> getRandomAliveBrokerId() {
        return getRandomAliveBroker(currentImage);
    }

    private Optional<Integer> getRandomAliveBroker(MetadataImage image) {
        List<Integer> aliveBrokers = image.cluster().brokers().values().stream()
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
