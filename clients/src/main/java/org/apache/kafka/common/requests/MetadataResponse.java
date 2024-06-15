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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Possible topic-level error codes:
 *  UnknownTopic (3)
 *  LeaderNotAvailable (5)
 *  InvalidTopic (17)
 *  TopicAuthorizationFailed (29)

 * Possible partition-level error codes:
 *  LeaderNotAvailable (5)
 *  ReplicaNotAvailable (9)
 */
public class MetadataResponse extends AbstractResponse {
    public static final int NO_CONTROLLER_ID = -1;
    public static final int NO_LEADER_ID = -1;
    public static final int AUTHORIZED_OPERATIONS_OMITTED = Integer.MIN_VALUE;

    private final MetadataResponseData data;
    private volatile Holder holder;
    private final boolean hasReliableLeaderEpochs;

    public MetadataResponse(MetadataResponseData data, short version) {
        this(data, hasReliableLeaderEpochs(version));
    }

    MetadataResponse(MetadataResponseData data, boolean hasReliableLeaderEpochs) {
        super(ApiKeys.METADATA);
        this.data = data;
        this.hasReliableLeaderEpochs = hasReliableLeaderEpochs;
    }

    @Override
    public MetadataResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    /**
     * Get a map of the topics which had metadata errors
     * @return the map
     */
    public Map<String, Errors> errors() {
        Map<String, Errors> errors = new HashMap<>();
        for (MetadataResponseTopic metadata : data.topics()) {
            if (metadata.name() == null) {
                throw new IllegalStateException("Use errorsByTopicId() when managing topic using topic id");
            }
            if (metadata.errorCode() != Errors.NONE.code())
                errors.put(metadata.name(), Errors.forCode(metadata.errorCode()));
        }
        return errors;
    }

    /**
     * Get a map of the topicIds which had metadata errors
     * @return the map
     */
    public Map<Uuid, Errors> errorsByTopicId() {
        Map<Uuid, Errors> errors = new HashMap<>();
        for (MetadataResponseTopic metadata : data.topics()) {
            if (metadata.topicId().equals(Uuid.ZERO_UUID)) {
                throw new IllegalStateException("Use errors() when managing topic using topic name");
            }
            if (metadata.errorCode() != Errors.NONE.code())
                errors.put(metadata.topicId(), Errors.forCode(metadata.errorCode()));
        }
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(metadata -> {
            metadata.partitions().forEach(p -> updateErrorCounts(errorCounts, Errors.forCode(p.errorCode())));
            updateErrorCounts(errorCounts, Errors.forCode(metadata.errorCode()));
        });
        return errorCounts;
    }

    /**
     * Returns the set of topics with the specified error
     */
    public Set<String> topicsByError(Errors error) {
        Set<String> errorTopics = new HashSet<>();
        for (MetadataResponseTopic metadata : data.topics()) {
            if (metadata.errorCode() == error.code())
                errorTopics.add(metadata.name());
        }
        return errorTopics;
    }

    /**
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
    public Cluster buildCluster() {
        Set<String> internalTopics = new HashSet<>();
        List<PartitionInfo> partitions = new ArrayList<>();
        Map<String, Uuid> topicIds = new HashMap<>();

        for (TopicMetadata metadata : topicMetadata()) {
            if (metadata.error == Errors.NONE) {
                if (metadata.isInternal)
                    internalTopics.add(metadata.topic);
                if (metadata.topicId() != null && !Uuid.ZERO_UUID.equals(metadata.topicId())) {
                    topicIds.put(metadata.topic, metadata.topicId());
                }
                for (PartitionMetadata partitionMetadata : metadata.partitionMetadata) {
                    partitions.add(toPartitionInfo(partitionMetadata, holder().brokers));
                }
            }
        }
        return new Cluster(data.clusterId(), brokers(), partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                topicsByError(Errors.INVALID_TOPIC_EXCEPTION), internalTopics, controller(), topicIds);
    }

    public static PartitionInfo toPartitionInfo(PartitionMetadata metadata, Map<Integer, Node> nodesById) {
        return new PartitionInfo(metadata.topic(),
                metadata.partition(),
                metadata.leaderId.map(nodesById::get).orElse(null),
                (metadata.replicaIds == null) ? null : convertToNodeArray(metadata.replicaIds, nodesById),
                (metadata.inSyncReplicaIds == null) ? null : convertToNodeArray(metadata.inSyncReplicaIds, nodesById),
                (metadata.offlineReplicaIds == null) ? null : convertToNodeArray(metadata.offlineReplicaIds, nodesById));
    }

    private static Node[] convertToNodeArray(List<Integer> replicaIds, Map<Integer, Node> nodesById) {
        return replicaIds.stream().map(replicaId -> {
            Node node = nodesById.get(replicaId);
            if (node == null)
                return new Node(replicaId, "", -1);
            return node;
        }).toArray(Node[]::new);
    }

    /**
     * Returns a 32-bit bitfield to represent authorized operations for this topic.
     */
    public Optional<Integer> topicAuthorizedOperations(String topicName) {
        MetadataResponseTopic topic = data.topics().find(topicName);
        if (topic == null)
            return Optional.empty();
        else
            return Optional.of(topic.topicAuthorizedOperations());
    }

    /**
     * Returns a 32-bit bitfield to represent authorized operations for this cluster.
     */
    public int clusterAuthorizedOperations() {
        return data.clusterAuthorizedOperations();
    }

    private Holder holder() {
        if (holder == null) {
            synchronized (data) {
                if (holder == null)
                    holder = new Holder(data);
            }
        }
        return holder;
    }

    /**
     * Get all brokers returned in metadata response
     * @return the brokers
     */
    public Collection<Node> brokers() {
        return holder().brokers.values();
    }

    public Map<Integer, Node> brokersById() {
        return holder().brokers;
    }

    /**
     * Get all topic metadata returned in the metadata response
     * @return the topicMetadata
     */
    public Collection<TopicMetadata> topicMetadata() {
        return holder().topicMetadata;
    }

    /**
     * The controller node returned in metadata response
     * @return the controller node or null if it doesn't exist
     */
    public Node controller() {
        return holder().controller;
    }

    /**
     * The cluster identifier returned in the metadata response.
     * @return cluster identifier if it is present in the response, null otherwise.
     */
    public String clusterId() {
        return this.data.clusterId();
    }

    /**
     * Check whether the leader epochs returned from the response can be relied on
     * for epoch validation in Fetch, ListOffsets, and OffsetsForLeaderEpoch requests.
     * If not, then the client will not retain the leader epochs and hence will not
     * forward them in requests.
     *
     * @return true if the epoch can be used for validation
     */
    public boolean hasReliableLeaderEpochs() {
        return hasReliableLeaderEpochs;
    }

    // Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker
    // does not propagate leader epoch information accurately while a reassignment is in
    // progress. Relying on a stale epoch can lead to FENCED_LEADER_EPOCH errors which
    // can prevent consumption throughout the course of a reassignment. It is safer in
    // this case to revert to the behavior in previous protocol versions which checks
    // leader status only.
    private static boolean hasReliableLeaderEpochs(short version) {
        return version >= 9;
    }

    public static MetadataResponse parse(ByteBuffer buffer, short version) {
        return new MetadataResponse(new MetadataResponseData(new ByteBufferAccessor(buffer), version),
            hasReliableLeaderEpochs(version));
    }

    public static class TopicMetadata {
        private final Errors error;
        private final String topic;
        private final Uuid topicId;
        private final boolean isInternal;
        private final List<PartitionMetadata> partitionMetadata;
        private int authorizedOperations;

        public TopicMetadata(Errors error,
                             String topic,
                             Uuid topicId,
                             boolean isInternal,
                             List<PartitionMetadata> partitionMetadata,
                             int authorizedOperations) {
            this.error = error;
            this.topic = topic;
            this.topicId = topicId;
            this.isInternal = isInternal;
            this.partitionMetadata = partitionMetadata;
            this.authorizedOperations = authorizedOperations;
        }

        public TopicMetadata(Errors error,
                             String topic,
                             boolean isInternal,
                             List<PartitionMetadata> partitionMetadata) {
            this(error, topic, Uuid.ZERO_UUID, isInternal, partitionMetadata, AUTHORIZED_OPERATIONS_OMITTED);
        }

        public Errors error() {
            return error;
        }

        public String topic() {
            return topic;
        }

        public Uuid topicId() {
            return topicId;
        }

        public boolean isInternal() {
            return isInternal;
        }

        public List<PartitionMetadata> partitionMetadata() {
            return partitionMetadata;
        }

        public void authorizedOperations(int authorizedOperations) {
            this.authorizedOperations = authorizedOperations;
        }

        public int authorizedOperations() {
            return authorizedOperations;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final TopicMetadata that = (TopicMetadata) o;
            return isInternal == that.isInternal &&
                error == that.error &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(topicId, that.topicId) &&
                Objects.equals(partitionMetadata, that.partitionMetadata) &&
                Objects.equals(authorizedOperations, that.authorizedOperations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(error, topic, isInternal, partitionMetadata, authorizedOperations);
        }

        @Override
        public String toString() {
            return "TopicMetadata{" +
                "error=" + error +
                ", topic='" + topic + '\'' +
                ", topicId='" + topicId + '\'' +
                ", isInternal=" + isInternal +
                ", partitionMetadata=" + partitionMetadata +
                ", authorizedOperations=" + authorizedOperations +
                '}';
        }
    }

    // This is used to describe per-partition state in the MetadataResponse
    public static class PartitionMetadata {
        public final TopicPartition topicPartition;
        public final Errors error;
        public final Optional<Integer> leaderId;
        public final Optional<Integer> leaderEpoch;
        public final List<Integer> replicaIds;
        public final List<Integer> inSyncReplicaIds;
        public final List<Integer> offlineReplicaIds;

        public PartitionMetadata(Errors error,
                                 TopicPartition topicPartition,
                                 Optional<Integer> leaderId,
                                 Optional<Integer> leaderEpoch,
                                 List<Integer> replicaIds,
                                 List<Integer> inSyncReplicaIds,
                                 List<Integer> offlineReplicaIds) {
            this.error = error;
            this.topicPartition = topicPartition;
            this.leaderId = leaderId;
            this.leaderEpoch = leaderEpoch;
            this.replicaIds = replicaIds;
            this.inSyncReplicaIds = inSyncReplicaIds;
            this.offlineReplicaIds = offlineReplicaIds;
        }

        public int partition() {
            return topicPartition.partition();
        }

        public String topic() {
            return topicPartition.topic();
        }

        public PartitionMetadata withoutLeaderEpoch() {
            return new PartitionMetadata(error,
                    topicPartition,
                    leaderId,
                    Optional.empty(),
                    replicaIds,
                    inSyncReplicaIds,
                    offlineReplicaIds);
        }

        @Override
        public String toString() {
            return "PartitionMetadata(" +
                    "error=" + error +
                    ", partition=" + topicPartition +
                    ", leader=" + leaderId +
                    ", leaderEpoch=" + leaderEpoch +
                    ", replicas=" + replicaIds.stream().map(Object::toString).collect(Collectors.joining(",")) +
                    ", isr=" + inSyncReplicaIds.stream().map(Object::toString).collect(Collectors.joining(",")) +
                    ", offlineReplicas=" + offlineReplicaIds.stream().map(Object::toString).collect(Collectors.joining(",")) + ')';
        }
    }

    private static class Holder {
        private final Map<Integer, Node> brokers;
        private final Node controller;
        private final Collection<TopicMetadata> topicMetadata;

        Holder(MetadataResponseData data) {
            this.brokers = Collections.unmodifiableMap(createBrokers(data));
            this.topicMetadata = createTopicMetadata(data);
            this.controller = brokers.get(data.controllerId());
        }

        private Map<Integer, Node> createBrokers(MetadataResponseData data) {
            return data.brokers().valuesList().stream().map(b -> new Node(b.nodeId(), b.host(), b.port(), b.rack()))
                    .collect(Collectors.toMap(Node::id, Function.identity()));
        }

        private Collection<TopicMetadata> createTopicMetadata(MetadataResponseData data) {
            List<TopicMetadata> topicMetadataList = new ArrayList<>();
            for (MetadataResponseTopic topicMetadata : data.topics()) {
                Errors topicError = Errors.forCode(topicMetadata.errorCode());
                String topic = topicMetadata.name();
                Uuid topicId = topicMetadata.topicId();
                boolean isInternal = topicMetadata.isInternal();
                List<PartitionMetadata> partitionMetadataList = new ArrayList<>();

                for (MetadataResponsePartition partitionMetadata : topicMetadata.partitions()) {
                    Errors partitionError = Errors.forCode(partitionMetadata.errorCode());
                    int partitionIndex = partitionMetadata.partitionIndex();

                    int leaderId = partitionMetadata.leaderId();
                    Optional<Integer> leaderIdOpt = leaderId < 0 ? Optional.empty() : Optional.of(leaderId);

                    Optional<Integer> leaderEpoch = RequestUtils.getLeaderEpoch(partitionMetadata.leaderEpoch());
                    TopicPartition topicPartition = new TopicPartition(topic, partitionIndex);
                    partitionMetadataList.add(new PartitionMetadata(partitionError, topicPartition, leaderIdOpt,
                            leaderEpoch, partitionMetadata.replicaNodes(), partitionMetadata.isrNodes(),
                            partitionMetadata.offlineReplicas()));
                }

                topicMetadataList.add(new TopicMetadata(topicError, topic, topicId, isInternal, partitionMetadataList,
                        topicMetadata.topicAuthorizedOperations()));
            }
            return topicMetadataList;
        }

    }

    public static MetadataResponse prepareResponse(short version,
                                                   int throttleTimeMs,
                                                   Collection<Node> brokers,
                                                   String clusterId,
                                                   int controllerId,
                                                   List<MetadataResponseTopic> topics,
                                                   int clusterAuthorizedOperations) {
        return prepareResponse(hasReliableLeaderEpochs(version), throttleTimeMs, brokers, clusterId, controllerId,
                topics, clusterAuthorizedOperations);
    }

    // Visible for testing
    public static MetadataResponse prepareResponse(boolean hasReliableEpoch,
                                                   int throttleTimeMs,
                                                   Collection<Node> brokers,
                                                   String clusterId,
                                                   int controllerId,
                                                   List<MetadataResponseTopic> topics,
                                                   int clusterAuthorizedOperations) {
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.setThrottleTimeMs(throttleTimeMs);
        brokers.forEach(broker ->
            responseData.brokers().add(new MetadataResponseBroker()
                .setNodeId(broker.id())
                .setHost(broker.host())
                .setPort(broker.port())
                .setRack(broker.rack()))
        );

        responseData.setClusterId(clusterId);
        responseData.setControllerId(controllerId);
        responseData.setClusterAuthorizedOperations(clusterAuthorizedOperations);

        topics.forEach(topicMetadata -> responseData.topics().add(topicMetadata));
        return new MetadataResponse(responseData, hasReliableEpoch);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
