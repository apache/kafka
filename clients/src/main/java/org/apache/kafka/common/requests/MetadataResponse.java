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
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

    public static final int AUTHORIZED_OPERATIONS_OMITTED = Integer.MIN_VALUE;

    private final MetadataResponseData data;
    private volatile Holder holder;
    private final boolean hasReliableLeaderEpochs;

    public MetadataResponse(MetadataResponseData data) {
        this(data, true);
    }

    public MetadataResponse(Struct struct, short version) {
        // Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker
        // does not propagate leader epoch information accurately while a reassignment is in
        // progress. Relying on a stale epoch can lead to FENCED_LEADER_EPOCH errors which
        // can prevent consumption throughout the course of a reassignment. It is safer in
        // this case to revert to the behavior in previous protocol versions which checks
        // leader status only.
        this(new MetadataResponseData(struct, version), version >= 9);
    }

    private MetadataResponse(MetadataResponseData data, boolean hasReliableLeaderEpochs) {
        this.data = data;
        this.hasReliableLeaderEpochs = hasReliableLeaderEpochs;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    /**
     * Get a map of the topics which had metadata errors
     * @return the map
     */
    public Map<String, Errors> errors() {
        Map<String, Errors> errors = new HashMap<>();
        for (MetadataResponseTopic metadata : data.topics()) {
            if (metadata.errorCode() != Errors.NONE.code())
                errors.put(metadata.name(), Errors.forCode(metadata.errorCode()));
        }
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (MetadataResponseTopic metadata : data.topics())
            updateErrorCounts(errorCounts, Errors.forCode(metadata.errorCode()));
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
    public Cluster cluster() {
        Set<String> internalTopics = new HashSet<>();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (TopicMetadata metadata : topicMetadata()) {
            if (metadata.error == Errors.NONE) {
                if (metadata.isInternal)
                    internalTopics.add(metadata.topic);
                for (PartitionMetadata partitionMetadata : metadata.partitionMetadata) {
                    partitions.add(partitionMetaToInfo(metadata.topic, partitionMetadata));
                }
            }
        }
        return new Cluster(data.clusterId(), brokers(), partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                topicsByError(Errors.INVALID_TOPIC_EXCEPTION), internalTopics, controller());
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

    /**
     * Transform a topic and PartitionMetadata into PartitionInfo.
     */
    public static PartitionInfo partitionMetaToInfo(String topic, PartitionMetadata partitionMetadata) {
        return new PartitionInfo(
                topic,
                partitionMetadata.partition(),
                partitionMetadata.leader(),
                partitionMetadata.replicas().toArray(new Node[0]),
                partitionMetadata.isr().toArray(new Node[0]),
                partitionMetadata.offlineReplicas().toArray(new Node[0]));
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

    /**
     * Get a map of all brokers keyed by the brokerId.
     * @return A map from the brokerId to the broker `Node` information
     */
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

    public static MetadataResponse parse(ByteBuffer buffer, short version) {
        return new MetadataResponse(ApiKeys.METADATA.responseSchema(version).read(buffer), version);
    }

    public static class TopicMetadata {
        private final Errors error;
        private final String topic;
        private final boolean isInternal;
        private final List<PartitionMetadata> partitionMetadata;
        private int authorizedOperations;

        public TopicMetadata(Errors error,
                             String topic,
                             boolean isInternal,
                             List<PartitionMetadata> partitionMetadata,
                             int authorizedOperations) {
            this.error = error;
            this.topic = topic;
            this.isInternal = isInternal;
            this.partitionMetadata = partitionMetadata;
            this.authorizedOperations = authorizedOperations;
        }

        public TopicMetadata(Errors error,
                             String topic,
                             boolean isInternal,
                             List<PartitionMetadata> partitionMetadata) {
            this(error, topic, isInternal, partitionMetadata, 0);
        }

        public Errors error() {
            return error;
        }

        public String topic() {
            return topic;
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
                ", isInternal=" + isInternal +
                ", partitionMetadata=" + partitionMetadata +
                ", authorizedOperations=" + authorizedOperations +
                '}';
        }
    }

    // This is used to describe per-partition state in the MetadataResponse
    public static class PartitionMetadata {
        private final Errors error;
        private final int partition;
        private final Node leader;
        private final Optional<Integer> leaderEpoch;
        private final List<Node> replicas;
        private final List<Node> isr;
        private final List<Node> offlineReplicas;

        public PartitionMetadata(Errors error,
                                 int partition,
                                 Node leader,
                                 Optional<Integer> leaderEpoch,
                                 List<Node> replicas,
                                 List<Node> isr,
                                 List<Node> offlineReplicas) {
            this.error = error;
            this.partition = partition;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.replicas = replicas;
            this.isr = isr;
            this.offlineReplicas = offlineReplicas;
        }

        public Errors error() {
            return error;
        }

        public int partition() {
            return partition;
        }

        public int leaderId() {
            return leader == null ? -1 : leader.id();
        }

        public Optional<Integer> leaderEpoch() {
            return leaderEpoch;
        }

        public Node leader() {
            return leader;
        }

        public List<Node> replicas() {
            return replicas;
        }

        public List<Node> isr() {
            return isr;
        }

        public List<Node> offlineReplicas() {
            return offlineReplicas;
        }

        @Override
        public String toString() {
            return "(type=PartitionMetadata" +
                    ", error=" + error +
                    ", partition=" + partition +
                    ", leader=" + leader +
                    ", leaderEpoch=" + leaderEpoch +
                    ", replicas=" + Utils.join(replicas, ",") +
                    ", isr=" + Utils.join(isr, ",") +
                    ", offlineReplicas=" + Utils.join(offlineReplicas, ",") + ')';
        }
    }

    private static class Holder {
        private final Map<Integer, Node> brokers;
        private final Node controller;
        private final Collection<TopicMetadata> topicMetadata;

        Holder(MetadataResponseData data) {
            this.brokers = createBrokers(data);
            this.topicMetadata = createTopicMetadata(data);
            this.controller = brokers.get(data.controllerId());
        }

        private Map<Integer, Node> createBrokers(MetadataResponseData data) {
            return data.brokers().valuesList().stream().map(b -> new Node(b.nodeId(), b.host(), b.port(), b.rack()))
                    .collect(Collectors.toMap(Node::id, b -> b));
        }

        private Collection<TopicMetadata> createTopicMetadata(MetadataResponseData data) {
            List<TopicMetadata> topicMetadataList = new ArrayList<>();
            for (MetadataResponseTopic topicMetadata : data.topics()) {
                Errors topicError = Errors.forCode(topicMetadata.errorCode());
                String topic = topicMetadata.name();
                boolean isInternal = topicMetadata.isInternal();
                List<PartitionMetadata> partitionMetadataList = new ArrayList<>();

                for (MetadataResponsePartition partitionMetadata : topicMetadata.partitions()) {
                    Errors partitionError = Errors.forCode(partitionMetadata.errorCode());
                    int partitionIndex = partitionMetadata.partitionIndex();
                    int leader = partitionMetadata.leaderId();
                    Optional<Integer> leaderEpoch = RequestUtils.getLeaderEpoch(partitionMetadata.leaderEpoch());
                    Node leaderNode = leader == -1 ? null : brokers.get(leader);
                    List<Node> replicaNodes = convertToNodes(partitionMetadata.replicaNodes());
                    List<Node> isrNodes = convertToNodes(partitionMetadata.isrNodes());
                    List<Node> offlineNodes = convertToNodes(partitionMetadata.offlineReplicas());
                    partitionMetadataList.add(new PartitionMetadata(partitionError, partitionIndex, leaderNode, leaderEpoch,
                            replicaNodes, isrNodes, offlineNodes));
                }

                topicMetadataList.add(new TopicMetadata(topicError, topic, isInternal, partitionMetadataList,
                        topicMetadata.topicAuthorizedOperations()));
            }
            return topicMetadataList;
        }

        private List<Node> convertToNodes(List<Integer> brokerIds) {
            List<Node> nodes = new ArrayList<>(brokerIds.size());
            for (Integer brokerId : brokerIds) {
                Node node = brokers.get(brokerId);
                if (node == null)
                    nodes.add(new Node(brokerId, "", -1));
                else
                    nodes.add(node);
            }
            return nodes;
        }

    }

    public static MetadataResponse prepareResponse(int throttleTimeMs, Collection<Node> brokers, String clusterId,
                                                   int controllerId, List<TopicMetadata> topicMetadataList,
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

        topicMetadataList.forEach(topicMetadata -> {
            MetadataResponseTopic metadataResponseTopic = new MetadataResponseTopic();
            metadataResponseTopic
                .setErrorCode(topicMetadata.error.code())
                .setName(topicMetadata.topic)
                .setIsInternal(topicMetadata.isInternal)
                .setTopicAuthorizedOperations(topicMetadata.authorizedOperations);

            for (PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata) {
                metadataResponseTopic.partitions().add(new MetadataResponsePartition()
                    .setErrorCode(partitionMetadata.error.code())
                    .setPartitionIndex(partitionMetadata.partition)
                    .setLeaderId(partitionMetadata.leader == null ? -1 : partitionMetadata.leader.id())
                    .setLeaderEpoch(partitionMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
                    .setReplicaNodes(partitionMetadata.replicas.stream().map(Node::id).collect(Collectors.toList()))
                    .setIsrNodes(partitionMetadata.isr.stream().map(Node::id).collect(Collectors.toList()))
                    .setOfflineReplicas(partitionMetadata.offlineReplicas.stream().map(Node::id).collect(Collectors.toList())));
            }
            responseData.topics().add(metadataResponseTopic);
        });
        return new MetadataResponse(responseData);
    }

    public static MetadataResponse prepareResponse(int throttleTimeMs, Collection<Node> brokers, String clusterId,
                                                   int controllerId, List<TopicMetadata> topicMetadataList) {
        return prepareResponse(throttleTimeMs, brokers, clusterId, controllerId, topicMetadataList,
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    }

    public static MetadataResponse prepareResponse(Collection<Node> brokers, String clusterId, int controllerId,
                                                   List<TopicMetadata> topicMetadata) {
        return prepareResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, brokers, clusterId, controllerId, topicMetadata);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
