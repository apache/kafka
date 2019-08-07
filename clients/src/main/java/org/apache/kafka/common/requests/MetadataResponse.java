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
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
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

    private MetadataResponseData data;

    public MetadataResponse(MetadataResponseData data) {
        this.data = data;
    }

    private Map<Integer, Node> brokersMap() {
        return data.brokers().stream().collect(
            Collectors.toMap(MetadataResponseBroker::nodeId, b -> new Node(b.nodeId(), b.host(), b.port(), b.rack())));
    }

    public MetadataResponse(Struct struct, short version) {
        this(new MetadataResponseData(struct, version));
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public MetadataResponseData data() {
        return data;
    }

    private List<Node> convertToNodes(Map<Integer, Node> brokers, List<Integer> brokerIds) {
        List<Node> nodes = new ArrayList<>(brokerIds.size());
        for (Integer brokerId : brokerIds)
            if (brokers.containsKey(brokerId))
                nodes.add(brokers.get(brokerId));
            else
                nodes.add(new Node(brokerId, "", -1));
        return nodes;
    }

    private Node getControllerNode(int controllerId, Collection<Node> brokers) {
        for (Node broker : brokers) {
            if (broker.id() == controllerId)
                return broker;
        }
        return null;
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
        return new Cluster(data.clusterId(), brokersMap().values(), partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                topicsByError(Errors.INVALID_TOPIC_EXCEPTION), internalTopics, controller());
    }

    /**
     * Transform a topic and PartitionMetadata into PartitionInfo
     * @return
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

    /**
     * Get all brokers returned in metadata response
     * @return the brokers
     */
    public Collection<Node> brokers() {
        return new ArrayList<>(brokersMap().values());
    }

    /**
     * Get all topic metadata returned in the metadata response
     * @return the topicMetadata
     */
    public Collection<TopicMetadata> topicMetadata() {
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
                Node leaderNode = leader == -1 ? null : brokersMap().get(leader);
                List<Node> replicaNodes = convertToNodes(brokersMap(), partitionMetadata.replicaNodes());
                List<Node> isrNodes = convertToNodes(brokersMap(), partitionMetadata.isrNodes());
                List<Node> offlineNodes = convertToNodes(brokersMap(), partitionMetadata.offlineReplicas());
                partitionMetadataList.add(new PartitionMetadata(partitionError, partitionIndex, leaderNode, leaderEpoch,
                    replicaNodes, isrNodes, offlineNodes));
            }

            topicMetadataList.add(new TopicMetadata(topicError, topic, isInternal, partitionMetadataList,
                topicMetadata.topicAuthorizedOperations()));
        }
        return  topicMetadataList;
    }

    /**
     * The controller node returned in metadata response
     * @return the controller node or null if it doesn't exist
     */
    public Node controller() {
        return getControllerNode(data.controllerId(), brokers());
    }

    /**
     * The cluster identifier returned in the metadata response.
     * @return cluster identifier if it is present in the response, null otherwise.
     */
    public String clusterId() {
        return this.data.clusterId();
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

    public static MetadataResponse prepareResponse(int throttleTimeMs, List<Node> brokers, String clusterId,
                                                   int controllerId, List<TopicMetadata> topicMetadataList,
                                                   int clusterAuthorizedOperations) {
        MetadataResponseData responseData = new MetadataResponseData();
        responseData.setThrottleTimeMs(throttleTimeMs);
        brokers.forEach(broker -> {
            responseData.brokers().add(new MetadataResponseBroker()
                .setNodeId(broker.id())
                .setHost(broker.host())
                .setPort(broker.port())
                .setRack(broker.rack()));
        });

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

    public static MetadataResponse prepareResponse(int throttleTimeMs, List<Node> brokers, String clusterId,
                                                   int controllerId, List<TopicMetadata> topicMetadataList) {
        return prepareResponse(throttleTimeMs, brokers, clusterId, controllerId, topicMetadataList,
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    }

    public static MetadataResponse prepareResponse(List<Node> brokers, String clusterId, int controllerId,
                                                   List<TopicMetadata> topicMetadata) {
        return prepareResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, brokers, clusterId, controllerId, topicMetadata);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
