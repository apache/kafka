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
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

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

    private static final Field.ComplexArray BROKERS = new Field.ComplexArray("brokers",
            "Host and port information for all brokers.");
    private static final Field.ComplexArray TOPIC_METADATA = new Field.ComplexArray("topic_metadata",
            "Metadata for requested topics");

    // cluster level fields
    private static final Field.NullableStr CLUSTER_ID = new Field.NullableStr("cluster_id",
            "The cluster id that this broker belongs to.");
    private static final Field.Int32 CONTROLLER_ID = new Field.Int32("controller_id",
            "The broker id of the controller broker.");

    // broker level fields
    private static final Field.Int32 NODE_ID = new Field.Int32("node_id", "The broker id.");
    private static final Field.Str HOST = new Field.Str("host", "The hostname of the broker.");
    private static final Field.Int32 PORT = new Field.Int32("port", "The port on which the broker accepts requests.");
    private static final Field.NullableStr RACK = new Field.NullableStr("rack", "The rack of the broker.");

    // topic level fields
    private static final Field.ComplexArray PARTITION_METADATA = new Field.ComplexArray("partition_metadata",
            "Metadata for each partition of the topic.");
    private static final Field.Bool IS_INTERNAL = new Field.Bool("is_internal",
            "Indicates if the topic is considered a Kafka internal topic");

    // partition level fields
    private static final Field.Int32 LEADER = new Field.Int32("leader",
            "The id of the broker acting as leader for this partition.");
    private static final Field.Array REPLICAS = new Field.Array("replicas", INT32,
            "The set of all nodes that host this partition.");
    private static final Field.Array ISR = new Field.Array("isr", INT32,
            "The set of nodes that are in sync with the leader for this partition.");
    private static final Field.Array OFFLINE_REPLICAS = new Field.Array("offline_replicas", INT32,
            "The set of offline replicas of this partition.");

    private static final Field METADATA_BROKER_V0 = BROKERS.withFields(
            NODE_ID,
            HOST,
            PORT);

    private static final Field PARTITION_METADATA_V0 = PARTITION_METADATA.withFields(
            ERROR_CODE,
            PARTITION_ID,
            LEADER,
            REPLICAS,
            ISR);

    private static final Field TOPIC_METADATA_V0 = TOPIC_METADATA.withFields(
            ERROR_CODE,
            TOPIC_NAME,
            PARTITION_METADATA_V0);

    private static final Schema METADATA_RESPONSE_V0 = new Schema(
            METADATA_BROKER_V0,
            TOPIC_METADATA_V0);

    // V1 adds fields for the rack of each broker, the controller id, and whether or not the topic is internal
    private static final Field METADATA_BROKER_V1 = BROKERS.withFields(
            NODE_ID,
            HOST,
            PORT,
            RACK);

    private static final Field TOPIC_METADATA_V1 = TOPIC_METADATA.withFields(
            ERROR_CODE,
            TOPIC_NAME,
            IS_INTERNAL,
            PARTITION_METADATA_V0);

    private static final Schema METADATA_RESPONSE_V1 = new Schema(
            METADATA_BROKER_V1,
            CONTROLLER_ID,
            TOPIC_METADATA_V1);

    // V2 added a field for the cluster id
    private static final Schema METADATA_RESPONSE_V2 = new Schema(
            METADATA_BROKER_V1,
            CLUSTER_ID,
            CONTROLLER_ID,
            TOPIC_METADATA_V1);

    // V3 adds the throttle time to the response
    private static final Schema METADATA_RESPONSE_V3 = new Schema(
            THROTTLE_TIME_MS,
            METADATA_BROKER_V1,
            CLUSTER_ID,
            CONTROLLER_ID,
            TOPIC_METADATA_V1);

    private static final Schema METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3;

    // V5 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Field PARTITION_METADATA_V5 = PARTITION_METADATA.withFields(
            ERROR_CODE,
            PARTITION_ID,
            LEADER,
            REPLICAS,
            ISR,
            OFFLINE_REPLICAS);

    private static final Field TOPIC_METADATA_V5 = TOPIC_METADATA.withFields(
            ERROR_CODE,
            TOPIC_NAME,
            IS_INTERNAL,
            PARTITION_METADATA_V5);

    private static final Schema METADATA_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            METADATA_BROKER_V1,
            CLUSTER_ID,
            CONTROLLER_ID,
            TOPIC_METADATA_V5);

    // V6 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema METADATA_RESPONSE_V6 = METADATA_RESPONSE_V5;

    // V7 adds the leader epoch to the partition metadata
    private static final Field PARTITION_METADATA_V7 = PARTITION_METADATA.withFields(
            ERROR_CODE,
            PARTITION_ID,
            LEADER,
            LEADER_EPOCH,
            REPLICAS,
            ISR,
            OFFLINE_REPLICAS);

    private static final Field TOPIC_METADATA_V7 = TOPIC_METADATA.withFields(
            ERROR_CODE,
            TOPIC_NAME,
            IS_INTERNAL,
            PARTITION_METADATA_V7);

    private static final Schema METADATA_RESPONSE_V7 = new Schema(
            THROTTLE_TIME_MS,
            METADATA_BROKER_V1,
            CLUSTER_ID,
            CONTROLLER_ID,
            TOPIC_METADATA_V7);

    public static Schema[] schemaVersions() {
        return new Schema[] {METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3,
            METADATA_RESPONSE_V4, METADATA_RESPONSE_V5, METADATA_RESPONSE_V6, METADATA_RESPONSE_V7};
    }

    private final int throttleTimeMs;
    private final Collection<Node> brokers;
    private final Node controller;
    private final List<TopicMetadata> topicMetadata;
    private final String clusterId;

    /**
     * Constructor for all versions.
     */
    public MetadataResponse(List<Node> brokers, String clusterId, int controllerId, List<TopicMetadata> topicMetadata) {
        this(DEFAULT_THROTTLE_TIME, brokers, clusterId, controllerId, topicMetadata);
    }

    public MetadataResponse(int throttleTimeMs, List<Node> brokers, String clusterId, int controllerId, List<TopicMetadata> topicMetadata) {
        this.throttleTimeMs = throttleTimeMs;
        this.brokers = brokers;
        this.controller = getControllerNode(controllerId, brokers);
        this.topicMetadata = topicMetadata;
        this.clusterId = clusterId;
    }

    public MetadataResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        Map<Integer, Node> brokers = new HashMap<>();
        Object[] brokerStructs = struct.get(BROKERS);
        for (Object brokerStruct : brokerStructs) {
            Struct broker = (Struct) brokerStruct;
            int nodeId = broker.get(NODE_ID);
            String host = broker.get(HOST);
            int port = broker.get(PORT);
            // This field only exists in v1+
            // When we can't know if a rack exists in a v0 response we default to null
            String rack = broker.getOrElse(RACK, null);
            brokers.put(nodeId, new Node(nodeId, host, port, rack));
        }

        // This field only exists in v1+
        // When we can't know the controller id in a v0 response we default to NO_CONTROLLER_ID
        int controllerId = struct.getOrElse(CONTROLLER_ID, NO_CONTROLLER_ID);

        // This field only exists in v2+
        this.clusterId = struct.getOrElse(CLUSTER_ID, null);

        List<TopicMetadata> topicMetadata = new ArrayList<>();
        Object[] topicInfos = struct.get(TOPIC_METADATA);
        for (Object topicInfoObj : topicInfos) {
            Struct topicInfo = (Struct) topicInfoObj;
            Errors topicError = Errors.forCode(topicInfo.get(ERROR_CODE));
            String topic = topicInfo.get(TOPIC_NAME);
            // This field only exists in v1+
            // When we can't know if a topic is internal or not in a v0 response we default to false
            boolean isInternal = topicInfo.getOrElse(IS_INTERNAL, false);
            List<PartitionMetadata> partitionMetadata = new ArrayList<>();

            Object[] partitionInfos = topicInfo.get(PARTITION_METADATA);
            for (Object partitionInfoObj : partitionInfos) {
                Struct partitionInfo = (Struct) partitionInfoObj;
                Errors partitionError = Errors.forCode(partitionInfo.get(ERROR_CODE));
                int partition = partitionInfo.get(PARTITION_ID);
                int leader = partitionInfo.get(LEADER);
                Optional<Integer> leaderEpoch = RequestUtils.getLeaderEpoch(partitionInfo, LEADER_EPOCH);
                Node leaderNode = leader == -1 ? null : brokers.get(leader);

                Object[] replicas = partitionInfo.get(REPLICAS);
                List<Node> replicaNodes = convertToNodes(brokers, replicas);

                Object[] isr = partitionInfo.get(ISR);
                List<Node> isrNodes = convertToNodes(brokers, isr);

                Object[] offlineReplicas = partitionInfo.getOrEmpty(OFFLINE_REPLICAS);
                List<Node> offlineNodes = convertToNodes(brokers, offlineReplicas);

                partitionMetadata.add(new PartitionMetadata(partitionError, partition, leaderNode, leaderEpoch,
                        replicaNodes, isrNodes, offlineNodes));
            }

            topicMetadata.add(new TopicMetadata(topicError, topic, isInternal, partitionMetadata));
        }

        this.brokers = brokers.values();
        this.controller = getControllerNode(controllerId, brokers.values());
        this.topicMetadata = topicMetadata;
    }

    private List<Node> convertToNodes(Map<Integer, Node> brokers, Object[] brokerIds) {
        List<Node> nodes = new ArrayList<>(brokerIds.length);
        for (Object brokerId : brokerIds)
            if (brokers.containsKey(brokerId))
                nodes.add(brokers.get(brokerId));
            else
                nodes.add(new Node((int) brokerId, "", -1));
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
        return throttleTimeMs;
    }

    /**
     * Get a map of the topics which had metadata errors
     * @return the map
     */
    public Map<String, Errors> errors() {
        Map<String, Errors> errors = new HashMap<>();
        for (TopicMetadata metadata : topicMetadata) {
            if (metadata.error != Errors.NONE)
                errors.put(metadata.topic(), metadata.error);
        }
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (TopicMetadata metadata : topicMetadata)
            updateErrorCounts(errorCounts, metadata.error);
        return errorCounts;
    }

    /**
     * Returns the set of topics with the specified error
     */
    public Set<String> topicsByError(Errors error) {
        Set<String> errorTopics = new HashSet<>();
        for (TopicMetadata metadata : topicMetadata) {
            if (metadata.error == error)
                errorTopics.add(metadata.topic());
        }
        return errorTopics;
    }

    /**
     * Returns the set of topics with an error indicating invalid metadata
     * and topics with any partition whose error indicates invalid metadata.
     * This includes all non-existent topics specified in the metadata request
     * and any topic returned with one or more partitions whose leader is not known.
     */
    public Set<String> unavailableTopics() {
        Set<String> invalidMetadataTopics = new HashSet<>();
        for (TopicMetadata topicMetadata : this.topicMetadata) {
            if (topicMetadata.error.exception() instanceof InvalidMetadataException)
                invalidMetadataTopics.add(topicMetadata.topic);
            else {
                for (PartitionMetadata partitionMetadata : topicMetadata.partitionMetadata) {
                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        invalidMetadataTopics.add(topicMetadata.topic);
                        break;
                    }
                }
            }
        }
        return invalidMetadataTopics;
    }

    /**
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
    public Cluster cluster() {
        Set<String> internalTopics = new HashSet<>();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (TopicMetadata metadata : topicMetadata) {

            if (metadata.error == Errors.NONE) {
                if (metadata.isInternal)
                    internalTopics.add(metadata.topic);
                for (PartitionMetadata partitionMetadata : metadata.partitionMetadata) {
                    partitions.add(partitionMetaToInfo(metadata.topic, partitionMetadata));
                }
            }
        }
        return new Cluster(this.clusterId, this.brokers, partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                topicsByError(Errors.INVALID_TOPIC_EXCEPTION), internalTopics, this.controller);
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
        return brokers;
    }

    /**
     * Get all topic metadata returned in the metadata response
     * @return the topicMetadata
     */
    public Collection<TopicMetadata> topicMetadata() {
        return topicMetadata;
    }

    /**
     * The controller node returned in metadata response
     * @return the controller node or null if it doesn't exist
     */
    public Node controller() {
        return controller;
    }

    /**
     * The cluster identifier returned in the metadata response.
     * @return cluster identifier if it is present in the response, null otherwise.
     */
    public String clusterId() {
        return this.clusterId;
    }

    public static MetadataResponse parse(ByteBuffer buffer, short version) {
        return new MetadataResponse(ApiKeys.METADATA.parseResponse(version, buffer));
    }

    public static class TopicMetadata {
        private final Errors error;
        private final String topic;
        private final boolean isInternal;
        private final List<PartitionMetadata> partitionMetadata;

        public TopicMetadata(Errors error,
                             String topic,
                             boolean isInternal,
                             List<PartitionMetadata> partitionMetadata) {
            this.error = error;
            this.topic = topic;
            this.isInternal = isInternal;
            this.partitionMetadata = partitionMetadata;
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

        @Override
        public String toString() {
            return "(type=TopicMetadata" +
                    ", error=" + error +
                    ", topic=" + topic +
                    ", isInternal=" + isInternal +
                    ", partitionMetadata=" + partitionMetadata + ')';
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

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.METADATA.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> brokerArray = new ArrayList<>();
        for (Node node : brokers) {
            Struct broker = struct.instance(BROKERS);
            broker.set(NODE_ID, node.id());
            broker.set(HOST, node.host());
            broker.set(PORT, node.port());
            // This field only exists in v1+
            broker.setIfExists(RACK, node.rack());
            brokerArray.add(broker);
        }
        struct.set(BROKERS, brokerArray.toArray());

        // This field only exists in v1+
        struct.setIfExists(CONTROLLER_ID, controller == null ? NO_CONTROLLER_ID : controller.id());

        // This field only exists in v2+
        struct.setIfExists(CLUSTER_ID, clusterId);

        List<Struct> topicMetadataArray = new ArrayList<>(topicMetadata.size());
        for (TopicMetadata metadata : topicMetadata) {
            Struct topicData = struct.instance(TOPIC_METADATA);
            topicData.set(TOPIC_NAME, metadata.topic);
            topicData.set(ERROR_CODE, metadata.error.code());
            // This field only exists in v1+
            topicData.setIfExists(IS_INTERNAL, metadata.isInternal());

            List<Struct> partitionMetadataArray = new ArrayList<>(metadata.partitionMetadata.size());
            for (PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                Struct partitionData = topicData.instance(PARTITION_METADATA);
                partitionData.set(ERROR_CODE, partitionMetadata.error.code());
                partitionData.set(PARTITION_ID, partitionMetadata.partition);
                partitionData.set(LEADER, partitionMetadata.leaderId());

                // Leader epoch exists in v7 forward
                RequestUtils.setLeaderEpochIfExists(partitionData, LEADER_EPOCH, partitionMetadata.leaderEpoch);

                ArrayList<Integer> replicas = new ArrayList<>(partitionMetadata.replicas.size());
                for (Node node : partitionMetadata.replicas)
                    replicas.add(node.id());
                partitionData.set(REPLICAS, replicas.toArray());
                ArrayList<Integer> isr = new ArrayList<>(partitionMetadata.isr.size());
                for (Node node : partitionMetadata.isr)
                    isr.add(node.id());
                partitionData.set(ISR, isr.toArray());
                if (partitionData.hasField(OFFLINE_REPLICAS)) {
                    ArrayList<Integer> offlineReplicas = new ArrayList<>(partitionMetadata.offlineReplicas.size());
                    for (Node node : partitionMetadata.offlineReplicas)
                        offlineReplicas.add(node.id());
                    partitionData.set(OFFLINE_REPLICAS, offlineReplicas.toArray());
                }
                partitionMetadataArray.add(partitionData);

            }
            topicData.set(PARTITION_METADATA, partitionMetadataArray.toArray());
            topicMetadataArray.add(topicData);
        }
        struct.set(TOPIC_METADATA, topicMetadataArray.toArray());
        return struct;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
