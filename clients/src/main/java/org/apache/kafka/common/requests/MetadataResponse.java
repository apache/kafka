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
import org.apache.kafka.common.protocol.types.ArrayOf;
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
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class MetadataResponse extends AbstractResponse {
    private static final String BROKERS_KEY_NAME = "brokers";
    private static final String TOPIC_METADATA_KEY_NAME = "topic_metadata";

    // broker level field names
    private static final String NODE_ID_KEY_NAME = "node_id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";
    private static final String RACK_KEY_NAME = "rack";

    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    public static final int NO_CONTROLLER_ID = -1;

    private static final String CLUSTER_ID_KEY_NAME = "cluster_id";

    /**
     * Possible error codes:
     *
     * UnknownTopic (3)
     * LeaderNotAvailable (5)
     * InvalidTopic (17)
     * TopicAuthorizationFailed (29)
     */

    private static final String IS_INTERNAL_KEY_NAME = "is_internal";
    private static final String PARTITION_METADATA_KEY_NAME = "partition_metadata";

    /**
     * Possible error codes:
     *
     * LeaderNotAvailable (5)
     * ReplicaNotAvailable (9)
     */

    private static final String LEADER_KEY_NAME = "leader";
    private static final String REPLICAS_KEY_NAME = "replicas";
    private static final String ISR_KEY_NAME = "isr";
    private static final String OFFLINE_REPLICAS_KEY_NAME = "offline_replicas";

    private static final Schema METADATA_BROKER_V0 = new Schema(
            new Field(NODE_ID_KEY_NAME, INT32, "The broker id."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."));

    private static final Schema PARTITION_METADATA_V0 = new Schema(
            ERROR_CODE,
            PARTITION_ID,
            new Field(LEADER_KEY_NAME, INT32, "The id of the broker acting as leader for this partition."),
            new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The set of all nodes that host this partition."),
            new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The set of nodes that are in sync with the leader for this partition."));

    private static final Schema TOPIC_METADATA_V0 = new Schema(
            ERROR_CODE,
            TOPIC_NAME,
            new Field(PARTITION_METADATA_KEY_NAME, new ArrayOf(PARTITION_METADATA_V0), "Metadata for each partition of the topic."));

    private static final Schema METADATA_RESPONSE_V0 = new Schema(
            new Field(BROKERS_KEY_NAME, new ArrayOf(METADATA_BROKER_V0), "Host and port information for all brokers."),
            new Field(TOPIC_METADATA_KEY_NAME, new ArrayOf(TOPIC_METADATA_V0)));

    private static final Schema METADATA_BROKER_V1 = new Schema(
            new Field(NODE_ID_KEY_NAME, INT32, "The broker id."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."),
            new Field(RACK_KEY_NAME, NULLABLE_STRING, "The rack of the broker."));

    private static final Schema PARTITION_METADATA_V1 = PARTITION_METADATA_V0;

    // PARTITION_METADATA_V2 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Schema PARTITION_METADATA_V2 = new Schema(
            ERROR_CODE,
            PARTITION_ID,
            new Field(LEADER_KEY_NAME, INT32, "The id of the broker acting as leader for this partition."),
            new Field(REPLICAS_KEY_NAME, new ArrayOf(INT32), "The set of all nodes that host this partition."),
            new Field(ISR_KEY_NAME, new ArrayOf(INT32), "The set of nodes that are in sync with the leader for this partition."),
            new Field(OFFLINE_REPLICAS_KEY_NAME, new ArrayOf(INT32), "The set of offline replicas of this partition."));

    private static final Schema TOPIC_METADATA_V1 = new Schema(
            ERROR_CODE,
            TOPIC_NAME,
            new Field(IS_INTERNAL_KEY_NAME, BOOLEAN, "Indicates if the topic is considered a Kafka internal topic"),
            new Field(PARTITION_METADATA_KEY_NAME, new ArrayOf(PARTITION_METADATA_V1), "Metadata for each partition of the topic."));

    // TOPIC_METADATA_V2 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Schema TOPIC_METADATA_V2 = new Schema(
            ERROR_CODE,
            TOPIC_NAME,
            new Field(IS_INTERNAL_KEY_NAME, BOOLEAN, "Indicates if the topic is considered a Kafka internal topic"),
            new Field(PARTITION_METADATA_KEY_NAME, new ArrayOf(PARTITION_METADATA_V2), "Metadata for each partition of the topic."));

    private static final Schema METADATA_RESPONSE_V1 = new Schema(
            new Field(BROKERS_KEY_NAME, new ArrayOf(METADATA_BROKER_V1), "Host and port information for all brokers."),
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The broker id of the controller broker."),
            new Field(TOPIC_METADATA_KEY_NAME, new ArrayOf(TOPIC_METADATA_V1)));

    private static final Schema METADATA_RESPONSE_V2 = new Schema(
            new Field(BROKERS_KEY_NAME, new ArrayOf(METADATA_BROKER_V1), "Host and port information for all brokers."),
            new Field(CLUSTER_ID_KEY_NAME, NULLABLE_STRING, "The cluster id that this broker belongs to."),
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The broker id of the controller broker."),
            new Field(TOPIC_METADATA_KEY_NAME, new ArrayOf(TOPIC_METADATA_V1)));

    private static final Schema METADATA_RESPONSE_V3 = new Schema(
            THROTTLE_TIME_MS,
            new Field(BROKERS_KEY_NAME, new ArrayOf(METADATA_BROKER_V1), "Host and port information for all brokers."),
            new Field(CLUSTER_ID_KEY_NAME, NULLABLE_STRING, "The cluster id that this broker belongs to."),
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The broker id of the controller broker."),
            new Field(TOPIC_METADATA_KEY_NAME, new ArrayOf(TOPIC_METADATA_V1)));

    private static final Schema METADATA_RESPONSE_V4 = METADATA_RESPONSE_V3;

    // METADATA_RESPONSE_V5 added a per-partition offline_replicas field. This field specifies the list of replicas that are offline.
    private static final Schema METADATA_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            new Field(BROKERS_KEY_NAME, new ArrayOf(METADATA_BROKER_V1), "Host and port information for all brokers."),
            new Field(CLUSTER_ID_KEY_NAME, NULLABLE_STRING, "The cluster id that this broker belongs to."),
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The broker id of the controller broker."),
            new Field(TOPIC_METADATA_KEY_NAME, new ArrayOf(TOPIC_METADATA_V2)));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema METADATA_RESPONSE_V6 = METADATA_RESPONSE_V5;

    public static Schema[] schemaVersions() {
        return new Schema[] {METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3,
            METADATA_RESPONSE_V4, METADATA_RESPONSE_V5, METADATA_RESPONSE_V6};
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
        Object[] brokerStructs = (Object[]) struct.get(BROKERS_KEY_NAME);
        for (Object brokerStruct : brokerStructs) {
            Struct broker = (Struct) brokerStruct;
            int nodeId = broker.getInt(NODE_ID_KEY_NAME);
            String host = broker.getString(HOST_KEY_NAME);
            int port = broker.getInt(PORT_KEY_NAME);
            // This field only exists in v1+
            // When we can't know if a rack exists in a v0 response we default to null
            String rack = broker.hasField(RACK_KEY_NAME) ? broker.getString(RACK_KEY_NAME) : null;
            brokers.put(nodeId, new Node(nodeId, host, port, rack));
        }

        // This field only exists in v1+
        // When we can't know the controller id in a v0 response we default to NO_CONTROLLER_ID
        int controllerId = NO_CONTROLLER_ID;
        if (struct.hasField(CONTROLLER_ID_KEY_NAME))
            controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);

        // This field only exists in v2+
        if (struct.hasField(CLUSTER_ID_KEY_NAME)) {
            this.clusterId = struct.getString(CLUSTER_ID_KEY_NAME);
        } else {
            this.clusterId = null;
        }

        List<TopicMetadata> topicMetadata = new ArrayList<>();
        Object[] topicInfos = (Object[]) struct.get(TOPIC_METADATA_KEY_NAME);
        for (Object topicInfoObj : topicInfos) {
            Struct topicInfo = (Struct) topicInfoObj;
            Errors topicError = Errors.forCode(topicInfo.get(ERROR_CODE));
            String topic = topicInfo.get(TOPIC_NAME);
            // This field only exists in v1+
            // When we can't know if a topic is internal or not in a v0 response we default to false
            boolean isInternal = topicInfo.hasField(IS_INTERNAL_KEY_NAME) ? topicInfo.getBoolean(IS_INTERNAL_KEY_NAME) : false;

            List<PartitionMetadata> partitionMetadata = new ArrayList<>();

            Object[] partitionInfos = (Object[]) topicInfo.get(PARTITION_METADATA_KEY_NAME);
            for (Object partitionInfoObj : partitionInfos) {
                Struct partitionInfo = (Struct) partitionInfoObj;
                Errors partitionError = Errors.forCode(partitionInfo.get(ERROR_CODE));
                int partition = partitionInfo.get(PARTITION_ID);
                int leader = partitionInfo.getInt(LEADER_KEY_NAME);
                Node leaderNode = leader == -1 ? null : brokers.get(leader);

                Object[] replicas = (Object[]) partitionInfo.get(REPLICAS_KEY_NAME);
                List<Node> replicaNodes = convertToNodes(brokers, replicas);

                Object[] isr = (Object[]) partitionInfo.get(ISR_KEY_NAME);
                List<Node> isrNodes = convertToNodes(brokers, isr);

                Object[] offlineReplicas = partitionInfo.hasField(OFFLINE_REPLICAS_KEY_NAME) ?
                    (Object[]) partitionInfo.get(OFFLINE_REPLICAS_KEY_NAME) : new Object[0];
                List<Node> offlineNodes = convertToNodes(brokers, offlineReplicas);

                partitionMetadata.add(new PartitionMetadata(partitionError, partition, leaderNode, replicaNodes, isrNodes, offlineNodes));
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
                for (PartitionMetadata partitionMetadata : metadata.partitionMetadata)
                    partitions.add(new PartitionInfo(
                            metadata.topic,
                            partitionMetadata.partition,
                            partitionMetadata.leader,
                            partitionMetadata.replicas.toArray(new Node[0]),
                            partitionMetadata.isr.toArray(new Node[0]),
                            partitionMetadata.offlineReplicas.toArray(new Node[0])));
            }
        }

        return new Cluster(this.clusterId, this.brokers, partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                internalTopics, this.controller);
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
        private final List<Node> replicas;
        private final List<Node> isr;
        private final List<Node> offlineReplicas;

        public PartitionMetadata(Errors error,
                                 int partition,
                                 Node leader,
                                 List<Node> replicas,
                                 List<Node> isr,
                                 List<Node> offlineReplicas) {
            this.error = error;
            this.partition = partition;
            this.leader = leader;
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
            Struct broker = struct.instance(BROKERS_KEY_NAME);
            broker.set(NODE_ID_KEY_NAME, node.id());
            broker.set(HOST_KEY_NAME, node.host());
            broker.set(PORT_KEY_NAME, node.port());
            // This field only exists in v1+
            if (broker.hasField(RACK_KEY_NAME))
                broker.set(RACK_KEY_NAME, node.rack());
            brokerArray.add(broker);
        }
        struct.set(BROKERS_KEY_NAME, brokerArray.toArray());

        // This field only exists in v1+
        if (struct.hasField(CONTROLLER_ID_KEY_NAME))
            struct.set(CONTROLLER_ID_KEY_NAME, controller == null ? NO_CONTROLLER_ID : controller.id());

        // This field only exists in v2+
        if (struct.hasField(CLUSTER_ID_KEY_NAME))
            struct.set(CLUSTER_ID_KEY_NAME, clusterId);

        List<Struct> topicMetadataArray = new ArrayList<>(topicMetadata.size());
        for (TopicMetadata metadata : topicMetadata) {
            Struct topicData = struct.instance(TOPIC_METADATA_KEY_NAME);
            topicData.set(TOPIC_NAME, metadata.topic);
            topicData.set(ERROR_CODE, metadata.error.code());
            // This field only exists in v1+
            if (topicData.hasField(IS_INTERNAL_KEY_NAME))
                topicData.set(IS_INTERNAL_KEY_NAME, metadata.isInternal());

            List<Struct> partitionMetadataArray = new ArrayList<>(metadata.partitionMetadata.size());
            for (PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                Struct partitionData = topicData.instance(PARTITION_METADATA_KEY_NAME);
                partitionData.set(ERROR_CODE, partitionMetadata.error.code());
                partitionData.set(PARTITION_ID, partitionMetadata.partition);
                partitionData.set(LEADER_KEY_NAME, partitionMetadata.leaderId());
                ArrayList<Integer> replicas = new ArrayList<>(partitionMetadata.replicas.size());
                for (Node node : partitionMetadata.replicas)
                    replicas.add(node.id());
                partitionData.set(REPLICAS_KEY_NAME, replicas.toArray());
                ArrayList<Integer> isr = new ArrayList<>(partitionMetadata.isr.size());
                for (Node node : partitionMetadata.isr)
                    isr.add(node.id());
                partitionData.set(ISR_KEY_NAME, isr.toArray());
                if (partitionData.hasField(OFFLINE_REPLICAS_KEY_NAME)) {
                    ArrayList<Integer> offlineReplicas = new ArrayList<>(partitionMetadata.offlineReplicas.size());
                    for (Node node : partitionMetadata.offlineReplicas)
                        offlineReplicas.add(node.id());
                    partitionData.set(OFFLINE_REPLICAS_KEY_NAME, offlineReplicas.toArray());
                }
                partitionMetadataArray.add(partitionData);

            }
            topicData.set(PARTITION_METADATA_KEY_NAME, partitionMetadataArray.toArray());
            topicMetadataArray.add(topicData);
        }
        struct.set(TOPIC_METADATA_KEY_NAME, topicMetadataArray.toArray());
        return struct;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6;
    }
}
