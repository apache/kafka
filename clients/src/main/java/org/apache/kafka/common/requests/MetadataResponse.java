/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetadataResponse extends AbstractRequestResponse {

    private static final short CURRENT_VERSION = ProtoUtils.latestVersion(ApiKeys.METADATA.id);
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

    // topic level field names
    private static final String TOPIC_ERROR_CODE_KEY_NAME = "topic_error_code";

    /**
     * Possible error codes:
     *
     * UnknownTopic (3)
     * LeaderNotAvailable (5)
     * InvalidTopic (17)
     * TopicAuthorizationFailed (29)
     */

    private static final String TOPIC_KEY_NAME = "topic";
    private static final String IS_INTERNAL_KEY_NAME = "is_internal";
    private static final String PARTITION_METADATA_KEY_NAME = "partition_metadata";

    // partition level field names
    private static final String PARTITION_ERROR_CODE_KEY_NAME = "partition_error_code";

    /**
     * Possible error codes:
     *
     * LeaderNotAvailable (5)
     * ReplicaNotAvailable (9)
     */

    private static final String PARTITION_KEY_NAME = "partition_id";
    private static final String LEADER_KEY_NAME = "leader";
    private static final String REPLICAS_KEY_NAME = "replicas";
    private static final String ISR_KEY_NAME = "isr";

    private final Collection<Node> brokers;
    private final Node controller;
    private final List<TopicMetadata> topicMetadata;
    private final String clusterId;

    /**
     * Constructor for the latest version
     */
    public MetadataResponse(List<Node> brokers, String clusterId, int controllerId, List<TopicMetadata> topicMetadata) {
        this(brokers, clusterId, controllerId, topicMetadata, CURRENT_VERSION);
    }

    /**
     * Constructor for a specific version
     */
    public MetadataResponse(List<Node> brokers, String clusterId, int controllerId, List<TopicMetadata> topicMetadata, int version) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.METADATA.id, version)));
        this.brokers = brokers;
        this.controller = getControllerNode(controllerId, brokers);
        this.topicMetadata = topicMetadata;
        this.clusterId = clusterId;

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
            struct.set(CONTROLLER_ID_KEY_NAME, controllerId);

        // This field only exists in v2+
        if (struct.hasField(CLUSTER_ID_KEY_NAME))
            struct.set(CLUSTER_ID_KEY_NAME, clusterId);

        List<Struct> topicMetadataArray = new ArrayList<>(topicMetadata.size());
        for (TopicMetadata metadata : topicMetadata) {
            Struct topicData = struct.instance(TOPIC_METADATA_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, metadata.topic);
            topicData.set(TOPIC_ERROR_CODE_KEY_NAME, metadata.error.code());
            // This field only exists in v1+
            if (topicData.hasField(IS_INTERNAL_KEY_NAME))
                topicData.set(IS_INTERNAL_KEY_NAME, metadata.isInternal());

            List<Struct> partitionMetadataArray = new ArrayList<>(metadata.partitionMetadata.size());
            for (PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                Struct partitionData = topicData.instance(PARTITION_METADATA_KEY_NAME);
                partitionData.set(PARTITION_ERROR_CODE_KEY_NAME, partitionMetadata.error.code());
                partitionData.set(PARTITION_KEY_NAME, partitionMetadata.partition);
                partitionData.set(LEADER_KEY_NAME, partitionMetadata.leader.id());
                ArrayList<Integer> replicas = new ArrayList<>(partitionMetadata.replicas.size());
                for (Node node : partitionMetadata.replicas)
                    replicas.add(node.id());
                partitionData.set(REPLICAS_KEY_NAME, replicas.toArray());
                ArrayList<Integer> isr = new ArrayList<>(partitionMetadata.isr.size());
                for (Node node : partitionMetadata.isr)
                    isr.add(node.id());
                partitionData.set(ISR_KEY_NAME, isr.toArray());
                partitionMetadataArray.add(partitionData);

            }
            topicData.set(PARTITION_METADATA_KEY_NAME, partitionMetadataArray.toArray());
            topicMetadataArray.add(topicData);
        }
        struct.set(TOPIC_METADATA_KEY_NAME, topicMetadataArray.toArray());
    }

    public MetadataResponse(Struct struct) {
        super(struct);

        Map<Integer, Node> brokers = new HashMap<>();
        Object[] brokerStructs = (Object[]) struct.get(BROKERS_KEY_NAME);
        for (int i = 0; i < brokerStructs.length; i++) {
            Struct broker = (Struct) brokerStructs[i];
            int nodeId = broker.getInt(NODE_ID_KEY_NAME);
            String host = broker.getString(HOST_KEY_NAME);
            int port = broker.getInt(PORT_KEY_NAME);
            // This field only exists in v1+
            // When we can't know if a rack exists in a v0 response we default to null
            String rack =  broker.hasField(RACK_KEY_NAME) ? broker.getString(RACK_KEY_NAME) : null;
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
        for (int i = 0; i < topicInfos.length; i++) {
            Struct topicInfo = (Struct) topicInfos[i];
            Errors topicError = Errors.forCode(topicInfo.getShort(TOPIC_ERROR_CODE_KEY_NAME));
            String topic = topicInfo.getString(TOPIC_KEY_NAME);
            // This field only exists in v1+
            // When we can't know if a topic is internal or not in a v0 response we default to false
            boolean isInternal = topicInfo.hasField(IS_INTERNAL_KEY_NAME) ? topicInfo.getBoolean(IS_INTERNAL_KEY_NAME) : false;

            List<PartitionMetadata> partitionMetadata = new ArrayList<>();

            Object[] partitionInfos = (Object[]) topicInfo.get(PARTITION_METADATA_KEY_NAME);
            for (int j = 0; j < partitionInfos.length; j++) {
                Struct partitionInfo = (Struct) partitionInfos[j];
                Errors partitionError = Errors.forCode(partitionInfo.getShort(PARTITION_ERROR_CODE_KEY_NAME));
                int partition = partitionInfo.getInt(PARTITION_KEY_NAME);
                int leader = partitionInfo.getInt(LEADER_KEY_NAME);
                Node leaderNode = leader == -1 ? null : brokers.get(leader);
                Object[] replicas = (Object[]) partitionInfo.get(REPLICAS_KEY_NAME);

                List<Node> replicaNodes = new ArrayList<>(replicas.length);
                for (Object replicaNodeId : replicas)
                    if (brokers.containsKey(replicaNodeId))
                        replicaNodes.add(brokers.get(replicaNodeId));
                    else
                        replicaNodes.add(new Node((int) replicaNodeId, "", -1));

                Object[] isr = (Object[]) partitionInfo.get(ISR_KEY_NAME);
                List<Node> isrNodes = new ArrayList<>(isr.length);
                for (Object isrNode : isr)
                    if (brokers.containsKey(isrNode))
                        isrNodes.add(brokers.get(isrNode));
                    else
                        isrNodes.add(new Node((int) isrNode, "", -1));

                partitionMetadata.add(new PartitionMetadata(partitionError, partition, leaderNode, replicaNodes, isrNodes));
            }

            topicMetadata.add(new TopicMetadata(topicError, topic, isInternal, partitionMetadata));
        }

        this.brokers = brokers.values();
        this.controller = getControllerNode(controllerId, brokers.values());
        this.topicMetadata = topicMetadata;
    }

    private Node getControllerNode(int controllerId, Collection<Node> brokers) {
        for (Node broker : brokers) {
            if (broker.id() == controllerId)
                return broker;
        }
        return null;
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
                            partitionMetadata.isr.toArray(new Node[0])));
            }
        }

        return new Cluster(this.clusterId, this.brokers, partitions, topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED), internalTopics);
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

    public static MetadataResponse parse(ByteBuffer buffer) {
        return parse(buffer, CURRENT_VERSION);
    }

    public static MetadataResponse parse(ByteBuffer buffer, int version) {
        return new MetadataResponse(ProtoUtils.responseSchema(ApiKeys.METADATA.id, version).read(buffer));
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

    }

    public static class PartitionMetadata {
        private final Errors error;
        private final int partition;
        private final Node leader;
        private final List<Node> replicas;
        private final List<Node> isr;

        public PartitionMetadata(Errors error,
                                 int partition,
                                 Node leader,
                                 List<Node> replicas,
                                 List<Node> isr) {
            this.error = error;
            this.partition = partition;
            this.leader = leader;
            this.replicas = replicas;
            this.isr = isr;
        }

        public Errors error() {
            return error;
        }

        public int partition() {
            return partition;
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

    }

}
