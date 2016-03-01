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
import org.apache.kafka.common.protocol.types.Schema;
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

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.METADATA.id);
    private static final String BROKERS_KEY_NAME = "brokers";
    private static final String TOPIC_METATDATA_KEY_NAME = "topic_metadata";

    // broker level field names
    private static final String NODE_ID_KEY_NAME = "node_id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

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
    private final List<TopicMetadata> topicMetadata;


    public MetadataResponse(List<Node> brokers, List<TopicMetadata> topicMetadata) {
        super(new Struct(CURRENT_SCHEMA));

        this.brokers = brokers;
        this.topicMetadata = topicMetadata;

        List<Struct> brokerArray = new ArrayList<>();
        for (Node node : brokers) {
            Struct broker = struct.instance(BROKERS_KEY_NAME);
            broker.set(NODE_ID_KEY_NAME, node.id());
            broker.set(HOST_KEY_NAME, node.host());
            broker.set(PORT_KEY_NAME, node.port());
            brokerArray.add(broker);
        }
        struct.set(BROKERS_KEY_NAME, brokerArray.toArray());

        List<Struct> topicMetadataArray = new ArrayList<>(topicMetadata.size());
        for (TopicMetadata metadata : topicMetadata) {
            Struct topicData = struct.instance(TOPIC_METATDATA_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, metadata.topic);
            topicData.set(TOPIC_ERROR_CODE_KEY_NAME, metadata.error.code());

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
        struct.set(TOPIC_METATDATA_KEY_NAME, topicMetadataArray.toArray());
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
            brokers.put(nodeId, new Node(nodeId, host, port));
        }

        List<TopicMetadata> topicMetadata = new ArrayList<>();
        Object[] topicInfos = (Object[]) struct.get(TOPIC_METATDATA_KEY_NAME);
        for (int i = 0; i < topicInfos.length; i++) {
            Struct topicInfo = (Struct) topicInfos[i];
            Errors topicError = Errors.forCode(topicInfo.getShort(TOPIC_ERROR_CODE_KEY_NAME));
            String topic = topicInfo.getString(TOPIC_KEY_NAME);
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
                    replicaNodes.add(brokers.get(replicaNodeId));
                Object[] isr = (Object[]) partitionInfo.get(ISR_KEY_NAME);
                List<Node> isrNodes = new ArrayList<>(isr.length);
                for (Object isrNode : isr)
                    isrNodes.add(brokers.get(isrNode));
                partitionMetadata.add(new PartitionMetadata(partitionError, partition, leaderNode, replicaNodes, isrNodes));
            }

            topicMetadata.add(new TopicMetadata(topicError, topic, partitionMetadata));
        }

        this.brokers = brokers.values();
        this.topicMetadata = topicMetadata;
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
     * Get a snapshot of the cluster metadata from this response
     * @return the cluster snapshot
     */
    public Cluster cluster() {
        Set<String> unauthorizedTopics = new HashSet<>();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (TopicMetadata metadata : topicMetadata) {
            if (metadata.error == Errors.NONE) {
                for (PartitionMetadata partitionMetadata : metadata.partitionMetadata)
                    partitions.add(new PartitionInfo(
                            metadata.topic,
                            partitionMetadata.partition,
                            partitionMetadata.leader,
                            partitionMetadata.replicas.toArray(new Node[0]),
                            partitionMetadata.isr.toArray(new Node[0])));
            } else if (metadata.error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                unauthorizedTopics.add(metadata.topic);
            }
        }

        return new Cluster(this.brokers, partitions, unauthorizedTopics);
    }

    /**
     * Get all brokers returned in metadata response
     * @return the brokers
     */
    public Collection<Node> brokers() {
        return brokers;
    }

    public static MetadataResponse parse(ByteBuffer buffer) {
        return new MetadataResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static class TopicMetadata {
        private final Errors error;
        private final String topic;
        private final List<PartitionMetadata> partitionMetadata;

        public TopicMetadata(Errors error,
                             String topic,
                             List<PartitionMetadata> partitionMetadata) {
            this.error = error;
            this.topic = topic;
            this.partitionMetadata = partitionMetadata;
        }

        public Errors error() {
            return error;
        }

        public String topic() {
            return topic;
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
