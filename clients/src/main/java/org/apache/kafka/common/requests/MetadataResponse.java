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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

public class MetadataResponse extends AbstractRequestResponse {
    private static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.METADATA.id);
    private static String BROKERS_KEY_NAME = "brokers";
    private static String TOPIC_METATDATA_KEY_NAME = "topic_metadata";

    // broker level field names
    private static String NODE_ID_KEY_NAME = "node_id";
    private static String HOST_KEY_NAME = "host";
    private static String PORT_KEY_NAME = "port";

    // topic level field names
    private static String TOPIC_ERROR_CODE_KEY_NAME = "topic_error_code";
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITION_METADATA_KEY_NAME = "partition_metadata";

    // partition level field names
    private static String PARTITION_ERROR_CODE_KEY_NAME = "partition_error_code";
    private static String PARTITION_KEY_NAME = "partition_id";
    private static String LEADER_KEY_NAME = "leader";
    private static String REPLICAS_KEY_NAME = "replicas";
    private static String ISR_KEY_NAME = "isr";

    private final Cluster cluster;
    private final Map<String, Errors> errors;

    public MetadataResponse(Cluster cluster) {
        super(new Struct(curSchema));

        List<Struct> brokerArray = new ArrayList<Struct>();
        for (Node node: cluster.nodes()) {
            Struct broker = struct.instance(BROKERS_KEY_NAME);
            broker.set(NODE_ID_KEY_NAME, node.id());
            broker.set(HOST_KEY_NAME, node.host());
            broker.set(PORT_KEY_NAME, node.port());
            brokerArray.add(broker);
        }
        struct.set(BROKERS_KEY_NAME, brokerArray.toArray());

        List<Struct> topicArray = new ArrayList<Struct>();
        for (String topic: cluster.topics()) {
            Struct topicData = struct.instance(TOPIC_METATDATA_KEY_NAME);
            topicData.set(TOPIC_ERROR_CODE_KEY_NAME, (short)0);  // no error
            topicData.set(TOPIC_KEY_NAME, topic);
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (PartitionInfo fetchPartitionData : cluster.partitionsForTopic(topic)) {
                Struct partitionData = topicData.instance(PARTITION_METADATA_KEY_NAME);
                partitionData.set(PARTITION_ERROR_CODE_KEY_NAME, (short)0);  // no error
                partitionData.set(PARTITION_KEY_NAME, fetchPartitionData.partition());
                partitionData.set(LEADER_KEY_NAME, fetchPartitionData.leader().id());
                ArrayList<Integer> replicas = new ArrayList<Integer>();
                for (Node node: fetchPartitionData.replicas())
                    replicas.add(node.id());
                partitionData.set(REPLICAS_KEY_NAME, replicas.toArray());
                ArrayList<Integer> isr = new ArrayList<Integer>();
                for (Node node: fetchPartitionData.inSyncReplicas())
                    isr.add(node.id());
                partitionData.set(ISR_KEY_NAME, isr.toArray());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITION_METADATA_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPIC_METATDATA_KEY_NAME, topicArray.toArray());

        this.cluster = cluster;
        this.errors = new HashMap<String, Errors>();
    }

    public MetadataResponse(Struct struct) {
        super(struct);
        Map<String, Errors> errors = new HashMap<String, Errors>();
        Map<Integer, Node> brokers = new HashMap<Integer, Node>();
        Object[] brokerStructs = (Object[]) struct.get(BROKERS_KEY_NAME);
        for (int i = 0; i < brokerStructs.length; i++) {
            Struct broker = (Struct) brokerStructs[i];
            int nodeId = broker.getInt(NODE_ID_KEY_NAME);
            String host = broker.getString(HOST_KEY_NAME);
            int port = broker.getInt(PORT_KEY_NAME);
            brokers.put(nodeId, new Node(nodeId, host, port));
        }
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Object[] topicInfos = (Object[]) struct.get(TOPIC_METATDATA_KEY_NAME);
        for (int i = 0; i < topicInfos.length; i++) {
            Struct topicInfo = (Struct) topicInfos[i];
            short topicError = topicInfo.getShort(TOPIC_ERROR_CODE_KEY_NAME);
            String topic = topicInfo.getString(TOPIC_KEY_NAME);
            if (topicError == Errors.NONE.code()) {
                Object[] partitionInfos = (Object[]) topicInfo.get(PARTITION_METADATA_KEY_NAME);
                for (int j = 0; j < partitionInfos.length; j++) {
                    Struct partitionInfo = (Struct) partitionInfos[j];
                    int partition = partitionInfo.getInt(PARTITION_KEY_NAME);
                    int leader = partitionInfo.getInt(LEADER_KEY_NAME);
                    Node leaderNode = leader == -1 ? null : brokers.get(leader);
                    Object[] replicas = (Object[]) partitionInfo.get(REPLICAS_KEY_NAME);
                    Node[] replicaNodes = new Node[replicas.length];
                    for (int k = 0; k < replicas.length; k++)
                        replicaNodes[k] = brokers.get(replicas[k]);
                    Object[] isr = (Object[]) partitionInfo.get(ISR_KEY_NAME);
                    Node[] isrNodes = new Node[isr.length];
                    for (int k = 0; k < isr.length; k++)
                        isrNodes[k] = brokers.get(isr[k]);
                    partitions.add(new PartitionInfo(topic, partition, leaderNode, replicaNodes, isrNodes));
                }
            } else {
                errors.put(topic, Errors.forCode(topicError));
            }
        }
        this.errors = errors;
        this.cluster = new Cluster(brokers.values(), partitions);
    }

    public Map<String, Errors> errors() {
        return this.errors;
    }

    public Cluster cluster() {
        return this.cluster;
    }

    public static MetadataResponse parse(ByteBuffer buffer) {
        return new MetadataResponse(((Struct) curSchema.read(buffer)));
    }
}
