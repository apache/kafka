package org.apache.kafka.common.requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class MetadataResponse {

    private final Cluster cluster;
    private final Map<String, Errors> errors;

    public MetadataResponse(Cluster cluster, Map<String, Errors> errors) {
        this.cluster = cluster;
        this.errors = errors;
    }

    public MetadataResponse(Struct struct) {
        Map<String, Errors> errors = new HashMap<String, Errors>();
        Map<Integer, Node> brokers = new HashMap<Integer, Node>();
        Object[] brokerStructs = (Object[]) struct.get("brokers");
        for (int i = 0; i < brokerStructs.length; i++) {
            Struct broker = (Struct) brokerStructs[i];
            int nodeId = (Integer) broker.get("node_id");
            String host = (String) broker.get("host");
            int port = (Integer) broker.get("port");
            brokers.put(nodeId, new Node(nodeId, host, port));
        }
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Object[] topicInfos = (Object[]) struct.get("topic_metadata");
        for (int i = 0; i < topicInfos.length; i++) {
            Struct topicInfo = (Struct) topicInfos[i];
            short topicError = topicInfo.getShort("topic_error_code");
            String topic = topicInfo.getString("topic");
            if (topicError == Errors.NONE.code()) {
                Object[] partitionInfos = (Object[]) topicInfo.get("partition_metadata");
                for (int j = 0; j < partitionInfos.length; j++) {
                    Struct partitionInfo = (Struct) partitionInfos[j];
                    short partError = partitionInfo.getShort("partition_error_code");
                    if (partError == Errors.NONE.code()) {
                        int partition = partitionInfo.getInt("partition_id");
                        int leader = partitionInfo.getInt("leader");
                        Node leaderNode = leader == -1 ? null : brokers.get(leader);
                        Object[] replicas = (Object[]) partitionInfo.get("replicas");
                        Node[] replicaNodes = new Node[replicas.length];
                        for (int k = 0; k < replicas.length; k++)
                            replicaNodes[k] = brokers.get(replicas[k]);
                        Object[] isr = (Object[]) partitionInfo.get("isr");
                        Node[] isrNodes = new Node[isr.length];
                        for (int k = 0; k < isr.length; k++)
                            isrNodes[k] = brokers.get(isr[k]);
                        partitions.add(new PartitionInfo(topic, partition, leaderNode, replicaNodes, isrNodes));
                    }
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

}
