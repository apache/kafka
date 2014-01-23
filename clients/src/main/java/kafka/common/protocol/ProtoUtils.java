package kafka.common.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.common.Cluster;
import kafka.common.Node;
import kafka.common.PartitionInfo;
import kafka.common.protocol.types.Schema;
import kafka.common.protocol.types.Struct;

public class ProtoUtils {

    private static Schema schemaFor(Schema[][] schemas, int apiKey, int version) {
        if (apiKey < 0 || apiKey > schemas.length)
            throw new IllegalArgumentException("Invalid api key: " + apiKey);
        Schema[] versions = schemas[apiKey];
        if (version < 0 || version > versions.length)
            throw new IllegalArgumentException("Invalid version for API key " + apiKey + ": " + version);
        return versions[version];
    }

    public static short latestVersion(int apiKey) {
        if (apiKey < 0 || apiKey >= Protocol.CURR_VERSION.length)
            throw new IllegalArgumentException("Invalid api key: " + apiKey);
        return Protocol.CURR_VERSION[apiKey];
    }

    public static Schema requestSchema(int apiKey, int version) {
        return schemaFor(Protocol.REQUESTS, apiKey, version);
    }

    public static Schema currentRequestSchema(int apiKey) {
        return requestSchema(apiKey, latestVersion(apiKey));
    }

    public static Schema responseSchema(int apiKey, int version) {
        return schemaFor(Protocol.RESPONSES, apiKey, version);
    }

    public static Schema currentResponseSchema(int apiKey) {
        return schemaFor(Protocol.RESPONSES, apiKey, latestVersion(apiKey));
    }

    public static Struct parseRequest(int apiKey, int version, ByteBuffer buffer) {
        return (Struct) requestSchema(apiKey, version).read(buffer);
    }

    public static Struct parseResponse(int apiKey, ByteBuffer buffer) {
        return (Struct) currentResponseSchema(apiKey).read(buffer);
    }

    public static Cluster parseMetadataResponse(Struct response) {
        List<Node> brokers = new ArrayList<Node>();
        Object[] brokerStructs = (Object[]) response.get("brokers");
        for (int i = 0; i < brokerStructs.length; i++) {
            Struct broker = (Struct) brokerStructs[i];
            int nodeId = (Integer) broker.get("node_id");
            String host = (String) broker.get("host");
            int port = (Integer) broker.get("port");
            brokers.add(new Node(nodeId, host, port));
        }
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        Object[] topicInfos = (Object[]) response.get("topic_metadata");
        for (int i = 0; i < topicInfos.length; i++) {
            Struct topicInfo = (Struct) topicInfos[i];
            short topicError = topicInfo.getShort("topic_error_code");
            if (topicError == Errors.NONE.code()) {
                String topic = topicInfo.getString("topic");
                Object[] partitionInfos = (Object[]) topicInfo.get("partition_metadata");
                for (int j = 0; j < partitionInfos.length; j++) {
                    Struct partitionInfo = (Struct) partitionInfos[j];
                    short partError = partitionInfo.getShort("partition_error_code");
                    if (partError == Errors.NONE.code()) {
                        int partition = partitionInfo.getInt("partition_id");
                        int leader = partitionInfo.getInt("leader");
                        int[] replicas = intArray((Object[]) partitionInfo.get("replicas"));
                        int[] isr = intArray((Object[]) partitionInfo.get("isr"));
                        partitions.add(new PartitionInfo(topic, partition, leader, replicas, isr));
                    }
                }
            }
        }
        return new Cluster(brokers, partitions);
    }

    private static int[] intArray(Object[] ints) {
        int[] copy = new int[ints.length];
        for (int i = 0; i < ints.length; i++)
            copy[i] = (Integer) ints[i];
        return copy;
    }

}
