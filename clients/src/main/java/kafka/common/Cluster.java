package kafka.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.utils.Utils;

/**
 * A representation of the nodes, topics, and partitions in the Kafka cluster
 */
public final class Cluster {

    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<Node> nodes;
    private final Map<Integer, Node> nodesById;
    private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
    private final Map<String, List<PartitionInfo>> partitionsByTopic;

    /**
     * Create a new cluster with the given nodes and partitions
     * @param nodes The nodes in the cluster
     * @param partitions Information about a subset of the topic-partitions this cluster hosts
     */
    public Cluster(Collection<Node> nodes, Collection<PartitionInfo> partitions) {
        this.nodes = new ArrayList<Node>(nodes);
        this.nodesById = new HashMap<Integer, Node>(this.nodes.size());
        this.partitionsByTopicPartition = new HashMap<TopicPartition, PartitionInfo>(partitions.size());
        this.partitionsByTopic = new HashMap<String, List<PartitionInfo>>(partitions.size());

        Collections.shuffle(this.nodes);
        for (Node n : this.nodes)
            this.nodesById.put(n.id(), n);
        for (PartitionInfo p : partitions)
            this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
        for (PartitionInfo p : partitions) {
            if (!this.partitionsByTopic.containsKey(p.topic()))
                this.partitionsByTopic.put(p.topic(), new ArrayList<PartitionInfo>());
            List<PartitionInfo> ps = this.partitionsByTopic.get(p.topic());
            ps.add(p);
        }
    }

    /**
     * Create an empty cluster instance with no nodes and no topic-partitions.
     */
    public static Cluster empty() {
        return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0));
    }

    /**
     * Create a "bootstrap" cluster using the given list of host/ports
     * @param addresses The addresses
     * @return A cluster for these hosts/ports
     */
    public static Cluster bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<Node>();
        int nodeId = Integer.MIN_VALUE;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId++, address.getHostName(), address.getPort()));
        return new Cluster(nodes, new ArrayList<PartitionInfo>(0));
    }

    /**
     * Get the current leader for the given topic-partition
     * @param topicPartition The topic and partition we want to know the leader for
     * @return The node that is the leader for this topic-partition, or null if there is currently no leader
     */
    public Node leaderFor(TopicPartition topicPartition) {
        PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
        if (info == null)
            return null;
        else
            return nodesById.get(info.leader());
    }

    /**
     * Get the list of partitions for this topic
     * @param topic The topic name
     * @return A list of partitions
     */
    public List<PartitionInfo> partitionsFor(String topic) {
        return this.partitionsByTopic.get(topic);
    }

    /**
     * Round-robin over the nodes in this cluster
     */
    public Node nextNode() {
        int size = nodes.size();
        if (size == 0)
            throw new IllegalStateException("No known nodes.");
        int idx = Utils.abs(counter.getAndIncrement()) % size;
        return this.nodes.get(idx);
    }

}
