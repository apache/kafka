package kafka.common;

/**
 * Information about a topic-partition.
 */
public class PartitionInfo {

    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;

    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
    }

    /**
     * The topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition id
     */
    public int partition() {
        return partition;
    }

    /**
     * The node id of the node currently acting as a leader for this partition or -1 if there is no leader
     */
    public Node leader() {
        return leader;
    }

    /**
     * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
     */
    public Node[] replicas() {
        return replicas;
    }

    /**
     * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if
     * the leader should fail
     */
    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }

}
