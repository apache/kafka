package kafka.common;

/**
 * Information about a topic-partition.
 */
public class PartitionInfo {

    private final String topic;
    private final int partition;
    private final int leader;
    private final int[] replicas;
    private final int[] inSyncReplicas;

    public PartitionInfo(String topic, int partition, int leader, int[] replicas, int[] inSyncReplicas) {
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
    public int leader() {
        return leader;
    }

    /**
     * The complete set of replicas for this partition regardless of whether they are alive or up-to-date
     */
    public int[] replicas() {
        return replicas;
    }

    /**
     * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if
     * the leader should fail
     */
    public int[] inSyncReplicas() {
        return inSyncReplicas;
    }

}
