package kafka.clients.producer;

import kafka.common.Cluster;

/**
 * An interface by which clients can override the default partitioning behavior that maps records to topic partitions.
 * <p>
 * A partitioner can use either the original java object the user provided or the serialized bytes.
 * <p>
 * It is expected that the partitioner will make use the key for partitioning, but there is no requirement that an
 * implementation do so. An implementation can use the key, the value, the state of the cluster, or any other side data.
 */
public interface Partitioner {

    /**
     * Compute the partition for the given record. This partition number must be in the range [0...numPartitions). The
     * cluster state provided is the most up-to-date view that the client has but leadership can change at any time so
     * there is no guarantee that the node that is the leader for a particular partition at the time the partition
     * function is called will still be the leader by the time the request is sent.
     * 
     * @param record The record being sent
     * @param key The serialized bytes of the key (null if no key is given or the serialized form is null)
     * @param value The serialized bytes of the value (null if no value is given or the serialized form is null)
     * @param cluster The current state of the cluster
     * @param numPartitions The total number of partitions for the given topic
     * @return The partition to send this record to
     */
    public int partition(ProducerRecord record, byte[] key, byte[] partitionKey, byte[] value, Cluster cluster, int numPartitions);

}
