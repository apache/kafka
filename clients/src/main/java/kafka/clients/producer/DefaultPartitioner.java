package kafka.clients.producer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.common.Cluster;
import kafka.common.utils.Utils;

/**
 * A simple partitioning strategy that will work for messages with or without keys.
 * <p>
 * If there is a partition key specified in the record the partitioner will use that for partitioning. Otherwise, if
 * there there is no partitionKey but there is a normal key that will be used. If neither key is specified the
 * partitioner will round-robin over partitions in the topic.
 * <p>
 * For the cases where there is some key present the partition is computed based on the murmur2 hash of the serialized
 * key.
 */
public class DefaultPartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * Compute the partition
     */
    @Override
    public int partition(ProducerRecord record, byte[] key, byte[] partitionKey, byte[] value, Cluster cluster, int numPartitions) {
        byte[] keyToUse = partitionKey != null ? partitionKey : key;
        if (keyToUse == null)
            return Utils.abs(counter.getAndIncrement()) % numPartitions;
        else
            return Utils.abs(Utils.murmur2(keyToUse)) % numPartitions;
    }

}
