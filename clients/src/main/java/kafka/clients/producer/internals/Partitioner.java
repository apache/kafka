package kafka.clients.producer.internals;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.clients.producer.ProducerRecord;
import kafka.common.Cluster;
import kafka.common.PartitionInfo;
import kafka.common.utils.Utils;

/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * Compute the partition for the given record.
     * 
     * @param record The record being sent
     * @param numPartitions The total number of partitions for the given topic
     */
    public int partition(ProducerRecord record, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsFor(record.topic());
        int numPartitions = partitions.size();
        if (record.partition() != null) {
            // they have given us a partition, use it
            if (record.partition() < 0 || record.partition() >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + record.partition()
                                                   + " is not in the range [0..."
                                                   + numPartitions
                                                   + "].");
            return record.partition();
        } else if (record.key() == null) {
            // choose the next available node in a round-robin fashion
            for (int i = 0; i < numPartitions; i++) {
                int partition = Utils.abs(counter.getAndIncrement()) % numPartitions;
                if (partitions.get(partition).leader() != null)
                    return partition;
            }
            // no partitions are available, give a non-available partition
            return Utils.abs(counter.getAndIncrement()) % numPartitions;
        } else {
            // hash the key to choose a partition
            return Utils.abs(Utils.murmur2(record.key())) % numPartitions;
        }
    }

}
