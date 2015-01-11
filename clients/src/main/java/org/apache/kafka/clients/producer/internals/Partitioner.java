/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;


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
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param partition The partition to use (or null if none)
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, byte[] key, Integer partition, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (partition != null) {
            // they have given us a partition, use it
            if (partition < 0 || partition >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + partition
                                                   + " is not in the range [0..."
                                                   + numPartitions
                                                   + "].");
            return partition;
        } else if (key == null) {
            // choose the next available node in a round-robin fashion
            for (int i = 0; i < numPartitions; i++) {
                int part = Utils.abs(counter.getAndIncrement()) % numPartitions;
                if (partitions.get(part).leader() != null)
                    return part;
            }
            // no partitions are available, give a non-available partition
            return Utils.abs(counter.getAndIncrement()) % numPartitions;
        } else {
            // hash the key to choose a partition
            return Utils.abs(Utils.murmur2(key)) % numPartitions;
        }
    }

}
