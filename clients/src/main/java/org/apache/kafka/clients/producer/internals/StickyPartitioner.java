/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.utils.Utils;

public class StickyPartitioner {
    private final ConcurrentMap<String, Integer> indexCache;
    public StickyPartitioner() {
        this.indexCache = new ConcurrentHashMap<>();
    }

    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 1) {
            indexCache.put(topic, 0);
            return 0;
        } else if (indexCache.get(topic) == null || prevPartition == indexCache.get(topic)) {
            Integer part = indexCache.get(topic);
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
            if (availablePartitions.size() < 1) {
                part = random % numPartitions;
            } else if (availablePartitions.size() == 1) {
                part = availablePartitions.get(0).partition();
            } else {
                while (part == indexCache.get(topic)) {
                    random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    part = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }   
            indexCache.put(topic, part);
            return part;
        }
        return indexCache.get(topic);
    }

}