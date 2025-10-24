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
package org.apache.kafka.server.share.fetch;

import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

/**
 * This interface helps identify the max bytes for topic partitions in a share fetch request based on different strategy types.
 */
public interface PartitionMaxBytesStrategy {
    Random RANDOM = new Random();

    enum StrategyType {
        UNIFORM;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Returns the partition max bytes for a given partition based on the strategy type.
     * The partitions passed for maxBytes calculation are a subset of total acquired partitions for the share fetch request.
     * Thus, partitions for which we want to compute the max bytes <= acquired partitions.
     *
     * @param requestMaxBytes - The total max bytes available for the share fetch request
     * @param partitions - The topic partitions in the order for which we compute the partition max bytes.
     * @param acquiredPartitionsSize - The total partitions that have been acquired.
     * @return the partition max bytes for the topic partitions
     */
    LinkedHashMap<TopicIdPartition, Integer> maxBytes(int requestMaxBytes, Set<TopicIdPartition> partitions, int acquiredPartitionsSize);

    static PartitionMaxBytesStrategy type(StrategyType type) {
        if (type == null)
            throw new IllegalArgumentException("Strategy type cannot be null");
        return switch (type) {
            case UNIFORM -> PartitionMaxBytesStrategy::uniformPartitionMaxBytes;
        };
    }


    private static LinkedHashMap<TopicIdPartition, Integer> uniformPartitionMaxBytes(int requestMaxBytes, Set<TopicIdPartition> partitions, int acquiredPartitionsSize) {
        checkValidArguments(requestMaxBytes, partitions, acquiredPartitionsSize);
        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = new LinkedHashMap<>();
        if (requestMaxBytes >= acquiredPartitionsSize) {
            // Case 1: requestMaxBytes can be evenly distributed within partitions. If there is extra bytes left post
            // dividing it uniformly, assign it randomly to any one of the partitions.
            partitions.forEach(partition -> partitionMaxBytes.put(partition, requestMaxBytes / acquiredPartitionsSize));
            if (requestMaxBytes % acquiredPartitionsSize != 0) {
                TopicIdPartition randomPartition = selectPartitionRandomly(partitionMaxBytes);
                partitionMaxBytes.put(randomPartition,
                    (requestMaxBytes / acquiredPartitionsSize) + (requestMaxBytes % acquiredPartitionsSize));
            }
        } else if (requestMaxBytes >= partitions.size()) {
            // Case 2: we will be distributing requestMaxBytes greedily in this scenario to prevent any starvation. If
            // there is extra bytes left post dividing it uniformly, assign it randomly to any one of the partitions.
            partitions.forEach(partition -> partitionMaxBytes.put(partition, requestMaxBytes / partitions.size()));
            if (requestMaxBytes % partitions.size() != 0) {
                TopicIdPartition randomPartition = selectPartitionRandomly(partitionMaxBytes);
                partitionMaxBytes.put(randomPartition,
                    (requestMaxBytes / partitions.size()) + (requestMaxBytes % partitions.size()));
            }
        } else {
            // Case 3: we will distribute requestMaxBytes to as many partitions possible randomly to avoid starvation.
            List<TopicIdPartition> partitionsList = new ArrayList<>(partitions);
            Collections.shuffle(partitionsList);
            Set<TopicIdPartition> nonEmptyPartitions = new HashSet<>(partitionsList.subList(0, requestMaxBytes));
            partitions.forEach(
                partition -> {
                    if (nonEmptyPartitions.contains(partition)) {
                        partitionMaxBytes.put(partition, 1);
                    } else {
                        partitionMaxBytes.put(partition, 0);
                    }
                }
            );
        }
        return partitionMaxBytes;
    }

    private static TopicIdPartition selectPartitionRandomly(LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        List<TopicIdPartition> partitionsList = new ArrayList<>(partitionMaxBytes.keySet());
        return partitionsList.get(RANDOM.nextInt(partitionsList.size()));
    }

    // Visible for testing.
    static void checkValidArguments(int requestMaxBytes, Set<TopicIdPartition> partitions, int acquiredPartitionsSize) {
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("Partitions to generate max bytes is null or empty");
        }
        if (requestMaxBytes <= 0) {
            throw new IllegalArgumentException("Request max bytes must be greater than 0");
        }
        if (acquiredPartitionsSize <= 0) {
            throw new IllegalArgumentException("Acquired partitions size must be greater than 0");
        }
    }
}
