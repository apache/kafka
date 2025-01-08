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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Set;

public class PartitionMaxBytesDivisionStrategy {

    public enum StrategyType {
        EQUAL_DIVISION;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
    private final StrategyType type;

    public PartitionMaxBytesDivisionStrategy(StrategyType type) {
        if (type == null) {
            throw new IllegalArgumentException("Partition bytes division strategy is null");
        }
        this.type = type;
    }

    /**
     * Returns the partition max bytes for a given partition based on the strategy type.
     *
     * @param requestMaxBytes - The total max bytes available for the share fetch request
     * @param partitions - The topic partitions in the order for which we compute the partition max bytes.
     * @param acquiredPartitionsSize - The total partitions that have been acquired.
     * @return the partition max bytes for the topic partitions
     */
    public LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes(int requestMaxBytes, Set<TopicIdPartition> partitions, int acquiredPartitionsSize) {
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("Partitions to generate max bytes is null or empty");
        }
        if (requestMaxBytes <= 0) {
            throw new IllegalArgumentException("Requested max bytes must be greater than 0");
        }
        if (acquiredPartitionsSize <= 0) {
            throw new IllegalArgumentException("Acquired partitions size must be greater than 0");
        }

        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes = new LinkedHashMap<>();
        if (type == StrategyType.EQUAL_DIVISION) {
            partitions.forEach(partition -> partitionMaxBytes.put(
                partition, requestMaxBytes / acquiredPartitionsSize));
        }
        return partitionMaxBytes;
    }

    // Visible for testing
    protected StrategyType type() {
        return type;
    }
}
