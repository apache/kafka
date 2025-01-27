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
import java.util.Map;

/**
 * The PartitionRotateStrategy is used to rotate the partitions based on the respective strategy.
 * The share-partitions are rotated to ensure no share-partitions are starved from records being fetched.
 */
public interface PartitionRotateStrategy {

    /**
     * The strategy type to rotate the partitions.
     */
    enum StrategyType {
        ROUND_ROBIN;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Rotate the partitions based on the strategy.
     *
     * @param topicIdPartitions the topicIdPartitions to rotate
     * @param metadata the metadata to rotate
     *
     * @return the rotated topicIdPartitions
     */
    LinkedHashMap<TopicIdPartition, Integer> rotate(LinkedHashMap<TopicIdPartition, Integer> topicIdPartitions, PartitionRotateMetadata metadata);

    static PartitionRotateStrategy type(StrategyType type) {
        return switch (type) {
            case ROUND_ROBIN -> PartitionRotateStrategy::rotateRoundRobin;
        };
    }

    /**
     * Rotate the partitions based on the round-robin strategy.
     *
     * @param topicIdPartitions the topicIdPartitions to rotate
     * @param metadata the metadata to rotate
     *
     * @return the rotated topicIdPartitions
     */
    static LinkedHashMap<TopicIdPartition, Integer> rotateRoundRobin(
        LinkedHashMap<TopicIdPartition, Integer> topicIdPartitions,
        PartitionRotateMetadata metadata
    ) {
        if (topicIdPartitions.isEmpty() || topicIdPartitions.size() == 1 || metadata.sessionEpoch < 1) {
            // No need to rotate the partitions if there are no partitions, only one partition or the
            // session epoch is initial or final.
            return topicIdPartitions;
        }

        int rotateAt = metadata.sessionEpoch % topicIdPartitions.size();
        if (rotateAt == 0) {
            // No need to rotate the partitions if the rotateAt is 0.
            return topicIdPartitions;
        }

        // TODO: Once the partition max bytes is removed then the partition will be a linked list and rotation
        //  will be a simple operation. Else consider using ImplicitLinkedHashCollection.
        LinkedHashMap<TopicIdPartition, Integer> suffixPartitions = new LinkedHashMap<>(rotateAt);
        LinkedHashMap<TopicIdPartition, Integer> rotatedPartitions = new LinkedHashMap<>(topicIdPartitions.size());
        int i = 0;
        for (Map.Entry<TopicIdPartition, Integer> entry : topicIdPartitions.entrySet()) {
            if (i < rotateAt) {
                suffixPartitions.put(entry.getKey(), entry.getValue());
            } else {
                rotatedPartitions.put(entry.getKey(), entry.getValue());
            }
            i++;
        }
        rotatedPartitions.putAll(suffixPartitions);
        return rotatedPartitions;
    }

    /**
     * The partition rotate metadata which can be used to store the metadata for the partition rotation.
     *
     * @param sessionEpoch the share session epoch.
     */
    record PartitionRotateMetadata(int sessionEpoch) { }
}
