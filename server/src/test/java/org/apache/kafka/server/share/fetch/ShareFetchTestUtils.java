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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helper functions for writing share fetch unit tests.
 */
public class ShareFetchTestUtils {

    /**
     * Create an ordered map of TopicIdPartition to partition max bytes.
     *
     * @param partitionMaxBytes The maximum number of bytes that can be fetched for each partition.
     * @param topicIdPartitions The topic partitions to create the map for.
     * @return The ordered map of TopicIdPartition to partition max bytes.
     */
    public static LinkedHashMap<TopicIdPartition, Integer> orderedMap(int partitionMaxBytes, TopicIdPartition... topicIdPartitions) {
        LinkedHashMap<TopicIdPartition, Integer> map = new LinkedHashMap<>();
        for (TopicIdPartition tp : topicIdPartitions) {
            map.put(tp, partitionMaxBytes);
        }
        return map;
    }

    /**
     * Validate that the rotated map is equal to the original map with the keys rotated by the given position.
     *
     * @param original The original map.
     * @param result The rotated map.
     * @param rotationAt The position to rotate the keys at.
     */
    public static void validateRotatedMapEquals(
        LinkedHashMap<TopicIdPartition, Integer> original,
        LinkedHashMap<TopicIdPartition, Integer> result,
        int rotationAt
    ) {
        Set<TopicIdPartition> originalKeys = original.keySet();
        Set<TopicIdPartition> resultKeys = result.keySet();

        TopicIdPartition[] originalKeysArray = new TopicIdPartition[originalKeys.size()];
        int i = 0;
        for (TopicIdPartition key : originalKeys) {
            if (i < rotationAt) {
                originalKeysArray[originalKeys.size() - rotationAt + i] = key;
            } else {
                originalKeysArray[i - rotationAt] = key;
            }
            i++;
        }
        assertArrayEquals(originalKeysArray, resultKeys.toArray());
        for (TopicIdPartition key : originalKeys) {
            assertEquals(original.get(key), result.get(key));
        }
    }
}
