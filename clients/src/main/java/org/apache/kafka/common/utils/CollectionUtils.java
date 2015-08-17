/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionUtils {
    /**
     * group data by topic
     * @param data Data to be partitioned
     * @param <T> Partition data type
     * @return partitioned data
     */
    public static <T> Map<String, Map<Integer, T>> groupDataByTopic(Map<TopicPartition, T> data) {
        Map<String, Map<Integer, T>> dataByTopic = new HashMap<String, Map<Integer, T>>();
        for (Map.Entry<TopicPartition, T> entry: data.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            Map<Integer, T> topicData = dataByTopic.get(topic);
            if (topicData == null) {
                topicData = new HashMap<Integer, T>();
                dataByTopic.put(topic, topicData);
            }
            topicData.put(partition, entry.getValue());
        }
        return dataByTopic;
    }

    /**
     * group partitions by topic
     * @param partitions
     * @return partitions per topic
     */
    public static Map<String, List<Integer>> groupDataByTopic(List<TopicPartition> partitions) {
        Map<String, List<Integer>> partitionsByTopic = new HashMap<String, List<Integer>>();
        for (TopicPartition tp: partitions) {
            String topic = tp.topic();
            List<Integer> topicData = partitionsByTopic.get(topic);
            if (topicData == null) {
                topicData = new ArrayList<Integer>();
                partitionsByTopic.put(topic, topicData);
            }
            topicData.add(tp.partition());
        }
        return  partitionsByTopic;
    }
}
