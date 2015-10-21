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

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DefaultPartitionGrouper extends PartitionGrouper {

    public Map<Long, List<TopicPartition>> partitionGroups(Cluster metadata) {
        Map<Long, List<TopicPartition>> groups = new HashMap<>();
        List<List<String>> sortedTopicGroups = sort(topicGroups);

        long groupId = 0;
        for (List<String> topicGroup : sortedTopicGroups) {
            for (String topic : topicGroup) {
                List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

                if (infos == null)
                    throw new KafkaException("topic not found :" + topic);

                int numPartitions = infos.size();

                for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                    long taskId = (groupId << 32) | (long) partitionId;

                    List<TopicPartition> group = groups.get(taskId);

                    if (group == null) {
                        group = new ArrayList<>(topicGroup.size());
                        groups.put(taskId, group);
                    }
                    group.add(new TopicPartition(topic, partitionId));
                }
            }
            groupId++;
        }

        // make the data unmodifiable, then return
        Map<Long, List<TopicPartition>> unmodifiableGroups = new HashMap<>();
        for (Map.Entry<Long, List<TopicPartition>> entry : groups.entrySet()) {
            unmodifiableGroups.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(unmodifiableGroups);
    }

    protected List<List<String>> sort(Collection<Set<String>> topicGroups) {
        TreeMap<String, String[]> sortedMap = new TreeMap<>();

        for (Set<String> group : topicGroups) {
            String[] arr = group.toArray(new String[group.size()]);
            Arrays.sort(arr);
            sortedMap.put(arr[0], arr);
        }

        ArrayList<List<String>> list = new ArrayList(sortedMap.size());
        for (String[] arr : sortedMap.values()) {
            list.add(Arrays.asList(arr));
        }

        return list;
    }

}
