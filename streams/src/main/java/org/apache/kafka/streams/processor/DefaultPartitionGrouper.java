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
import java.util.Iterator;
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
            int numPartitions = ensureCopartitioning(topicGroup, metadata);

            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                long taskId = (groupId << 32) | (long) partitionId;

                ArrayList<TopicPartition> group = new ArrayList<>(topicGroup.size());
                for (String topic : topicGroup) {
                    group.add(new TopicPartition(topic, partitionId));
                }

                groups.put(taskId, Collections.unmodifiableList(group));
            }
            groupId++;
        }

        return Collections.unmodifiableMap(groups);
    }

    protected int ensureCopartitioning(List<String> topicGroup, Cluster metadata) {
        int numPartitions = -1;

        for (String topic : topicGroup) {
            List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

            if (infos == null)
                throw new KafkaException("topic not found :" + topic);

            if (numPartitions == -1) {
                numPartitions = infos.size();
            } else if (numPartitions != infos.size()) {
                throw new KafkaException("not copartitioned : [" + toString(topicGroup, ", ") + "]");
            }
        }
        return numPartitions;
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

    protected <T> CharSequence toString(Collection<T> set, String separator) {
        StringBuilder sb = new StringBuilder();
        Iterator<T> iter = set.iterator();
        if (iter.hasNext()) {
            sb.append(iter.next().toString());

            while (iter.hasNext()) {
                sb.append(separator);
                sb.append(iter.next().toString());
            }
        }
        return sb;
    }
}
