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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public final class Utils {

    public static String sortedTopicPartitions(Collection<TopicPartition> partitions) {
        SortedSet<TopicPartition> sortedPartitions;

        if (partitions instanceof SortedSet) {
            sortedPartitions = (SortedSet<TopicPartition>) partitions;
        } else {
            sortedPartitions = new TreeSet<>(new TopicPartitionComparator());
            sortedPartitions.addAll(partitions);
        }

        return join(sortedPartitions);
    }

    private static String join(SortedSet<?> elements) {
        StringBuilder b = new StringBuilder();

        for (Object o : elements) {
            if (b.length() > 0)
                b.append(", ");

            b.append(o.toString());
        }

        return b.toString();
    }

    static final class PartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Map<String, List<String>> map;

        PartitionComparator(Map<String, List<String>> map) {
            this.map = map;
        }

        @Override
        public int compare(TopicPartition o1, TopicPartition o2) {
            int ret = map.get(o1.topic()).size() - map.get(o2.topic()).size();
            if (ret == 0) {
                ret = o1.topic().compareTo(o2.topic());
                if (ret == 0)
                    ret = o1.partition() - o2.partition();
            }
            return ret;
        }
    }

    public static final class TopicPartitionComparator implements Comparator<TopicPartition>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(TopicPartition topicPartition1, TopicPartition topicPartition2) {
            String topic1 = topicPartition1.topic();
            String topic2 = topicPartition2.topic();

            if (topic1.equals(topic2)) {
                return topicPartition1.partition() - topicPartition2.partition();
            } else {
                return topic1.compareTo(topic2);
            }
        }
    }

    public static final class TopicIdPartitionComparator implements Comparator<TopicIdPartition>, Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * Comparison based on topic name and partition number.
         */
        @Override
        public int compare(TopicIdPartition topicPartition1, TopicIdPartition topicPartition2) {
            String topic1 = topicPartition1.topic();
            String topic2 = topicPartition2.topic();

            if (topic1.equals(topic2)) {
                return topicPartition1.partition() - topicPartition2.partition();
            } else {
                return topic1.compareTo(topic2);
            }
        }
    }
}
