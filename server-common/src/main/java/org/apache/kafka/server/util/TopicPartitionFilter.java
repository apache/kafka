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
package org.apache.kafka.server.util;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.TopicFilter.IncludeList;

import java.util.List;

public interface TopicPartitionFilter {
    /**
     * Used to filter topics based on a certain criteria, for example, a set of topic names or a regular expression.
     */
    boolean isTopicAllowed(String topic);

    /**
     * Used to filter topic-partitions based on a certain criteria, for example, a topic pattern and a set of partition ids.
     */
    boolean isTopicPartitionAllowed(TopicPartition partition);

    class TopicFilterAndPartitionFilter implements TopicPartitionFilter {
        private final IncludeList topicFilter;
        private final PartitionFilter partitionFilter;
        public TopicFilterAndPartitionFilter(IncludeList topicFilter, PartitionFilter partitionFilter) {
            this.topicFilter = topicFilter;
            this.partitionFilter = partitionFilter;
        }

        @Override
        public boolean isTopicAllowed(String topic) {
            return topicFilter.isTopicAllowed(topic, false);
        }

        @Override
        public boolean isTopicPartitionAllowed(TopicPartition partition) {
            return isTopicAllowed(partition.topic()) && partitionFilter.isPartitionAllowed(partition.partition());
        }
    }

    class CompositeTopicPartitionFilter implements TopicPartitionFilter {
        private final List<TopicPartitionFilter> filters;

        public CompositeTopicPartitionFilter(List<TopicPartitionFilter> filters) {
            this.filters = filters;
        }

        @Override
        public boolean isTopicAllowed(String topic) {
            return filters.stream().anyMatch(tp -> tp.isTopicAllowed(topic));
        }

        @Override
        public boolean isTopicPartitionAllowed(TopicPartition partition) {
            return filters.stream().anyMatch(tp -> tp.isTopicPartitionAllowed(partition));
        }
    }
}
