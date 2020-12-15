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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Used for testing the assignment of a subset of a topology group, not the entire topology
 */
@SuppressWarnings("deprecation")
public class SingleGroupPartitionGrouperStub implements org.apache.kafka.streams.processor.PartitionGrouper {
    private org.apache.kafka.streams.processor.PartitionGrouper defaultPartitionGrouper =
        new org.apache.kafka.streams.processor.DefaultPartitionGrouper();

    @Override
    public Map<TaskId, Set<TopicPartition>> partitionGroups(final Map<Integer, Set<String>> topicGroups, final Cluster metadata) {
        final Map<Integer, Set<String>> includedTopicGroups = new HashMap<>();

        for (final Map.Entry<Integer, Set<String>> entry : topicGroups.entrySet()) {
            includedTopicGroups.put(entry.getKey(), entry.getValue());
            break; // arbitrarily use the first entry only
        }
        return defaultPartitionGrouper.partitionGroups(includedTopicGroups, metadata);
    }
}
