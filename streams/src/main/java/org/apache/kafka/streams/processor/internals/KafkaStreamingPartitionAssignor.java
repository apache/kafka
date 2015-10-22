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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.PartitionGrouper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaStreamingPartitionAssignor implements PartitionAssignor, Configurable {

    private PartitionGrouper partitionGrouper;
    private Map<TopicPartition, Set<Integer>> partitionToTaskIds;

    @Override
    public void configure(Map<String, ?> configs) {
        Object o = configs.get(StreamingConfig.InternalConfig.PARTITION_GROUPER_INSTANCE);
        if (o == null)
            throw new KafkaException("PartitionGrouper is not specified");

        if (!PartitionGrouper.class.isInstance(o))
            throw new KafkaException(o.getClass().getName() + " is not an instance of " + PartitionGrouper.class.getName());

        partitionGrouper = (PartitionGrouper) o;
        partitionGrouper.partitionAssignor(this);
    }

    @Override
    public String name() {
        return "streaming";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        Map<Integer, List<TopicPartition>> partitionGroups = partitionGrouper.partitionGroups(metadata);

        String[] clientIds = subscriptions.keySet().toArray(new String[subscriptions.size()]);
        Integer[] taskIds = partitionGroups.keySet().toArray(new Integer[partitionGroups.size()]);

        Map<String, Assignment> assignment = new HashMap<>();

        for (int i = 0; i < clientIds.length; i++) {
            List<TopicPartition> partitions = new ArrayList<>();
            List<Integer> ids = new ArrayList<>();
            for (int j = i; j < taskIds.length; j += clientIds.length) {
                Integer taskId = taskIds[j];
                for (TopicPartition partition : partitionGroups.get(taskId)) {
                    partitions.add(partition);
                    ids.add(taskId);
                }
            }
            // encode task ids
            ByteBuffer buf = ByteBuffer.allocate(ids.size() * 4);
            for (Integer id : ids) {
                buf.putInt(id);
            }
            buf.rewind();
            assignment.put(clientIds[i], new Assignment(partitions, buf));
        }

        return assignment;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        List<TopicPartition> partitions = assignment.partitions();
        ByteBuffer data = assignment.userData();

        Map<TopicPartition, Set<Integer>> partitionToTaskIds = new HashMap<>();

        int i = 0;
        for (TopicPartition partition : partitions) {
            Set<Integer> taskIds = partitionToTaskIds.get(partition);
            if (taskIds == null) {
                taskIds = new HashSet<>();
                partitionToTaskIds.put(partition, taskIds);
            }
            // decode a task id
            data.rewind();
            taskIds.add(data.getInt(i * 4));
            i++;
        }
        this.partitionToTaskIds = partitionToTaskIds;
    }

    public Set<Integer> taskIds(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

}
