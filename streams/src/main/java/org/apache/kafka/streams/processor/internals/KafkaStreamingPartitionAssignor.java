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
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaStreamingPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamingPartitionAssignor.class);

    private PartitionGrouper partitionGrouper;
    private Map<TopicPartition, Set<TaskId>> partitionToTaskIds;

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
        Map<TaskId, Set<TopicPartition>> partitionGroups = partitionGrouper.partitionGroups(metadata);

        String[] clientIds = subscriptions.keySet().toArray(new String[subscriptions.size()]);
        TaskId[] taskIds = partitionGroups.keySet().toArray(new TaskId[partitionGroups.size()]);

        Map<String, Assignment> assignment = new HashMap<>();

        for (int i = 0; i < clientIds.length; i++) {
            List<TopicPartition> partitions = new ArrayList<>();
            List<TaskId> ids = new ArrayList<>();
            for (int j = i; j < taskIds.length; j += clientIds.length) {
                TaskId taskId = taskIds[j];
                for (TopicPartition partition : partitionGroups.get(taskId)) {
                    partitions.add(partition);
                    ids.add(taskId);
                }
            }
            ByteBuffer buf = ByteBuffer.allocate(4 + ids.size() * 8);
            //version
            buf.putInt(1);
            // encode task ids
            for (TaskId id : ids) {
                buf.putInt(id.topicGroupId);
                buf.putInt(id.partition);
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
        data.rewind();

        Map<TopicPartition, Set<TaskId>> partitionToTaskIds = new HashMap<>();

        // check version
        int version = data.getInt();
        if (version == 1) {
            for (TopicPartition partition : partitions) {
                Set<TaskId> taskIds = partitionToTaskIds.get(partition);
                if (taskIds == null) {
                    taskIds = new HashSet<>();
                    partitionToTaskIds.put(partition, taskIds);
                }
                // decode a task id
                taskIds.add(new TaskId(data.getInt(), data.getInt()));
            }
        } else {
            KafkaException ex = new KafkaException("unknown assignment data version: " + version);
            log.error(ex.getMessage(), ex);
            throw ex;
        }
        this.partitionToTaskIds = partitionToTaskIds;
    }

    public Set<TaskId> taskIds(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

}
