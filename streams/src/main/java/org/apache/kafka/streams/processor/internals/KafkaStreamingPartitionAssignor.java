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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class KafkaStreamingPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamingPartitionAssignor.class);

    private StreamThread streamThread;
    private int numStandbyReplicas;
    private Map<TopicPartition, Set<TaskId>> partitionToTaskIds;
    private Set<TaskId> standbyTasks;

    @Override
    public void configure(Map<String, ?> configs) {
        numStandbyReplicas = (Integer) configs.get(StreamingConfig.NUM_STANDBY_REPLICAS_CONFIG);

        Object o = configs.get(StreamingConfig.InternalConfig.STREAM_THREAD_INSTANCE);
        if (o == null) {
            KafkaException ex = new KafkaException("StreamThread is not specified");
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        if (!(o instanceof StreamThread)) {
            KafkaException ex = new KafkaException(o.getClass().getName() + " is not an instance of " + StreamThread.class.getName());
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        streamThread = (StreamThread) o;
        streamThread.partitionGrouper.partitionAssignor(this);
    }

    @Override
    public String name() {
        return "streaming";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreaming)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        Set<TaskId> prevTasks = streamThread.prevTasks();
        Set<TaskId> standbyTasks = streamThread.cachedTasks();
        standbyTasks.removeAll(prevTasks);
        SubscriptionInfo data = new SubscriptionInfo(streamThread.clientUUID, prevTasks, standbyTasks);

        return new Subscription(new ArrayList<>(topics), data.encode());
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // This assigns tasks to consumer clients in two steps.
        // 1. using TaskAssignor tasks are assigned to streaming clients.
        //    - Assign a task to a client which was running it previously.
        //      If there is no such client, assign a task to a client which has its valid local state.
        //    - A client may have more than one stream threads.
        //      The assignor tries to assign tasks to a client proportionally to the number of threads.
        //    - We try not to assign the same set of tasks to two different clients
        //    We do the assignment in one-pass. The result may not satisfy above all.
        // 2. within each client, tasks are assigned to consumer clients in round-robin manner.
        Map<UUID, Set<String>> consumersByClient = new HashMap<>();
        Map<UUID, ClientState<TaskId>> states = new HashMap<>();

        // Decode subscription info
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            Subscription subscription = entry.getValue();

            SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());

            Set<String> consumers = consumersByClient.get(info.clientUUID);
            if (consumers == null) {
                consumers = new HashSet<>();
                consumersByClient.put(info.clientUUID, consumers);
            }
            consumers.add(consumerId);

            ClientState<TaskId> state = states.get(info.clientUUID);
            if (state == null) {
                state = new ClientState<>();
                states.put(info.clientUUID, state);
            }

            state.prevActiveTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.standbyTasks);
            state.capacity = state.capacity + 1d;
        }

        // Get partition groups from the partition grouper
        Map<TaskId, Set<TopicPartition>> partitionGroups = streamThread.partitionGrouper.partitionGroups(metadata);

        states = TaskAssignor.assign(states, partitionGroups.keySet(), numStandbyReplicas);
        Map<String, Assignment> assignment = new HashMap<>();

        for (Map.Entry<UUID, Set<String>> entry : consumersByClient.entrySet()) {
            UUID uuid = entry.getKey();
            Set<String> consumers = entry.getValue();
            ClientState<TaskId> state = states.get(uuid);

            ArrayList<TaskId> taskIds = new ArrayList<>(state.assignedTasks.size());
            final int numActiveTasks = state.activeTasks.size();
            for (TaskId id : state.activeTasks) {
                taskIds.add(id);
            }
            for (TaskId id : state.assignedTasks) {
                if (!state.activeTasks.contains(id))
                    taskIds.add(id);
            }

            final int numConsumers = consumers.size();
            List<TaskId> active = new ArrayList<>();
            Set<TaskId> standby = new HashSet<>();

            int i = 0;
            for (String consumer : consumers) {
                List<TopicPartition> partitions = new ArrayList<>();

                final int numTaskIds = taskIds.size();
                for (int j = i; j < numTaskIds; j += numConsumers) {
                    TaskId taskId = taskIds.get(j);
                    if (j < numActiveTasks) {
                        for (TopicPartition partition : partitionGroups.get(taskId)) {
                            partitions.add(partition);
                            active.add(taskId);
                        }
                    } else {
                        // no partition to a standby task
                        standby.add(taskId);
                    }
                }

                AssignmentInfo data = new AssignmentInfo(active, standby);
                assignment.put(consumer, new Assignment(partitions, data.encode()));
                i++;

                active.clear();
                standby.clear();
            }
        }

        return assignment;
    }

    @Override
    public void onAssignment(Assignment assignment) {
        List<TopicPartition> partitions = assignment.partitions();

        AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
        this.standbyTasks = info.standbyTasks;

        Map<TopicPartition, Set<TaskId>> partitionToTaskIds = new HashMap<>();
        Iterator<TaskId> iter = info.activeTasks.iterator();
        for (TopicPartition partition : partitions) {
            Set<TaskId> taskIds = partitionToTaskIds.get(partition);
            if (taskIds == null) {
                taskIds = new HashSet<>();
                partitionToTaskIds.put(partition, taskIds);
            }

            if (iter.hasNext()) {
                taskIds.add(iter.next());
            } else {
                TaskAssignmentException ex = new TaskAssignmentException(
                        "failed to find a task id for the partition=" + partition.toString() +
                        ", partitions=" + partitions.size() + ", assignmentInfo=" + info.toString()
                );
                log.error(ex.getMessage(), ex);
                throw ex;
            }
        }
        this.partitionToTaskIds = partitionToTaskIds;
    }

    public Set<TaskId> taskIds(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

    public Set<TaskId> standbyTasks() {
        return standbyTasks;
    }
}
