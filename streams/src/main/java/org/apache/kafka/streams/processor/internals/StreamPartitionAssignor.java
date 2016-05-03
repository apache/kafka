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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StreamPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(StreamPartitionAssignor.class);

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        public final TaskId taskId;
        public final TopicPartition partition;

        public AssignedPartition(TaskId taskId, TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }
    }

    private static final Comparator<TopicPartition> PARTITION_COMPARATOR = new Comparator<TopicPartition>() {
        @Override
        public int compare(TopicPartition p1, TopicPartition p2) {
            int result = p1.topic().compareTo(p2.topic());

            if (result != 0) {
                return result;
            } else {
                return p1.partition() < p2.partition() ? -1 : (p1.partition() > p2.partition() ? 1 : 0);
            }
        }
    };

    private StreamThread streamThread;

    private int numStandbyReplicas;
    private Map<Integer, TopologyBuilder.TopicsInfo> topicGroups;
    private Map<TopicPartition, Set<TaskId>> partitionToTaskIds;
    private Map<String, Set<TaskId>> stateChangelogTopicToTaskIds;
    private Map<String, Set<TaskId>> internalSourceTopicToTaskIds;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;

    private InternalTopicManager internalTopicManager;

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(Map<String, ?> configs) {
        numStandbyReplicas = (Integer) configs.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);

        Object o = configs.get(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE);
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
        streamThread.partitionAssignor(this);

        this.topicGroups = streamThread.builder.topicGroups(streamThread.applicationId);

        if (configs.containsKey(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG)) {
            internalTopicManager = new InternalTopicManager(
                    (String) configs.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG),
                    configs.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG) ? (Integer) configs.get(StreamsConfig.REPLICATION_FACTOR_CONFIG) : 1);
        }
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        Set<TaskId> prevTasks = streamThread.prevTasks();
        Set<TaskId> standbyTasks = streamThread.cachedTasks();
        standbyTasks.removeAll(prevTasks);
        SubscriptionInfo data = new SubscriptionInfo(streamThread.processId, prevTasks, standbyTasks);

        return new Subscription(new ArrayList<>(topics), data.encode());
    }

    /**
     * Internal helper function that creates a Kafka topic
     * @param topicToTaskIds Map that contains the topic names to be created
     * @param compactTopic If true, the topic should be a compacted topic. This is used for
     *                     change log topics usually.
     * @param outPartitionInfo If true, compute and return all partitions created
     * @param postPartitionPhase If true, the computation for calculating the number of partitions
     *                           is slightly different. Set to true after the initial topic-to-partition
     *                           assignment.
     * @return
     */
    private Map<TopicPartition, PartitionInfo> prepareTopic(Map<String, Set<TaskId>> topicToTaskIds,
                                                            boolean compactTopic,
                                                            boolean outPartitionInfo,
                                                            boolean postPartitionPhase) {
        Map<TopicPartition, PartitionInfo> partitionInfos = new HashMap<>();
        // if ZK is specified, prepare the internal source topic before calling partition grouper
        if (internalTopicManager != null) {
            log.debug("Starting to validate internal topics in partition assignor.");

            for (Map.Entry<String, Set<TaskId>> entry : topicToTaskIds.entrySet()) {
                String topic = entry.getKey();
                int numPartitions = 0;
                if (postPartitionPhase) {
                    // the expected number of partitions is the max value of TaskId.partition + 1
                    for (TaskId task : entry.getValue()) {
                        if (numPartitions < task.partition + 1)
                            numPartitions = task.partition + 1;
                    }
                } else {
                    // should have size 1 only
                    numPartitions = -1;
                    for (TaskId task : entry.getValue()) {
                        numPartitions = task.partition;
                    }
                }

                internalTopicManager.makeReady(topic, numPartitions, compactTopic);

                // wait until the topic metadata has been propagated to all brokers
                List<PartitionInfo> partitions;
                do {
                    partitions = streamThread.restoreConsumer.partitionsFor(topic);
                } while (partitions == null || partitions.size() != numPartitions);

                if (outPartitionInfo) {
                    for (PartitionInfo partition : partitions)
                        partitionInfos.put(new TopicPartition(partition.topic(), partition.partition()), partition);
                }
            }

            log.info("Completed validating internal topics in partition assignor.");
        }

        return partitionInfos;
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // This assigns tasks to consumer clients in two steps.
        // 1. using TaskAssignor to assign tasks to consumer clients.
        //    - Assign a task to a client which was running it previously.
        //      If there is no such client, assign a task to a client which has its valid local state.
        //    - A client may have more than one stream threads.
        //      The assignor tries to assign tasks to a client proportionally to the number of threads.
        //    - We try not to assign the same set of tasks to two different clients
        //    We do the assignment in one-pass. The result may not satisfy above all.
        // 2. within each client, tasks are assigned to consumer clients in round-robin manner.
        Map<UUID, Set<String>> consumersByClient = new HashMap<>();
        Map<UUID, ClientState<TaskId>> states = new HashMap<>();

        // decode subscription info
        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            Subscription subscription = entry.getValue();

            SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());

            Set<String> consumers = consumersByClient.get(info.processId);
            if (consumers == null) {
                consumers = new HashSet<>();
                consumersByClient.put(info.processId, consumers);
            }
            consumers.add(consumerId);

            ClientState<TaskId> state = states.get(info.processId);
            if (state == null) {
                state = new ClientState<>();
                states.put(info.processId, state);
            }

            state.prevActiveTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.standbyTasks);
            state.capacity = state.capacity + 1d;
        }

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those internal topics.
        internalSourceTopicToTaskIds = new HashMap<>();
        Map<Integer, Set<String>> sourceTopicGroups = new HashMap<>();
        Map<Integer, Set<String>> internalSourceTopicGroups = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            sourceTopicGroups.put(entry.getKey(), entry.getValue().sourceTopics);
            internalSourceTopicGroups.put(entry.getKey(), entry.getValue().interSourceTopics);
        }
        Collection<Set<String>> copartitionTopicGroups = streamThread.builder.copartitionGroups();

        ensureCopartitioning(copartitionTopicGroups, internalSourceTopicGroups, metadata);

        // for those internal source topics that do not have co-partition enforcement,
        // set the number of partitions to the maximum of the depending sub-topologies source topics
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            Set<String> internalTopics = entry.getValue().interSourceTopics;
            for (String internalTopic : internalTopics) {
                Set<TaskId> tasks = internalSourceTopicToTaskIds.get(internalTopic);

                if (tasks == null) {
                    int numPartitions = -1;
                    for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> other : topicGroups.entrySet()) {
                        Set<String> otherSinkTopics = other.getValue().sinkTopics;

                        if (otherSinkTopics.contains(internalTopic)) {
                            for (String topic : other.getValue().sourceTopics) {
                                List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

                                if (infos != null && infos.size() > numPartitions)
                                    numPartitions = infos.size();
                            }
                        }
                    }

                    internalSourceTopicToTaskIds.put(internalTopic, Collections.singleton(new TaskId(entry.getKey(), numPartitions)));
                }
            }
        }

        Map<TopicPartition, PartitionInfo> internalPartitionInfos = prepareTopic(internalSourceTopicToTaskIds, false, true, false);
        internalSourceTopicToTaskIds.clear();

        Cluster metadataWithInternalTopics = metadata;
        if (internalTopicManager != null)
            metadataWithInternalTopics = metadata.withPartitions(internalPartitionInfos);

        // get the tasks as partition groups from the partition grouper
        Map<TaskId, Set<TopicPartition>> partitionsForTask = streamThread.partitionGrouper.partitionGroups(
                sourceTopicGroups, metadataWithInternalTopics);

        // add tasks to state change log topic subscribers
        stateChangelogTopicToTaskIds = new HashMap<>();
        for (TaskId task : partitionsForTask.keySet()) {
            for (String topicName : topicGroups.get(task.topicGroupId).stateChangelogTopics) {
                Set<TaskId> tasks = stateChangelogTopicToTaskIds.get(topicName);
                if (tasks == null) {
                    tasks = new HashSet<>();
                    stateChangelogTopicToTaskIds.put(topicName, tasks);
                }

                tasks.add(task);
            }

            for (String topicName : topicGroups.get(task.topicGroupId).interSourceTopics) {
                Set<TaskId> tasks = internalSourceTopicToTaskIds.get(topicName);
                if (tasks == null) {
                    tasks = new HashSet<>();
                    internalSourceTopicToTaskIds.put(topicName, tasks);
                }

                tasks.add(task);
            }
        }

        // assign tasks to clients
        states = TaskAssignor.assign(states, partitionsForTask.keySet(), numStandbyReplicas);
        Map<String, Assignment> assignment = new HashMap<>();

        for (Map.Entry<UUID, Set<String>> entry : consumersByClient.entrySet()) {
            UUID processId = entry.getKey();
            Set<String> consumers = entry.getValue();
            ClientState<TaskId> state = states.get(processId);

            ArrayList<TaskId> taskIds = new ArrayList<>(state.assignedTasks.size());
            final int numActiveTasks = state.activeTasks.size();
            for (TaskId taskId : state.activeTasks) {
                taskIds.add(taskId);
            }
            for (TaskId id : state.assignedTasks) {
                if (!state.activeTasks.contains(id))
                    taskIds.add(id);
            }

            final int numConsumers = consumers.size();
            Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();

            int i = 0;
            for (String consumer : consumers) {
                ArrayList<AssignedPartition> assignedPartitions = new ArrayList<>();

                final int numTaskIds = taskIds.size();
                for (int j = i; j < numTaskIds; j += numConsumers) {
                    TaskId taskId = taskIds.get(j);
                    if (j < numActiveTasks) {
                        for (TopicPartition partition : partitionsForTask.get(taskId)) {
                            assignedPartitions.add(new AssignedPartition(taskId, partition));
                        }
                    } else {
                        Set<TopicPartition> standbyPartitions = standby.get(taskId);
                        if (standbyPartitions == null) {
                            standbyPartitions = new HashSet<>();
                            standby.put(taskId, standbyPartitions);
                        }
                        standbyPartitions.addAll(partitionsForTask.get(taskId));
                    }
                }

                Collections.sort(assignedPartitions);
                List<TaskId> active = new ArrayList<>();
                List<TopicPartition> activePartitions = new ArrayList<>();
                for (AssignedPartition partition : assignedPartitions) {
                    active.add(partition.taskId);
                    activePartitions.add(partition.partition);
                }

                AssignmentInfo data = new AssignmentInfo(active, standby);
                assignment.put(consumer, new Assignment(activePartitions, data.encode()));
                i++;

                active.clear();
                standby.clear();
            }
        }

        // if ZK is specified, validate the internal topics again
        prepareTopic(internalSourceTopicToTaskIds, false /* compactTopic */, false, true);
        // change log topics should be compacted
        prepareTopic(stateChangelogTopicToTaskIds, true /* compactTopic */, false, true);

        return assignment;
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(Assignment assignment) {
        List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());

        Collections.sort(partitions, PARTITION_COMPARATOR);

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

    private void ensureCopartitioning(Collection<Set<String>> copartitionGroups, Map<Integer, Set<String>> internalTopicGroups, Cluster metadata) {
        Set<String> internalTopics = new HashSet<>();
        for (Set<String> topics : internalTopicGroups.values())
            internalTopics.addAll(topics);

        for (Set<String> copartitionGroup : copartitionGroups) {
            ensureCopartitioning(copartitionGroup, internalTopics, metadata);
        }
    }

    private void ensureCopartitioning(Set<String> copartitionGroup, Set<String> internalTopics, Cluster metadata) {
        int numPartitions = -1;

        for (String topic : copartitionGroup) {
            if (!internalTopics.contains(topic)) {
                List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

                if (infos == null)
                    throw new TopologyBuilderException("External source topic not found: " + topic);

                if (numPartitions == -1) {
                    numPartitions = infos.size();
                } else if (numPartitions != infos.size()) {
                    String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                    Arrays.sort(topics);
                    throw new TopologyBuilderException("Topics not copartitioned: [" + Utils.mkString(Arrays.asList(topics), ",") + "]");
                }
            }
        }

        // enforce co-partitioning restrictions to internal topics reusing internalSourceTopicToTaskIds
        for (String topic : internalTopics) {
            if (copartitionGroup.contains(topic))
                internalSourceTopicToTaskIds.put(topic, Collections.singleton(new TaskId(-1, numPartitions)));
        }
    }

    /* For Test Only */
    public Set<TaskId> tasksForState(String stateName) {
        return stateChangelogTopicToTaskIds.get(ProcessorStateManager.storeChangelogTopic(streamThread.applicationId, stateName));
    }

    public Set<TaskId> tasksForPartition(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }

    public void setInternalTopicManager(InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }
}
