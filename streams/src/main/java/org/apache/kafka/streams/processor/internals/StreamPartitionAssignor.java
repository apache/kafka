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
import org.apache.kafka.streams.StreamConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignmentException;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.ZooDefs;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.I0Itec.zkclient.ZkClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class StreamPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(StreamPartitionAssignor.class);

    private StreamThread streamThread;

    private int numStandbyReplicas;
    private Map<Integer, TopologyBuilder.TopicsInfo> topicGroups;
    private Map<TopicPartition, Set<TaskId>> partitionToTaskIds;
    private Map<String, Set<TaskId>> stateChangelogTopicToTaskIds;
    private Map<String, Set<TaskId>> internalSourceTopicToTaskIds;
    private Map<TaskId, Set<TopicPartition>> standbyTasks;

    // TODO: the following ZK dependency should be removed after KIP-4
    private static final String ZK_TOPIC_PATH = "/brokers/topics";
    private static final String ZK_BROKER_PATH = "/brokers/ids";
    private static final String ZK_DELETE_TOPIC_PATH = "/admin/delete_topics";

    private ZkClient zkClient;

    private class ZKStringSerializer implements ZkSerializer {

        @Override
        public byte[] serialize(Object data) {
            try {
                return ((String) data).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public Object deserialize(byte[] bytes) {
            try {
                if (bytes == null)
                    return null;
                else
                    return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new AssertionError(e);
            }
        }
    }

    private List<Integer> getBrokers() {
        List<Integer> brokers = new ArrayList<>();
        for (String broker: zkClient.getChildren(ZK_BROKER_PATH)) {
            brokers.add(Integer.parseInt(broker));
        }
        Collections.sort(brokers);

        log.debug("Read brokers {} from ZK in partition assignor.", brokers);

        return brokers;
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, List<Integer>> getTopicMetadata(String topic) {
        String data = zkClient.readData(ZK_TOPIC_PATH + "/" + topic, true);

        if (data == null) return null;

        try {
            ObjectMapper mapper = new ObjectMapper();

            Map<String, Object> dataMap = mapper.readValue(data, new TypeReference<Map<String, Object>>() {

            });

            Map<Integer, List<Integer>> partitions = (Map<Integer, List<Integer>>) dataMap.get("partitions");

            log.debug("Read partitions {} for topic {} from ZK in partition assignor.", partitions, topic);

            return partitions;
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private void createTopic(String topic, int numPartitions) throws ZkNodeExistsException {
        log.debug("Creating topic {} with {} partitions from ZK in partition assignor.", topic, numPartitions);

        // we always assign leaders to brokers starting at the first one with replication factor 1
        List<Integer> brokers = getBrokers();

        Map<Integer, List<Integer>> assignment = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            assignment.put(i, Collections.singletonList(brokers.get(i % brokers.size())));
        }

        // try to write to ZK with open ACL
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("version", 1);
            dataMap.put("partitions", assignment);

            ObjectMapper mapper = new ObjectMapper();
            String data = mapper.writeValueAsString(dataMap);

            zkClient.createPersistent(ZK_TOPIC_PATH + "/" + topic, data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (JsonProcessingException e) {
            throw new KafkaException(e);
        }
    }

    private void deleteTopic(String topic) throws ZkNodeExistsException {
        log.debug("Deleting topic {} from ZK in partition assignor.", topic);

        zkClient.createPersistent(ZK_DELETE_TOPIC_PATH + "/" + topic, "", ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    private void addPartitions(String topic, int numPartitions, Map<Integer, List<Integer>> existingAssignment) {
        log.debug("Adding {} partitions topic {} from ZK with existing partitions assigned as {} in partition assignor.", topic, numPartitions, existingAssignment);

        // we always assign new leaders to brokers starting at the last broker of the existing assignment with replication factor 1
        List<Integer> brokers = getBrokers();

        int startIndex = existingAssignment.size();

        Map<Integer, List<Integer>> newAssignment = new HashMap<>(existingAssignment);

        for (int i = 0; i < numPartitions; i++) {
            newAssignment.put(i + startIndex, Collections.singletonList(brokers.get(i + startIndex) % brokers.size()));
        }

        // try to write to ZK with open ACL
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("version", 1);
            dataMap.put("partitions", newAssignment);

            ObjectMapper mapper = new ObjectMapper();
            String data = mapper.writeValueAsString(dataMap);

            zkClient.writeData(ZK_TOPIC_PATH + "/" + topic, data);
        } catch (JsonProcessingException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        numStandbyReplicas = (Integer) configs.get(StreamConfig.NUM_STANDBY_REPLICAS_CONFIG);

        Object o = configs.get(StreamConfig.InternalConfig.STREAM_THREAD_INSTANCE);
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

        this.topicGroups = streamThread.builder.topicGroups();

        if (configs.containsKey(StreamConfig.ZOOKEEPER_CONNECT_CONFIG))
            zkClient = new ZkClient((String) configs.get(StreamConfig.ZOOKEEPER_CONNECT_CONFIG), 30 * 1000, 30 * 1000, new ZKStringSerializer());
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of Streams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        Set<TaskId> prevTasks = streamThread.prevTasks();
        Set<TaskId> standbyTasks = streamThread.cachedTasks();
        standbyTasks.removeAll(prevTasks);
        SubscriptionInfo data = new SubscriptionInfo(streamThread.processId, prevTasks, standbyTasks);

        return new Subscription(new ArrayList<>(topics), data.encode());
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

        // Decode subscription info
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

        // get the tasks as partition groups from the partition grouper
        Map<Integer, Set<String>> sourceTopicGroups = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            sourceTopicGroups.put(entry.getKey(), entry.getValue().sourceTopics);
        }
        Map<TaskId, Set<TopicPartition>> partitionsForTask = streamThread.partitionGrouper.partitionGroups(sourceTopicGroups, metadata);

        // add tasks to state topic subscribers
        stateChangelogTopicToTaskIds = new HashMap<>();
        internalSourceTopicToTaskIds = new HashMap<>();
        for (TaskId task : partitionsForTask.keySet()) {
            for (String stateName : topicGroups.get(task.topicGroupId).stateChangelogTopics) {
                Set<TaskId> tasks = stateChangelogTopicToTaskIds.get(stateName);
                if (tasks == null) {
                    tasks = new HashSet<>();
                    stateChangelogTopicToTaskIds.put(stateName, tasks);
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
            List<TaskId> active = new ArrayList<>();
            Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();

            int i = 0;
            for (String consumer : consumers) {
                List<TopicPartition> activePartitions = new ArrayList<>();

                final int numTaskIds = taskIds.size();
                for (int j = i; j < numTaskIds; j += numConsumers) {
                    TaskId taskId = taskIds.get(j);
                    if (j < numActiveTasks) {
                        for (TopicPartition partition : partitionsForTask.get(taskId)) {
                            activePartitions.add(partition);
                            active.add(taskId);
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

                AssignmentInfo data = new AssignmentInfo(active, standby);
                assignment.put(consumer, new Assignment(activePartitions, data.encode()));
                i++;

                active.clear();
                standby.clear();
            }
        }

        // if ZK is specified, get the tasks / internal topics for each state topic and validate the topic partitions
        if (zkClient != null) {
            log.debug("Starting to validate changelog topics in partition assignor.");

            Map<String, Set<TaskId>> topicToTaskIds = new HashMap<>();
            topicToTaskIds.putAll(stateChangelogTopicToTaskIds);
            topicToTaskIds.putAll(internalSourceTopicToTaskIds);

            for (Map.Entry<String, Set<TaskId>> entry : topicToTaskIds.entrySet()) {
                String topic = streamThread.jobId + "-" + entry.getKey();

                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = 0;
                for (TaskId task : entry.getValue()) {
                    if (numPartitions < task.partition + 1)
                        numPartitions = task.partition + 1;
                }

                boolean topicNotReady = true;

                while (topicNotReady) {
                    Map<Integer, List<Integer>> topicMetadata = getTopicMetadata(topic);

                    // if topic does not exist, create it
                    if (topicMetadata == null) {
                        try {
                            createTopic(topic, numPartitions);
                        } catch (ZkNodeExistsException e) {
                            // ignore and continue
                        }
                    } else {
                        if (topicMetadata.size() > numPartitions) {
                            // else if topic exists with more #.partitions than needed, delete in order to re-create it
                            try {
                                deleteTopic(topic);
                            } catch (ZkNodeExistsException e) {
                                // ignore and continue
                            }
                        } else if (topicMetadata.size() < numPartitions) {
                            // else if topic exists with less #.partitions than needed, add partitions
                            try {
                                addPartitions(topic, numPartitions - topicMetadata.size(), topicMetadata);
                            } catch (ZkNoNodeException e) {
                                // ignore and continue
                            }
                        }

                        topicNotReady = false;
                    }
                }

                // wait until the topic metadata has been propagated to all brokers
                List<PartitionInfo> partitions;
                do {
                    partitions = streamThread.restoreConsumer.partitionsFor(topic);
                } while (partitions == null || partitions.size() != numPartitions);
            }

            log.info("Completed validating changelog topics in partition assignor.");
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

    /* For Test Only */
    public Set<TaskId> tasksForState(String stateName) {
        return stateChangelogTopicToTaskIds.get(stateName + ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX);
    }

    public Set<TaskId> tasksForPartition(TopicPartition partition) {
        return partitionToTaskIds.get(partition);
    }

    public Map<TaskId, Set<TopicPartition>> standbyTasks() {
        return standbyTasks;
    }
}
