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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.streams.state.HostInfo;
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

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.InternalTopicManager.WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;

public class StreamPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(StreamPartitionAssignor.class);

    public final static int UNKNOWN = -1;
    public final static int NOT_AVAILABLE = -2;

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        public final TaskId taskId;
        public final TopicPartition partition;

        AssignedPartition(final TaskId taskId, final TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(final AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }
    }

    static class ClientMetadata {
        final HostInfo hostInfo;
        final Map<String, ClientState<TaskId>> consumers = new HashMap<>();
        final ClientState<TaskId> processState = new ClientState<>();

        ClientMetadata(final String endPoint) {

            // get the host info if possible
            if (endPoint != null) {
                final String host = getHost(endPoint);
                final Integer port = getPort(endPoint);

                if (host == null || port == null)
                    throw new ConfigException(String.format("Error parsing host address %s. Expected format host:port.", endPoint));

                hostInfo = new HostInfo(host, port);
            } else {
                hostInfo = null;
            }

        }

        void addConsumer(final String consumerMemberId, final SubscriptionInfo info) {
            final ClientState<TaskId> consumerState = new ClientState<>();
            consumerState.prevActiveTasks.addAll(info.prevTasks);
            consumerState.prevAssignedTasks.addAll(info.prevTasks);
            consumerState.prevAssignedTasks.addAll(info.standbyTasks);
            consumers.put(consumerMemberId, consumerState);

            processState.prevActiveTasks.addAll(info.prevTasks);
            processState.prevAssignedTasks.addAll(info.prevTasks);
            processState.prevAssignedTasks.addAll(info.standbyTasks);
            processState.capacity = processState.capacity + 1d;
        }

        @Override
        public String toString() {
            return "ClientMetadata{" +
                    "hostInfo=" + hostInfo +
                    ", consumers=" + consumers +
                    ", state=" + processState +
                    '}';
        }
    }

    private static class InternalTopicMetadata {
        public final InternalTopicConfig config;

        public int numPartitions;

        InternalTopicMetadata(final InternalTopicConfig config) {
            this.config = config;
            this.numPartitions = UNKNOWN;
        }
    }

    private static final Comparator<TopicPartition> PARTITION_COMPARATOR = new Comparator<TopicPartition>() {
        @Override
        public int compare(TopicPartition p1, TopicPartition p2) {
            int result = p1.topic().compareTo(p2.topic());

            if (result != 0) {
                return result;
            } else {
                return p1.partition() < p2.partition() ? UNKNOWN : (p1.partition() > p2.partition() ? 1 : 0);
            }
        }
    };

    private StreamThread streamThread;

    private String userEndPoint;
    private int numStandbyReplicas;

    private Cluster metadataWithInternalTopics;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHostState;

    private Map<TaskId, Set<TopicPartition>> standbyTasks;
    private Map<TaskId, Set<TopicPartition>> activeTasks;

    InternalTopicManager internalTopicManager;

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
            KafkaException ex = new KafkaException(String.format("%s is not an instance of %s", o.getClass().getName(), StreamThread.class.getName()));
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        streamThread = (StreamThread) o;
        streamThread.partitionAssignor(this);

        String userEndPoint = (String) configs.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
        if (userEndPoint != null && !userEndPoint.isEmpty()) {
            try {
                String host = getHost(userEndPoint);
                Integer port = getPort(userEndPoint);

                if (host == null || port == null)
                    throw new ConfigException(String.format("stream-thread [%s] Config %s isn't in the correct format. Expected a host:port pair" +
                                    " but received %s",
                            streamThread.getName(), StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
            } catch (NumberFormatException nfe) {
                throw new ConfigException(String.format("stream-thread [%s] Invalid port supplied in %s for config %s",
                        streamThread.getName(), userEndPoint, StreamsConfig.APPLICATION_SERVER_CONFIG));
            }

            this.userEndPoint = userEndPoint;
        }

        internalTopicManager = new InternalTopicManager(
                new StreamsKafkaClient(this.streamThread.config),
                configs.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG) ? (Integer) configs.get(StreamsConfig.REPLICATION_FACTOR_CONFIG) : 1,
                configs.containsKey(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG) ?
                        (Long) configs.get(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG)
                        : WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT);
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
        SubscriptionInfo data = new SubscriptionInfo(streamThread.processId, prevTasks, standbyTasks, this.userEndPoint);

        if (streamThread.builder.sourceTopicPattern() != null) {
            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            log.debug("stream-thread [{}] found {} topics possibly matching regex", streamThread.getName(), topics);
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.updateTopics(topics);
            streamThread.builder.updateSubscriptions(subscriptionUpdates, streamThread.getName());
        }

        return new Subscription(new ArrayList<>(topics), data.encode());
    }

    /*
     * This assigns tasks to consumer clients in the following steps.
     *
     * 0. check all repartition source topics and use internal topic manager to make sure
     *    they have been created with the right number of partitions.
     *
     * 1. using user customized partition grouper to generate tasks along with their
     *    assigned partitions; also make sure that the task's corresponding changelog topics
     *    have been created with the right number of partitions.
     *
     * 2. using TaskAssignor to assign tasks to consumer clients.
     *    - Assign a task to a client which was running it previously.
     *      If there is no such client, assign a task to a client which has its valid local state.
     *    - A client may have more than one stream threads.
     *      The assignor tries to assign tasks to a client proportionally to the number of threads.
     *    - We try not to assign the same set of tasks to two different clients
     *    We do the assignment in one-pass. The result may not satisfy above all.
     *
     * 3. within each client, tasks are assigned to consumer clients in round-robin manner.
     */
    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {

        // construct the client metadata from the decoded subscription info
        final Map<UUID, ClientMetadata> clientsMetadata = new HashMap<>();

        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            Subscription subscription = entry.getValue();

            SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());

            // create the new client metadata if necessary
            ClientMetadata clientMetadata = clientsMetadata.get(info.processId);

            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint);
                clientsMetadata.put(info.processId, clientMetadata);
            }

            // add the consumer to the client
            clientMetadata.addConsumer(consumerId, info);
        }

        log.info("stream-thread [{}] Constructed client metadata {} from the member subscriptions.", streamThread.getName(), clientsMetadata);

        // ---------------- Step Zero ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        Map<Integer, TopologyBuilder.TopicsInfo> topicGroups = streamThread.builder.topicGroups();

        Map<String, InternalTopicMetadata> repartitionTopicMetadata = new HashMap<>();
        for (TopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
            for (InternalTopicConfig topic: topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), new InternalTopicMetadata(topic));
            }
        }

        boolean numPartitionsNeeded;
        do {
            numPartitionsNeeded = false;

            for (TopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                for (String topicName : topicsInfo.repartitionSourceTopics.keySet()) {
                    int numPartitions = repartitionTopicMetadata.get(topicName).numPartitions;

                    // try set the number of partitions for this repartition topic if it is not set yet
                    if (numPartitions == UNKNOWN) {
                        for (TopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    Integer numPartitionsCandidate;
                                    // It is possible the sourceTopic is another internal topic, i.e,
                                    // map().join().join(map())
                                    if (repartitionTopicMetadata.containsKey(sourceTopicName)) {
                                        numPartitionsCandidate = repartitionTopicMetadata.get(sourceTopicName).numPartitions;
                                    } else {
                                        numPartitionsCandidate = metadata.partitionCountForTopic(sourceTopicName);
                                        if (numPartitionsCandidate == null) {
                                            repartitionTopicMetadata.get(topicName).numPartitions = NOT_AVAILABLE;
                                        }
                                    }

                                    if (numPartitionsCandidate != null && numPartitionsCandidate > numPartitions) {
                                        numPartitions = numPartitionsCandidate;
                                    }
                                }
                            }
                        }
                        // if we still have not find the right number of partitions,
                        // another iteration is needed
                        if (numPartitions == UNKNOWN)
                            numPartitionsNeeded = true;
                        else
                            repartitionTopicMetadata.get(topicName).numPartitions = numPartitions;
                    }
                }
            }
        } while (numPartitionsNeeded);

        // augment the metadata with the newly computed number of partitions for all the
        // repartition source topics
        Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new HashMap<>();
        for (Map.Entry<String, InternalTopicMetadata> entry : repartitionTopicMetadata.entrySet()) {
            String topic = entry.getKey();
            Integer numPartitions = entry.getValue().numPartitions;

            for (int partition = 0; partition < numPartitions; partition++) {
                allRepartitionTopicPartitions.put(new TopicPartition(topic, partition),
                        new PartitionInfo(topic, partition, null, new Node[0], new Node[0]));
            }
        }

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(streamThread.builder.copartitionGroups(), repartitionTopicMetadata, metadata);

        // make sure the repartition source topics exist with the right number of partitions,
        // create these topics if necessary
        prepareTopic(repartitionTopicMetadata);

        metadataWithInternalTopics = metadata;
        if (internalTopicManager != null)
            metadataWithInternalTopics = metadata.withPartitions(allRepartitionTopicPartitions);

        log.debug("stream-thread [{}] Created repartition topics {} from the parsed topology.", streamThread.getName(), allRepartitionTopicPartitions.values());

        // ---------------- Step One ---------------- //

        // get the tasks as partition groups from the partition grouper
        Set<String> allSourceTopics = new HashSet<>();
        Map<Integer, Set<String>> sourceTopicsByGroup = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            allSourceTopics.addAll(entry.getValue().sourceTopics);
            sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
        }

        final Map<TaskId, Set<TopicPartition>> partitionsForTask = streamThread.partitionGrouper.partitionGroups(
                sourceTopicsByGroup, metadataWithInternalTopics);

        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        Map<Integer, Set<TaskId>> tasksByTopicGroup = new HashMap<>();
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            Set<TopicPartition> partitions = entry.getValue();
            for (TopicPartition partition : partitions) {
                if (allAssignedPartitions.contains(partition)) {
                    log.warn("stream-thread [{}] Partition {} is assigned to more than one tasks: {}", streamThread.getName(), partition, partitionsForTask);
                }
            }
            allAssignedPartitions.addAll(partitions);

            TaskId id = entry.getKey();
            Set<TaskId> ids = tasksByTopicGroup.get(id.topicGroupId);
            if (ids == null) {
                ids = new HashSet<>();
                tasksByTopicGroup.put(id.topicGroupId, ids);
            }
            ids.add(id);
        }
        for (String topic : allSourceTopics) {
            List<PartitionInfo> partitionInfoList = metadataWithInternalTopics.partitionsForTopic(topic);
            if (partitionInfoList != null) {
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    if (!allAssignedPartitions.contains(partition)) {
                        log.warn("stream-thread [{}] Partition {} is not assigned to any tasks: {}", streamThread.getName(), partition, partitionsForTask);
                    }
                }
            } else {
                log.warn("stream-thread [{}] No partitions found for topic {}", streamThread.getName(), topic);
            }
        }

        // add tasks to state change log topic subscribers
        Map<String, InternalTopicMetadata> changelogTopicMetadata = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final Map<String, InternalTopicConfig> stateChangelogTopics = entry.getValue().stateChangelogTopics;

            for (InternalTopicConfig topicConfig : stateChangelogTopics.values()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                if (tasksByTopicGroup.get(topicGroupId) != null) {
                    for (TaskId task : tasksByTopicGroup.get(topicGroupId)) {
                        if (numPartitions < task.partition + 1)
                            numPartitions = task.partition + 1;
                    }
                    InternalTopicMetadata topicMetadata = new InternalTopicMetadata(topicConfig);
                    topicMetadata.numPartitions = numPartitions;

                    changelogTopicMetadata.put(topicConfig.name(), topicMetadata);
                } else {
                    log.debug("stream-thread [{}] No tasks found for topic group {}", streamThread.getName(), topicGroupId);
                }
            }
        }

        prepareTopic(changelogTopicMetadata);

        log.debug("stream-thread [{}] Created state changelog topics {} from the parsed topology.", streamThread.getName(), changelogTopicMetadata);

        // ---------------- Step Two ---------------- //

        // assign tasks to clients
        final Map<UUID, ClientState<TaskId>> states = new HashMap<>();
        for (Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            states.put(entry.getKey(), entry.getValue().processState);
        }

        log.debug("stream-thread [{}] Assigning tasks {} to clients {} with number of replicas {}",
                streamThread.getName(), partitionsForTask.keySet(), states, numStandbyReplicas);

        TaskAssignor.assign(states, partitionsForTask.keySet(), numStandbyReplicas);

        log.info("stream-thread [{}] Assigned tasks to clients as {}.", streamThread.getName(), states);

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map
        partitionsByHostState = new HashMap<>();
        for (Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            HostInfo hostInfo = entry.getValue().hostInfo;

            if (hostInfo != null) {
                final Set<TopicPartition> topicPartitions = new HashSet<>();
                final ClientState<TaskId> state = entry.getValue().processState;

                for (TaskId id : state.activeTasks) {
                    topicPartitions.addAll(partitionsForTask.get(id));
                }

                partitionsByHostState.put(hostInfo, topicPartitions);
            }
        }

        return new ProcessTaskAssignor(clientsMetadata, partitionsForTask, partitionsByHostState).assign();
    }

    // Assigns the tasks to the consumers that belong to a single process.
    // Trying to keep the assignment sticky to avoid unnecessary closing, and creating of tasks.
    static class ProcessTaskAssignor {
        private final Map<UUID, ClientMetadata> clientsMetadata;
        private final Map<TaskId, Set<TopicPartition>> partitionsForTask;
        private final Map<HostInfo, Set<TopicPartition>> partitionsByHostState;

        ProcessTaskAssignor(final Map<UUID, ClientMetadata> clientsMetadata,
                            final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                            final Map<HostInfo, Set<TopicPartition>> partitionsByHostState) {
            this.clientsMetadata = clientsMetadata;
            this.partitionsForTask = partitionsForTask;
            this.partitionsByHostState = partitionsByHostState;
        }

        public Map<String, Assignment> assign() {
            // within the client, distribute tasks to its owned consumers
            final Map<String, Assignment> assignment = new HashMap<>();
            for (final Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
                final Map<String, ClientState<TaskId>> consumers = entry.getValue().consumers;
                final ClientState<TaskId> processState = entry.getValue().processState;

                final Map<String, List<AssignedPartition>> activeAssignment = new HashMap<>();
                final Map<String, Map<TaskId, Set<TopicPartition>>> standbyAssignment = new HashMap<>();
                // initialize active assignment for all consumers to avoid having to do
                // null checks etc later
                for (final String consumer : consumers.keySet()) {
                    activeAssignment.put(consumer, new ArrayList<AssignedPartition>());
                    standbyAssignment.put(consumer, new HashMap<TaskId, Set<TopicPartition>>());
                }

                final Map<TaskId, String> previousActiveTaskAssignment = buildPreviousActiveTaskMap(consumers);
                final Map<TaskId, String> previousStandbyTaskAssignment = buildPreviousStandbyTaskMap(consumers);
                assignActiveTasks(consumers, processState, activeAssignment, previousActiveTaskAssignment, previousStandbyTaskAssignment);
                assignStandbyTasks(consumers, processState, standbyAssignment, previousStandbyTaskAssignment);

                for (final String consumer : consumers.keySet()) {
                    final List<AssignedPartition> consumerAssignment = activeAssignment.get(consumer);
                    Collections.sort(consumerAssignment);
                    final List<TaskId> active = new ArrayList<>();
                    final List<TopicPartition> activePartitions = new ArrayList<>();
                    for (AssignedPartition partition : consumerAssignment) {
                        active.add(partition.taskId);
                        activePartitions.add(partition.partition);
                    }
                    // finally, encode the assignment before sending back to coordinator
                    assignment.put(consumer, new Assignment(activePartitions, new AssignmentInfo(active, standbyAssignment.get(consumer), partitionsByHostState).encode()));
                }

            }
            return assignment;
        }

        private void assignStandbyTasks(final Map<String, ClientState<TaskId>> consumers,
                                        final ClientState<TaskId> processState,
                                        final Map<String, Map<TaskId, Set<TopicPartition>>> standbyAssignment,
                                        final Map<TaskId, String> previousStandbyTaskAssignment) {
            final int standbyTasksPerConsumer = processState.standbyTasks.size() / consumers.size();
            int standbyTaskRemainder = processState.standbyTasks.size() - standbyTasksPerConsumer * consumers.size();

            for (int i = 0; i < standbyTasksPerConsumer; i++) {
                for (final Iterator<TaskId> iterator = processState.standbyTasks.iterator(); iterator.hasNext(); ) {
                    final TaskId standbyTask = iterator.next();
                    String consumer = previousStandbyTaskAssignment.get(standbyTask);
                    if (consumer == null) {
                        consumer = leastLoadedStandby(standbyAssignment);
                    } else if (standbyAssignment.get(consumer).size() >= standbyTasksPerConsumer && standbyTaskRemainder == 0) {
                        consumer = leastLoadedStandby(standbyAssignment);
                    } else if (standbyAssignment.get(consumer).size() == standbyTasksPerConsumer && standbyTaskRemainder > 0) {
                        standbyTaskRemainder--;
                    }
                    standbyAssignment.get(consumer).put(standbyTask, partitionsForTask.get(standbyTask));
                    iterator.remove();
                }
            }

            for (final TaskId next : processState.standbyTasks) {
                final String consumer = leastLoadedStandby(standbyAssignment);
                standbyAssignment.get(consumer).put(next, partitionsForTask.get(next));
            }
        }

        private void assignActiveTasks(final Map<String, ClientState<TaskId>> consumers,
                                       final ClientState<TaskId> processState,
                                       final Map<String, List<AssignedPartition>> activeAssignment,
                                       final Map<TaskId, String> previousActiveTaskAssignment,
                                       final Map<TaskId, String> previousStandbyTaskAssignment) {
            final int taskPerConsumer = processState.activeTasks.size() / consumers.size();
            int remainder = processState.activeTasks.size() - taskPerConsumer * consumers.size();
            for (int i = 0; i < taskPerConsumer; i++) {
                for (final Iterator<TaskId> iterator = processState.activeTasks.iterator(); iterator.hasNext(); ) {
                    final TaskId next = iterator.next();
                    String consumer = previousActiveTaskAssignment.get(next);
                    if (consumer == null) {
                        consumer = previousStandbyTaskAssignment.get(next);
                    }
                    // get the least loaded consumer if we haven't yet found one.
                    if (consumer == null) {
                        consumer = leastLoadedActive(activeAssignment);
                        // get the least loaded consumer if the current consumer has already exceeded its quota
                    } else if (activeAssignment.get(consumer).size() >= taskPerConsumer && remainder == 0) {
                        consumer = leastLoadedActive(activeAssignment);
                    } else if (activeAssignment.get(consumer).size() == taskPerConsumer && remainder > 0) {
                        remainder--;
                    }

                    final List<AssignedPartition> assignedPartitions = activeAssignment.get(consumer);
                    for (final TopicPartition partition : partitionsForTask.get(next)) {
                        assignedPartitions.add(new AssignedPartition(next, partition));
                    }
                    iterator.remove();
                }
            }

            for (final TaskId next : processState.activeTasks) {
                final List<AssignedPartition> assignedPartitions = activeAssignment.get(leastLoadedActive(activeAssignment));
                for (final TopicPartition partition : partitionsForTask.get(next)) {
                    assignedPartitions.add(new AssignedPartition(next, partition));
                }
            }
        }

        private Map<TaskId, String> buildPreviousStandbyTaskMap(final Map<String, ClientState<TaskId>> consumers) {
            final Map<TaskId, String> previousStandbyTaskAssignment = new HashMap<>();
            for (Map.Entry<String, ClientState<TaskId>> clientState : consumers.entrySet()) {
                final Set<TaskId> assignedTasks = new HashSet<>(clientState.getValue().prevAssignedTasks);
                assignedTasks.removeAll(clientState.getValue().prevActiveTasks);
                for (final TaskId activeTask : assignedTasks) {
                    previousStandbyTaskAssignment.put(activeTask, clientState.getKey());
                }
            }
            return previousStandbyTaskAssignment;
        }

        private Map<TaskId, String> buildPreviousActiveTaskMap(final Map<String, ClientState<TaskId>> consumers) {
            final Map<TaskId, String> previousActiveTaskAssignment = new HashMap<>();
            for (Map.Entry<String, ClientState<TaskId>> clientState : consumers.entrySet()) {
                for (final TaskId activeTask : clientState.getValue().prevActiveTasks) {
                    previousActiveTaskAssignment.put(activeTask, clientState.getKey());
                }
            }
            return previousActiveTaskAssignment;
        }

        private String leastLoadedStandby(final Map<String, Map<TaskId, Set<TopicPartition>>> standbyAssignment) {
            int lowestAssigned = Integer.MAX_VALUE;
            String leastLoaded = null;
            for (final Map.Entry<String, Map<TaskId, Set<TopicPartition>>> assignment : standbyAssignment.entrySet()) {
                if (assignment.getValue().isEmpty()) {
                    return assignment.getKey();
                }
                if (assignment.getValue().size() < lowestAssigned) {
                    lowestAssigned = assignment.getValue().size();
                    leastLoaded = assignment.getKey();
                }
            }
            return leastLoaded;
        }

        private <T> String leastLoadedActive(final Map<String, List<T>> allAssignments) {
            int lowestAssigned = Integer.MAX_VALUE;
            String leastLoaded = null;
            for (final Map.Entry<String, List<T>> currentAssignment : allAssignments.entrySet()) {
                if (currentAssignment.getValue().isEmpty()) {
                    return currentAssignment.getKey();
                }
                if (currentAssignment.getValue().size() < lowestAssigned) {
                    lowestAssigned = currentAssignment.getValue().size();
                    leastLoaded = currentAssignment.getKey();
                }
            }
            return leastLoaded;
        }
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
        this.activeTasks = new HashMap<>();

        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks.size()) {
            throw new TaskAssignmentException(
                    String.format("stream-thread [%s] Number of assigned partitions %d is not equal to the number of active taskIds %d" +
                            ", assignmentInfo=%s", streamThread.getName(), partitions.size(), info.activeTasks.size(), info.toString())
            );
        }

        for (int i = 0; i < partitions.size(); i++) {
            TopicPartition partition = partitions.get(i);
            TaskId id = info.activeTasks.get(i);

            Set<TopicPartition> assignedPartitions = activeTasks.get(id);
            if (assignedPartitions == null) {
                assignedPartitions = new HashSet<>();
                activeTasks.put(id, assignedPartitions);
            }
            assignedPartitions.add(partition);
        }

        this.partitionsByHostState = info.partitionsByHost;

        final Collection<Set<TopicPartition>> values = partitionsByHostState.values();
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        for (Set<TopicPartition> value : values) {
            for (TopicPartition topicPartition : value) {
                topicToPartitionInfo.put(topicPartition, new PartitionInfo(topicPartition.topic(),
                                                                           topicPartition.partition(),
                                                                           null,
                                                                           new Node[0],
                                                                           new Node[0]));
            }
        }
        metadataWithInternalTopics = Cluster.empty().withPartitions(topicToPartitionInfo);
    }

    /**
     * Internal helper function that creates a Kafka topic
     *
     * @param topicPartitions Map that contains the topic names to be created with the number of partitions
     */
    private void prepareTopic(Map<String, InternalTopicMetadata> topicPartitions) {
        log.debug("stream-thread [{}] Starting to validate internal topics in partition assignor.", streamThread.getName());

        // if ZK is specified, prepare the internal source topic before calling partition grouper
        if (internalTopicManager != null) {
            for (Map.Entry<String, InternalTopicMetadata> entry : topicPartitions.entrySet()) {
                InternalTopicConfig topic = entry.getValue().config;
                Integer numPartitions = entry.getValue().numPartitions;

                if (numPartitions == NOT_AVAILABLE) {
                    continue;
                }
                if (numPartitions < 0) {
                    throw new TopologyBuilderException(String.format("stream-thread [%s] Topic [%s] number of partitions not defined", streamThread.getName(), topic.name()));
                }

                internalTopicManager.makeReady(topic, numPartitions);

                // wait until the topic metadata has been propagated to all brokers
                List<PartitionInfo> partitions;
                do {
                    partitions = streamThread.restoreConsumer.partitionsFor(topic.name());
                } while (partitions == null || partitions.size() != numPartitions);
            }
        } else {
            List<String> missingTopics = new ArrayList<>();
            for (String topic : topicPartitions.keySet()) {
                List<PartitionInfo> partitions = streamThread.restoreConsumer.partitionsFor(topic);
                if (partitions == null) {
                    missingTopics.add(topic);
                }
            }

            if (!missingTopics.isEmpty()) {
                log.warn("stream-thread [{}] Topic {} do not exists but couldn't created as the config '{}' isn't supplied",
                        streamThread.getName(), missingTopics, StreamsConfig.ZOOKEEPER_CONNECT_CONFIG);
            }
        }

        log.info("stream-thread [{}] Completed validating internal topics in partition assignor", streamThread.getName());
    }

    private void ensureCopartitioning(Collection<Set<String>> copartitionGroups,
                                      Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                                      Cluster metadata) {
        for (Set<String> copartitionGroup : copartitionGroups) {
            ensureCopartitioning(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
        }
    }

    private void ensureCopartitioning(Set<String> copartitionGroup,
                                      Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                                      Cluster metadata) {
        int numPartitions = UNKNOWN;

        for (String topic : copartitionGroup) {
            if (!allRepartitionTopicsNumPartitions.containsKey(topic)) {
                Integer partitions = metadata.partitionCountForTopic(topic);

                if (partitions == null)
                    throw new TopologyBuilderException(String.format("stream-thread [%s] Topic not found: %s", streamThread.getName(), topic));

                if (numPartitions == UNKNOWN) {
                    numPartitions = partitions;
                } else if (numPartitions != partitions) {
                    String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                    Arrays.sort(topics);
                    throw new TopologyBuilderException(String.format("stream-thread [%s] Topics not co-partitioned: [%s]", streamThread.getName(), Utils.mkString(Arrays.asList(topics), ",")));
                }
            }
        }

        // if all topics for this co-partition group is repartition topics,
        // then set the number of partitions to be the maximum of the number of partitions.
        if (numPartitions == UNKNOWN) {
            for (Map.Entry<String, InternalTopicMetadata> entry: allRepartitionTopicsNumPartitions.entrySet()) {
                if (copartitionGroup.contains(entry.getKey())) {
                    int partitions = entry.getValue().numPartitions;
                    if (partitions > numPartitions) {
                        numPartitions = partitions;
                    }
                }
            }
        }
        // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
        for (Map.Entry<String, InternalTopicMetadata> entry : allRepartitionTopicsNumPartitions.entrySet()) {
            if (copartitionGroup.contains(entry.getKey())) {
                entry.getValue().numPartitions = numPartitions;
            }
        }
    }

    Map<HostInfo, Set<TopicPartition>> getPartitionsByHostState() {
        if (partitionsByHostState == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(partitionsByHostState);
    }

    Cluster clusterMetadata() {
        if (metadataWithInternalTopics == null) {
            return Cluster.empty();
        }
        return metadataWithInternalTopics;
    }

    Map<TaskId, Set<TopicPartition>> activeTasks() {
        if (activeTasks == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(activeTasks);
    }

    Map<TaskId, Set<TopicPartition>> standbyTasks() {
        if (standbyTasks == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(standbyTasks);
    }

    void setInternalTopicManager(InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     */
    public static class SubscriptionUpdates {

        private final Set<String> updatedTopicSubscriptions = new HashSet<>();

        private  void updateTopics(Collection<String> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<String> getUpdates() {
            return Collections.unmodifiableSet(new HashSet<>(updatedTopicSubscriptions));
        }

        public boolean hasUpdates() {
            return !updatedTopicSubscriptions.isEmpty();
        }

        @Override
        public String toString() {
            return "SubscriptionUpdates{" +
                    "updatedTopicSubscriptions=" + updatedTopicSubscriptions +
                    '}';
        }
    }

    public void close() {
        internalTopicManager.close();
    }
}
