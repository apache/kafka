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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public class StreamsPartitionAssignor implements PartitionAssignor, Configurable {

    private final static int UNKNOWN = -1;
    public final static int NOT_AVAILABLE = -2;

    private Logger log;
    private String logPrefix;

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        public final TaskId taskId;
        public final TopicPartition partition;

        AssignedPartition(final TaskId taskId,
                          final TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(final AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof AssignedPartition)) {
                return false;
            }
            final AssignedPartition other = (AssignedPartition) o;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            // Only partition is important for compareTo, equals and hashCode.
            return partition.hashCode();
        }
    }

    private static class ClientMetadata {
        final HostInfo hostInfo;
        final Set<String> consumers;
        final ClientState state;

        ClientMetadata(final String endPoint) {

            // get the host info if possible
            if (endPoint != null) {
                final String host = getHost(endPoint);
                final Integer port = getPort(endPoint);

                if (host == null || port == null) {
                    throw new ConfigException(String.format("Error parsing host address %s. Expected format host:port.", endPoint));
                }

                hostInfo = new HostInfo(host, port);
            } else {
                hostInfo = null;
            }

            // initialize the consumer memberIds
            consumers = new HashSet<>();

            // initialize the client state
            state = new ClientState();
        }

        void addConsumer(final String consumerMemberId,
                         final SubscriptionInfo info) {
            consumers.add(consumerMemberId);
            state.addPreviousActiveTasks(info.prevTasks());
            state.addPreviousStandbyTasks(info.standbyTasks());
            state.incrementCapacity();
        }

        @Override
        public String toString() {
            return "ClientMetadata{" +
                    "hostInfo=" + hostInfo +
                    ", consumers=" + consumers +
                    ", state=" + state +
                    '}';
        }
    }

    static class InternalTopicMetadata {
        public final InternalTopicConfig config;

        public int numPartitions;

        InternalTopicMetadata(final InternalTopicConfig config) {
            this.config = config;
            this.numPartitions = UNKNOWN;
        }

        @Override
        public String toString() {
            return "InternalTopicMetadata(" +
                    "config=" + config +
                    ", numPartitions=" + numPartitions +
                    ")";
        }
    }

    private static final Comparator<TopicPartition> PARTITION_COMPARATOR = new Comparator<TopicPartition>() {
        @Override
        public int compare(final TopicPartition p1,
                           final TopicPartition p2) {
            final int result = p1.topic().compareTo(p2.topic());

            if (result != 0) {
                return result;
            } else {
                return Integer.compare(p1.partition(), p2.partition());
            }
        }
    };

    private String userEndPoint;
    private int numStandbyReplicas;

    private TaskManager taskManager;
    private PartitionGrouper partitionGrouper;

    private int userMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsValidator copartitionedTopicsValidator;

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final StreamsConfig streamsConfig = new StreamsConfig(configs);

        // Setting the logger with the passed in client thread name
        logPrefix = String.format("stream-thread [%s] ", streamsConfig.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        final String upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
        if (upgradeFrom != null) {
            switch (upgradeFrom) {
                case StreamsConfig.UPGRADE_FROM_0100:
                    log.info("Downgrading metadata version from {} to 1 for upgrade from 0.10.0.x.", SubscriptionInfo.LATEST_SUPPORTED_VERSION);
                    userMetadataVersion = 1;
                    break;
                case StreamsConfig.UPGRADE_FROM_0101:
                case StreamsConfig.UPGRADE_FROM_0102:
                case StreamsConfig.UPGRADE_FROM_0110:
                case StreamsConfig.UPGRADE_FROM_10:
                case StreamsConfig.UPGRADE_FROM_11:
                    log.info("Downgrading metadata version from {} to 2 for upgrade from " + upgradeFrom + ".x.", SubscriptionInfo.LATEST_SUPPORTED_VERSION);
                    userMetadataVersion = 2;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom);
            }
        }

        final Object o = configs.get(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR);
        if (o == null) {
            final KafkaException fatalException = new KafkaException("TaskManager is not specified");
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        if (!(o instanceof TaskManager)) {
            final KafkaException fatalException = new KafkaException(String.format("%s is not an instance of %s", o.getClass().getName(), TaskManager.class.getName()));
            log.error(fatalException.getMessage(), fatalException);
            throw fatalException;
        }

        taskManager = (TaskManager) o;

        numStandbyReplicas = streamsConfig.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);

        partitionGrouper = streamsConfig.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper.class);

        final String userEndPoint = streamsConfig.getString(StreamsConfig.APPLICATION_SERVER_CONFIG);
        if (userEndPoint != null && !userEndPoint.isEmpty()) {
            try {
                final String host = getHost(userEndPoint);
                final Integer port = getPort(userEndPoint);

                if (host == null || port == null)
                    throw new ConfigException(String.format("%s Config %s isn't in the correct format. Expected a host:port pair" +
                                    " but received %s",
                            logPrefix, StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
            } catch (final NumberFormatException nfe) {
                throw new ConfigException(String.format("%s Invalid port supplied in %s for config %s",
                        logPrefix, userEndPoint, StreamsConfig.APPLICATION_SERVER_CONFIG));
            }

            this.userEndPoint = userEndPoint;
        }

        internalTopicManager = new InternalTopicManager(taskManager.adminClient, streamsConfig);

        copartitionedTopicsValidator = new CopartitionedTopicsValidator(logPrefix);
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public Subscription subscription(final Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        final Set<TaskId> previousActiveTasks = taskManager.prevActiveTaskIds();
        final Set<TaskId> standbyTasks = taskManager.cachedTasksIds();
        standbyTasks.removeAll(previousActiveTasks);
        final SubscriptionInfo data = new SubscriptionInfo(
            userMetadataVersion,
            taskManager.processId(),
            previousActiveTasks,
            standbyTasks,
            this.userEndPoint);

        taskManager.updateSubscriptionsFromMetadata(topics);

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
    public Map<String, Assignment> assign(final Cluster metadata,
                                          final Map<String, Subscription> subscriptions) {
        // construct the client metadata from the decoded subscription info
        final Map<UUID, ClientMetadata> clientsMetadata = new HashMap<>();

        int minUserMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION;
        for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            final String consumerId = entry.getKey();
            final Subscription subscription = entry.getValue();

            final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            final int usedVersion = info.version();
            if (usedVersion > SubscriptionInfo.LATEST_SUPPORTED_VERSION) {
                throw new IllegalStateException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + SubscriptionInfo.LATEST_SUPPORTED_VERSION);
            }
            if (usedVersion < minUserMetadataVersion) {
                minUserMetadataVersion = usedVersion;
            }

            // create the new client metadata if necessary
            ClientMetadata clientMetadata = clientsMetadata.get(info.processId());

            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint());
                clientsMetadata.put(info.processId(), clientMetadata);
            }

            // add the consumer to the client
            clientMetadata.addConsumer(consumerId, info);
        }

        log.debug("Constructed client metadata {} from the member subscriptions.", clientsMetadata);

        // ---------------- Step Zero ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = taskManager.builder().topicGroups();

        final Map<String, InternalTopicMetadata> repartitionTopicMetadata = new HashMap<>();
        for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
            for (final InternalTopicConfig topic: topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), new InternalTopicMetadata(topic));
            }
        }

        boolean numPartitionsNeeded;
        do {
            numPartitionsNeeded = false;

            for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                for (final String topicName : topicsInfo.repartitionSourceTopics.keySet()) {
                    int numPartitions = repartitionTopicMetadata.get(topicName).numPartitions;

                    // try set the number of partitions for this repartition topic if it is not set yet
                    if (numPartitions == UNKNOWN) {
                        for (final InternalTopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            final Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (final String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    final Integer numPartitionsCandidate;
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
                        if (numPartitions == UNKNOWN) {
                            numPartitionsNeeded = true;
                        } else {
                            repartitionTopicMetadata.get(topicName).numPartitions = numPartitions;
                        }
                    }
                }
            }
        } while (numPartitionsNeeded);


        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(taskManager.builder().copartitionGroups(), repartitionTopicMetadata, metadata);

        // make sure the repartition source topics exist with the right number of partitions,
        // create these topics if necessary
        prepareTopic(repartitionTopicMetadata);

        // augment the metadata with the newly computed number of partitions for all the
        // repartition source topics
        final Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new HashMap<>();
        for (final Map.Entry<String, InternalTopicMetadata> entry : repartitionTopicMetadata.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue().numPartitions;

            for (int partition = 0; partition < numPartitions; partition++) {
                allRepartitionTopicPartitions.put(new TopicPartition(topic, partition),
                        new PartitionInfo(topic, partition, null, new Node[0], new Node[0]));
            }
        }

        final Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);
        taskManager.setClusterMetadata(fullMetadata);

        log.debug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.values());

        // ---------------- Step One ---------------- //

        // get the tasks as partition groups from the partition grouper
        final Set<String> allSourceTopics = new HashSet<>();
        final Map<Integer, Set<String>> sourceTopicsByGroup = new HashMap<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            allSourceTopics.addAll(entry.getValue().sourceTopics);
            sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
        }

        final Map<TaskId, Set<TopicPartition>> partitionsForTask = partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        final Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        final Map<Integer, Set<TaskId>> tasksByTopicGroup = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            final Set<TopicPartition> partitions = entry.getValue();
            for (final TopicPartition partition : partitions) {
                if (allAssignedPartitions.contains(partition)) {
                    log.warn("Partition {} is assigned to more than one tasks: {}", partition, partitionsForTask);
                }
            }
            allAssignedPartitions.addAll(partitions);

            final TaskId id = entry.getKey();
            Set<TaskId> ids = tasksByTopicGroup.get(id.topicGroupId);
            if (ids == null) {
                ids = new HashSet<>();
                tasksByTopicGroup.put(id.topicGroupId, ids);
            }
            ids.add(id);
        }
        for (final String topic : allSourceTopics) {
            final List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
            if (!partitionInfoList.isEmpty()) {
                for (final PartitionInfo partitionInfo : partitionInfoList) {
                    final TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    if (!allAssignedPartitions.contains(partition)) {
                        log.warn("Partition {} is not assigned to any tasks: {}"
                                 + " Possible causes of a partition not getting assigned"
                                 + " is that another topic defined in the topology has not been"
                                 + " created when starting your streams application,"
                                 + " resulting in no tasks created for this topology at all.", partition, partitionsForTask);
                    }
                }
            } else {
                log.warn("No partitions found for topic {}", topic);
            }
        }

        // add tasks to state change log topic subscribers
        final Map<String, InternalTopicMetadata> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final Map<String, InternalTopicConfig> stateChangelogTopics = entry.getValue().stateChangelogTopics;

            for (final InternalTopicConfig topicConfig : stateChangelogTopics.values()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                if (tasksByTopicGroup.get(topicGroupId) != null) {
                    for (final TaskId task : tasksByTopicGroup.get(topicGroupId)) {
                        if (numPartitions < task.partition + 1)
                            numPartitions = task.partition + 1;
                    }
                    final InternalTopicMetadata topicMetadata = new InternalTopicMetadata(topicConfig);
                    topicMetadata.numPartitions = numPartitions;

                    changelogTopicMetadata.put(topicConfig.name(), topicMetadata);
                } else {
                    log.debug("No tasks found for topic group {}", topicGroupId);
                }
            }
        }

        prepareTopic(changelogTopicMetadata);

        log.debug("Created state changelog topics {} from the parsed topology.", changelogTopicMetadata.values());

        // ---------------- Step Two ---------------- //

        // assign tasks to clients
        final Map<UUID, ClientState> states = new HashMap<>();
        for (final Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            states.put(entry.getKey(), entry.getValue().state);
        }

        log.debug("Assigning tasks {} to clients {} with number of replicas {}",
                partitionsForTask.keySet(), states, numStandbyReplicas);

        final StickyTaskAssignor<UUID> taskAssignor = new StickyTaskAssignor<>(states, partitionsForTask.keySet());
        taskAssignor.assign(numStandbyReplicas);

        log.info("Assigned tasks to clients as {}.", states);

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map
        final Map<HostInfo, Set<TopicPartition>> partitionsByHostState = new HashMap<>();
        if (minUserMetadataVersion == 2 || minUserMetadataVersion == 3) {
            for (final Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
                final HostInfo hostInfo = entry.getValue().hostInfo;

                if (hostInfo != null) {
                    final Set<TopicPartition> topicPartitions = new HashSet<>();
                    final ClientState state = entry.getValue().state;

                    for (final TaskId id : state.activeTasks()) {
                        topicPartitions.addAll(partitionsForTask.get(id));
                    }

                    partitionsByHostState.put(hostInfo, topicPartitions);
                }
            }
        }
        taskManager.setPartitionsByHostState(partitionsByHostState);

        // within the client, distribute tasks to its owned consumers
        final Map<String, Assignment> assignment = new HashMap<>();
        for (final Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            final Set<String> consumers = entry.getValue().consumers;
            final ClientState state = entry.getValue().state;

            final List<List<TaskId>> interleavedActive = interleaveTasksByGroupId(state.activeTasks(), consumers.size());
            final List<List<TaskId>> interleavedStandby = interleaveTasksByGroupId(state.standbyTasks(), consumers.size());

            int consumerTaskIndex = 0;

            for (final String consumer : consumers) {
                final Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();
                final ArrayList<AssignedPartition> assignedPartitions = new ArrayList<>();

                final List<TaskId> assignedActiveList = interleavedActive.get(consumerTaskIndex);

                for (final TaskId taskId : assignedActiveList) {
                    for (final TopicPartition partition : partitionsForTask.get(taskId)) {
                        assignedPartitions.add(new AssignedPartition(taskId, partition));
                    }
                }

                if (!state.standbyTasks().isEmpty()) {
                    final List<TaskId> assignedStandbyList = interleavedStandby.get(consumerTaskIndex);
                    for (final TaskId taskId : assignedStandbyList) {
                        Set<TopicPartition> standbyPartitions = standby.get(taskId);
                        if (standbyPartitions == null) {
                            standbyPartitions = new HashSet<>();
                            standby.put(taskId, standbyPartitions);
                        }
                        standbyPartitions.addAll(partitionsForTask.get(taskId));
                    }
                }

                consumerTaskIndex++;

                Collections.sort(assignedPartitions);
                final List<TaskId> active = new ArrayList<>();
                final List<TopicPartition> activePartitions = new ArrayList<>();
                for (final AssignedPartition partition : assignedPartitions) {
                    active.add(partition.taskId);
                    activePartitions.add(partition.partition);
                }

                // finally, encode the assignment before sending back to coordinator
                assignment.put(consumer, new Assignment(
                    activePartitions,
                    new AssignmentInfo(minUserMetadataVersion, active, standby, partitionsByHostState).encode()));
            }
        }

        return assignment;
    }

    // visible for testing
    List<List<TaskId>> interleaveTasksByGroupId(final Collection<TaskId> taskIds, final int numberThreads) {
        final LinkedList<TaskId> sortedTasks = new LinkedList<>(taskIds);
        Collections.sort(sortedTasks);
        final List<List<TaskId>> taskIdsForConsumerAssignment = new ArrayList<>(numberThreads);
        for (int i = 0; i < numberThreads; i++) {
            taskIdsForConsumerAssignment.add(new ArrayList<TaskId>());
        }
        while (!sortedTasks.isEmpty()) {
            for (final List<TaskId> taskIdList : taskIdsForConsumerAssignment) {
                final TaskId taskId = sortedTasks.poll();
                if (taskId == null) {
                    break;
                }
                taskIdList.add(taskId);
            }
        }
        return taskIdsForConsumerAssignment;
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(final Assignment assignment) {
        final List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
        Collections.sort(partitions, PARTITION_COMPARATOR);

        final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
        final int usedVersion = info.version();

        // version 1 field
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        // version 2 fields
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost;

        switch (usedVersion) {
            case 1:
                processVersionOneAssignment(info, partitions, activeTasks);
                partitionsByHost = Collections.emptyMap();
                break;
            case 2:
                processVersionTwoAssignment(info, partitions, activeTasks, topicToPartitionInfo);
                partitionsByHost = info.partitionsByHost();
                break;
            case 3:
                processVersionThreeAssignment(info, partitions, activeTasks, topicToPartitionInfo);
                partitionsByHost = info.partitionsByHost();
                break;
            default:
                throw new IllegalStateException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + AssignmentInfo.LATEST_SUPPORTED_VERSION);
        }

        taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
        taskManager.setPartitionsByHostState(partitionsByHost);
        taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
        taskManager.updateSubscriptionsFromAssignment(partitions);
    }

    private void processVersionOneAssignment(final AssignmentInfo info,
                                             final List<TopicPartition> partitions,
                                             final Map<TaskId, Set<TopicPartition>> activeTasks) {
        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks().size()) {
            throw new TaskAssignmentException(
                String.format("%sNumber of assigned partitions %d is not equal to the number of active taskIds %d" +
                    ", assignmentInfo=%s", logPrefix, partitions.size(), info.activeTasks().size(), info.toString())
            );
        }

        for (int i = 0; i < partitions.size(); i++) {
            final TopicPartition partition = partitions.get(i);
            final TaskId id = info.activeTasks().get(i);

            Set<TopicPartition> assignedPartitions = activeTasks.get(id);
            if (assignedPartitions == null) {
                assignedPartitions = new HashSet<>();
                activeTasks.put(id, assignedPartitions);
            }
            assignedPartitions.add(partition);
        }
    }

    private void processVersionTwoAssignment(final AssignmentInfo info,
                                             final List<TopicPartition> partitions,
                                             final Map<TaskId, Set<TopicPartition>> activeTasks,
                                             final Map<TopicPartition, PartitionInfo> topicToPartitionInfo) {
        processVersionOneAssignment(info, partitions, activeTasks);

        // process partitions by host
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = info.partitionsByHost();
        for (final Set<TopicPartition> value : partitionsByHost.values()) {
            for (final TopicPartition topicPartition : value) {
                topicToPartitionInfo.put(
                    topicPartition,
                    new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, new Node[0], new Node[0]));
            }
        }
    }

    private void processVersionThreeAssignment(final AssignmentInfo info,
                                               final List<TopicPartition> partitions,
                                               final Map<TaskId, Set<TopicPartition>> activeTasks,
                                               final Map<TopicPartition, PartitionInfo> topicToPartitionInfo) {
        processVersionTwoAssignment(info, partitions, activeTasks, topicToPartitionInfo);
    }

    /**
     * Internal helper function that creates a Kafka topic
     *
     * @param topicPartitions Map that contains the topic names to be created with the number of partitions
     */
    @SuppressWarnings("deprecation")
    private void prepareTopic(final Map<String, InternalTopicMetadata> topicPartitions) {
        log.debug("Starting to validate internal topics in partition assignor.");

        // first construct the topics to make ready
        final Map<String, InternalTopicConfig> topicsToMakeReady = new HashMap<>();

        for (final InternalTopicMetadata metadata : topicPartitions.values()) {
            final InternalTopicConfig topic = metadata.config;
            final int numPartitions = metadata.numPartitions;

            if (numPartitions == NOT_AVAILABLE) {
                continue;
            }
            if (numPartitions < 0) {
                throw new StreamsException(String.format("%sTopic [%s] number of partitions not defined", logPrefix, topic.name()));
            }

            topic.setNumberOfPartitions(numPartitions);
            topicsToMakeReady.put(topic.name(), topic);
        }

        if (!topicsToMakeReady.isEmpty()) {
            internalTopicManager.makeReady(topicsToMakeReady);
        }

        log.debug("Completed validating internal topics in partition assignor.");
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                                      final Cluster metadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsValidator.validate(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
        }
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     *
     * // TODO: this is a duplicate of the InternalTopologyBuilder#SubscriptionUpdates
     *          and is maintained only for compatibility of the deprecated TopologyBuilder API
     */
    public static class SubscriptionUpdates {

        private final Set<String> updatedTopicSubscriptions = new HashSet<>();

        public void updateTopics(final Collection<String> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<String> getUpdates() {
            return Collections.unmodifiableSet(updatedTopicSubscriptions);
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

    static class CopartitionedTopicsValidator {
        private final String logPrefix;

        CopartitionedTopicsValidator(final String logPrefix) {
            this.logPrefix = logPrefix;
        }

        @SuppressWarnings("deprecation")
        void validate(final Set<String> copartitionGroup,
                      final Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                      final Cluster metadata) {
            int numPartitions = UNKNOWN;

            for (final String topic : copartitionGroup) {
                if (!allRepartitionTopicsNumPartitions.containsKey(topic)) {
                    final Integer partitions = metadata.partitionCountForTopic(topic);

                    if (partitions == null) {
                        throw new org.apache.kafka.streams.errors.TopologyBuilderException(String.format("%sTopic not found: %s", logPrefix, topic));
                    }

                    if (numPartitions == UNKNOWN) {
                        numPartitions = partitions;
                    } else if (numPartitions != partitions) {
                        final String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                        Arrays.sort(topics);
                        throw new org.apache.kafka.streams.errors.TopologyBuilderException(String.format("%sTopics not co-partitioned: [%s]", logPrefix, Utils.join(Arrays.asList(topics), ",")));
                    }
                } else if (allRepartitionTopicsNumPartitions.get(topic).numPartitions == NOT_AVAILABLE) {
                    numPartitions = NOT_AVAILABLE;
                    break;
                }
            }

            // if all topics for this co-partition group is repartition topics,
            // then set the number of partitions to be the maximum of the number of partitions.
            if (numPartitions == UNKNOWN) {
                for (final Map.Entry<String, InternalTopicMetadata> entry: allRepartitionTopicsNumPartitions.entrySet()) {
                    if (copartitionGroup.contains(entry.getKey())) {
                        final int partitions = entry.getValue().numPartitions;
                        if (partitions > numPartitions) {
                            numPartitions = partitions;
                        }
                    }
                }
            }
            // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
            for (final Map.Entry<String, InternalTopicMetadata> entry : allRepartitionTopicsNumPartitions.entrySet()) {
                if (copartitionGroup.contains(entry.getKey())) {
                    entry.getValue().numPartitions = numPartitions;
                }
            }

        }
    }

    // following functions are for test only
    void setInternalTopicManager(final InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

}
