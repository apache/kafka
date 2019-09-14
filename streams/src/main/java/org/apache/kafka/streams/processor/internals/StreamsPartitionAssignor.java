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

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.PartitionGrouper;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_FIVE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_FOUR;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_ONE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_THREE;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.VERSION_TWO;

public class StreamsPartitionAssignor implements ConsumerPartitionAssignor, Configurable {
    private Logger log;
    private String logPrefix;

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        private final TaskId taskId;
        private final TopicPartition partition;

        AssignedPartition(final TaskId taskId,
                          final TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(final AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(partition, that.partition);
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
        private final HostInfo hostInfo;
        private final Set<String> consumers;
        private final ClientState state;

        ClientMetadata(final String endPoint) {

            // get the host info if possible
            if (endPoint != null) {
                final String host = getHost(endPoint);
                final Integer port = getPort(endPoint);

                if (host == null || port == null) {
                    throw new ConfigException(
                        String.format("Error parsing host address %s. Expected format host:port.", endPoint)
                    );
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


    protected static final Comparator<TopicPartition> PARTITION_COMPARATOR =
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private String userEndPoint;
    private int numStandbyReplicas;

    private TaskManager taskManager;
    private PartitionGrouper partitionGrouper;
    private AtomicInteger assignmentErrorCode;

    protected int usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;

    protected String userEndPoint() {
        return userEndPoint;
    }

    protected TaskManager taskManger() {
        return taskManager;
    }

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     *
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(configs);

        logPrefix = assignorConfiguration.logPrefix();
        log = new LogContext(logPrefix).logger(getClass());
        usedSubscriptionMetadataVersion = assignorConfiguration.configuredMetadataVersion(usedSubscriptionMetadataVersion);
        taskManager = assignorConfiguration.getTaskManager();
        assignmentErrorCode = assignorConfiguration.getAssignmentErrorCode(configs);
        numStandbyReplicas = assignorConfiguration.getNumStandbyReplicas();
        partitionGrouper = assignorConfiguration.getPartitionGrouper();
        userEndPoint = assignorConfiguration.getUserEndPoint();
        internalTopicManager = assignorConfiguration.getInternalTopicManager();
        copartitionedTopicsEnforcer = assignorConfiguration.getCopartitionedTopicsEnforcer();
    }


    @Override
    public String name() {
        return "stream";
    }

    @Override
    public ByteBuffer subscriptionUserData(final Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        final Set<TaskId> previousActiveTasks = taskManager.prevActiveTaskIds();
        final Set<TaskId> standbyTasks = taskManager.cachedTasksIds();
        standbyTasks.removeAll(previousActiveTasks);
        final SubscriptionInfo data = new SubscriptionInfo(
            usedSubscriptionMetadataVersion,
            taskManager.processId(),
            previousActiveTasks,
            standbyTasks,
            userEndPoint);

        taskManager.updateSubscriptionsFromMetadata(topics);

        return data.encode();
    }

    private Map<String, Assignment> errorAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                    final String topic,
                                                    final int errorCode) {
        log.error("{} is unknown yet during rebalance," +
                      " please make sure they have been pre-created before starting the Streams application.", topic);
        final Map<String, Assignment> assignment = new HashMap<>();
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            for (final String consumerId : clientMetadata.consumers) {
                assignment.put(consumerId, new Assignment(
                    Collections.emptyList(),
                    new AssignmentInfo(LATEST_SUPPORTED_VERSION,
                                       Collections.emptyList(),
                                       Collections.emptyMap(),
                                       Collections.emptyMap(),
                                       errorCode).encode()
                ));
            }
        }
        return assignment;
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
    public GroupAssignment assign(final Cluster metadata, final GroupSubscription groupSubscription) {
        final Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        // construct the client metadata from the decoded subscription info
        final Map<UUID, ClientMetadata> clientMetadataMap = new HashMap<>();
        final Set<String> futureConsumers = new HashSet<>();

        int minReceivedMetadataVersion = LATEST_SUPPORTED_VERSION;

        int futureMetadataVersion = UNKNOWN;
        for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            final String consumerId = entry.getKey();
            final Subscription subscription = entry.getValue();

            final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            final int usedVersion = info.version();
            if (usedVersion > LATEST_SUPPORTED_VERSION) {
                futureMetadataVersion = usedVersion;
                futureConsumers.add(consumerId);
                continue;
            }
            if (usedVersion < minReceivedMetadataVersion) {
                minReceivedMetadataVersion = usedVersion;
            }

            // create the new client metadata if necessary
            ClientMetadata clientMetadata = clientMetadataMap.get(info.processId());

            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint());
                clientMetadataMap.put(info.processId(), clientMetadata);
            }

            // add the consumer to the client
            clientMetadata.addConsumer(consumerId, info);
        }

        final boolean versionProbing;
        if (futureMetadataVersion == UNKNOWN) {
            versionProbing = false;
        } else {
            if (minReceivedMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {
                log.info("Received a future (version probing) subscription (version: {})."
                             + " Sending empty assignment back (with supported version {}).",
                         futureMetadataVersion,
                         LATEST_SUPPORTED_VERSION);
                versionProbing = true;
            } else {
                throw new IllegalStateException(
                    "Received a future (version probing) subscription (version: " + futureMetadataVersion
                        + ") and an incompatible pre Kafka 2.0 subscription (version: " + minReceivedMetadataVersion
                        + ") at the same time."
                );
            }
        }

        if (minReceivedMetadataVersion < LATEST_SUPPORTED_VERSION) {
            log.info("Downgrading metadata to version {}. Latest supported version is {}.",
                     minReceivedMetadataVersion,
                     LATEST_SUPPORTED_VERSION);
        }

        log.debug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

        // ---------------- Step Zero ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups = taskManager.builder().topicGroups();

        final Map<String, InternalTopicConfig> repartitionTopicMetadata = new HashMap<>();
        for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
            for (final String topic : topicsInfo.sourceTopics) {
                if (!topicsInfo.repartitionSourceTopics.keySet().contains(topic) &&
                    !metadata.topics().contains(topic)) {
                    log.error("Missing source topic {} during assignment. Returning error {}.",
                              topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.name());
                    return new GroupAssignment(
                        errorAssignment(clientMetadataMap, topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
                    );
                }
            }
            for (final InternalTopicConfig topic : topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), topic);
            }
        }

        boolean numPartitionsNeeded;
        do {
            numPartitionsNeeded = false;

            for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                for (final String topicName : topicsInfo.repartitionSourceTopics.keySet()) {
                    final Optional<Integer> maybeNumPartitions = repartitionTopicMetadata.get(topicName).numberOfPartitions();
                    Integer numPartitions = null;

                    if (!maybeNumPartitions.isPresent()) {
                        // try set the number of partitions for this repartition topic if it is not set yet
                        for (final InternalTopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            final Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (final String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    final int numPartitionsCandidate;
                                    // It is possible the sourceTopic is another internal topic, i.e,
                                    // map().join().join(map())
                                    if (repartitionTopicMetadata.containsKey(sourceTopicName)
                                        && repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().isPresent()) {
                                        numPartitionsCandidate = repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().get();
                                    } else {
                                        final Integer count = metadata.partitionCountForTopic(sourceTopicName);
                                        if (count == null) {
                                            throw new IllegalStateException(
                                                "No partition count found for source topic "
                                                    + sourceTopicName
                                                    + ", but it should have been."
                                            );
                                        }
                                        numPartitionsCandidate = count;
                                    }

                                    if (numPartitions == null || numPartitionsCandidate > numPartitions) {
                                        numPartitions = numPartitionsCandidate;
                                    }
                                }
                            }
                        }
                        // if we still have not find the right number of partitions,
                        // another iteration is needed
                        if (numPartitions == null) {
                            numPartitionsNeeded = true;
                        } else {
                            repartitionTopicMetadata.get(topicName).setNumberOfPartitions(numPartitions);
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
        for (final Map.Entry<String, InternalTopicConfig> entry : repartitionTopicMetadata.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue().numberOfPartitions().orElse(-1);

            for (int partition = 0; partition < numPartitions; partition++) {
                allRepartitionTopicPartitions.put(
                    new TopicPartition(topic, partition),
                    new PartitionInfo(topic, partition, null, new Node[0], new Node[0])
                );
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

        final Map<TaskId, Set<TopicPartition>> partitionsForTask =
            partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

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
            tasksByTopicGroup.computeIfAbsent(id.topicGroupId, k -> new HashSet<>()).add(id);
        }
        for (final String topic : allSourceTopics) {
            final List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
            if (partitionInfoList.isEmpty()) {
                log.warn("No partitions found for topic {}", topic);
            } else {
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
            }
        }

        // add tasks to state change log topic subscribers
        final Map<String, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<Integer, InternalTopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final Map<String, InternalTopicConfig> stateChangelogTopics = entry.getValue().stateChangelogTopics;

            for (final InternalTopicConfig topicConfig : stateChangelogTopics.values()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                if (tasksByTopicGroup.get(topicGroupId) != null) {
                    for (final TaskId task : tasksByTopicGroup.get(topicGroupId)) {
                        if (numPartitions < task.partition + 1) {
                            numPartitions = task.partition + 1;
                        }
                    }
                    topicConfig.setNumberOfPartitions(numPartitions);

                    changelogTopicMetadata.put(topicConfig.name(), topicConfig);
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
        for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
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
        if (minReceivedMetadataVersion >= 2) {
            for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
                final HostInfo hostInfo = entry.getValue().hostInfo;

                // if application server is configured, also include host state map
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

        final Map<String, Assignment> assignment;
        if (versionProbing) {
            assignment = versionProbingAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHostState,
                futureConsumers,
                minReceivedMetadataVersion
            );
        } else {
            assignment = computeNewAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHostState,
                minReceivedMetadataVersion
            );
        }

        return new GroupAssignment(assignment);
    }

    private static Map<String, Assignment> computeNewAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                                final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                                final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                                final int minUserMetadataVersion) {
        final Map<String, Assignment> assignment = new HashMap<>();

        // within the client, distribute tasks to its owned consumers
        for (final Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            final Set<String> consumers = entry.getValue().consumers;
            final ClientState state = entry.getValue().state;

            final List<List<TaskId>> interleavedActive =
                interleaveTasksByGroupId(state.activeTasks(), consumers.size());
            final List<List<TaskId>> interleavedStandby =
                interleaveTasksByGroupId(state.standbyTasks(), consumers.size());

            int consumerTaskIndex = 0;

            for (final String consumer : consumers) {
                final Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();
                final List<AssignedPartition> assignedPartitions = new ArrayList<>();

                final List<TaskId> assignedActiveList = interleavedActive.get(consumerTaskIndex);

                for (final TaskId taskId : assignedActiveList) {
                    for (final TopicPartition partition : partitionsForTask.get(taskId)) {
                        assignedPartitions.add(new AssignedPartition(taskId, partition));
                    }
                }

                if (!state.standbyTasks().isEmpty()) {
                    final List<TaskId> assignedStandbyList = interleavedStandby.get(consumerTaskIndex);
                    for (final TaskId taskId : assignedStandbyList) {
                        standby.computeIfAbsent(taskId, k -> new HashSet<>()).addAll(partitionsForTask.get(taskId));
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
                assignment.put(
                    consumer,
                    new Assignment(
                        activePartitions,
                        new AssignmentInfo(
                            minUserMetadataVersion,
                            active,
                            standby,
                            partitionsByHostState,
                            0
                        ).encode()
                    )
                );
            }
        }

        return assignment;
    }

    private static Map<String, Assignment> versionProbingAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                                    final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                                    final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                                    final Set<String> futureConsumers,
                                                                    final int minUserMetadataVersion) {
        final Map<String, Assignment> assignment = new HashMap<>();

        // assign previously assigned tasks to "old consumers"
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            for (final String consumerId : clientMetadata.consumers) {

                if (futureConsumers.contains(consumerId)) {
                    continue;
                }

                final List<TaskId> activeTasks = new ArrayList<>(clientMetadata.state.prevActiveTasks());

                final List<TopicPartition> assignedPartitions = new ArrayList<>();
                for (final TaskId taskId : activeTasks) {
                    assignedPartitions.addAll(partitionsForTask.get(taskId));
                }

                final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();
                for (final TaskId taskId : clientMetadata.state.prevStandbyTasks()) {
                    standbyTasks.put(taskId, partitionsForTask.get(taskId));
                }

                assignment.put(consumerId, new Assignment(
                    assignedPartitions,
                    new AssignmentInfo(
                        minUserMetadataVersion,
                        activeTasks,
                        standbyTasks,
                        partitionsByHostState,
                        0)
                        .encode()
                ));
            }
        }

        // add empty assignment for "future version" clients (ie, empty version probing response)
        for (final String consumerId : futureConsumers) {
            assignment.put(consumerId, new Assignment(
                Collections.emptyList(),
                new AssignmentInfo().encode()
            ));
        }

        return assignment;
    }

    // visible for testing
    static List<List<TaskId>> interleaveTasksByGroupId(final Collection<TaskId> taskIds, final int numberThreads) {
        final LinkedList<TaskId> sortedTasks = new LinkedList<>(taskIds);
        Collections.sort(sortedTasks);
        final List<List<TaskId>> taskIdsForConsumerAssignment = new ArrayList<>(numberThreads);
        for (int i = 0; i < numberThreads; i++) {
            taskIdsForConsumerAssignment.add(new ArrayList<>());
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

    private void upgradeSubscriptionVersionIfNeeded(final int leaderSupportedVersion) {
        if (leaderSupportedVersion > usedSubscriptionMetadataVersion) {
            log.info("Sent a version {} subscription and group leader's latest supported version is {}. " +
                         "Upgrading subscription metadata version to {} for next rebalance.",
                     usedSubscriptionMetadataVersion,
                     leaderSupportedVersion,
                     leaderSupportedVersion);
            usedSubscriptionMetadataVersion = leaderSupportedVersion;
        }
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(final Assignment assignment, final ConsumerGroupMetadata metadata) {
        final List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
        partitions.sort(PARTITION_COMPARATOR);

        final AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
        if (info.errCode() != AssignorError.NONE.code()) {
            // set flag to shutdown streams app
            assignmentErrorCode.set(info.errCode());
            return;
        }
        final int receivedAssignmentMetadataVersion = info.version();
        final int leaderSupportedVersion = info.latestSupportedVersion();

        if (receivedAssignmentMetadataVersion > usedSubscriptionMetadataVersion) {
            throw new IllegalStateException(
                "Sent a version " + usedSubscriptionMetadataVersion
                    + " subscription but got an assignment with higher version "
                    + receivedAssignmentMetadataVersion + "."
            );
        }

        if (receivedAssignmentMetadataVersion < usedSubscriptionMetadataVersion
            && receivedAssignmentMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {

            if (receivedAssignmentMetadataVersion == leaderSupportedVersion) {
                log.info(
                    "Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                        "Downgrading subscription metadata to received version and trigger new rebalance.",
                    usedSubscriptionMetadataVersion,
                    receivedAssignmentMetadataVersion
                );
                usedSubscriptionMetadataVersion = receivedAssignmentMetadataVersion;
            } else {
                log.info(
                    "Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                        "Setting subscription metadata to leaders supported version {} and trigger new rebalance.",
                    usedSubscriptionMetadataVersion,
                    receivedAssignmentMetadataVersion,
                    leaderSupportedVersion
                );
                usedSubscriptionMetadataVersion = leaderSupportedVersion;
            }

            assignmentErrorCode.set(AssignorError.VERSION_PROBING.code());
            return;
        }

        // version 1 field
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        // version 2 fields
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost;

        switch (receivedAssignmentMetadataVersion) {
            case VERSION_ONE:
                processVersionOneAssignment(logPrefix, info, partitions, activeTasks);
                partitionsByHost = Collections.emptyMap();
                break;
            case VERSION_TWO:
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo);
                partitionsByHost = info.partitionsByHost();
                break;
            case VERSION_THREE:
            case VERSION_FOUR:
            case VERSION_FIVE:
                upgradeSubscriptionVersionIfNeeded(leaderSupportedVersion);
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo);
                partitionsByHost = info.partitionsByHost();
                break;
            default:
                throw new IllegalStateException(
                    "This code should never be reached."
                        + " Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/"
                );
        }

        taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
        taskManager.setPartitionsByHostState(partitionsByHost);
        taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
        taskManager.updateSubscriptionsFromAssignment(partitions);
    }

    private static void processVersionOneAssignment(final String logPrefix,
                                                    final AssignmentInfo info,
                                                    final List<TopicPartition> partitions,
                                                    final Map<TaskId, Set<TopicPartition>> activeTasks) {
        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks().size()) {
            throw new TaskAssignmentException(
                String.format(
                    "%sNumber of assigned partitions %d is not equal to "
                        + "the number of active taskIds %d, assignmentInfo=%s",
                    logPrefix, partitions.size(),
                    info.activeTasks().size(), info.toString()
                )
            );
        }

        for (int i = 0; i < partitions.size(); i++) {
            final TopicPartition partition = partitions.get(i);
            final TaskId id = info.activeTasks().get(i);
            activeTasks.computeIfAbsent(id, k -> new HashSet<>()).add(partition);
        }
    }

    public static void processVersionTwoAssignment(final String logPrefix,
                                            final AssignmentInfo info,
                                            final List<TopicPartition> partitions,
                                            final Map<TaskId, Set<TopicPartition>> activeTasks,
                                            final Map<TopicPartition, PartitionInfo> topicToPartitionInfo) {
        processVersionOneAssignment(logPrefix, info, partitions, activeTasks);

        // process partitions by host
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = info.partitionsByHost();
        for (final Set<TopicPartition> value : partitionsByHost.values()) {
            for (final TopicPartition topicPartition : value) {
                topicToPartitionInfo.put(
                    topicPartition,
                    new PartitionInfo(
                        topicPartition.topic(),
                        topicPartition.partition(),
                        null,
                        new Node[0],
                        new Node[0]
                    )
                );
            }
        }
    }

    /**
     * Internal helper function that creates a Kafka topic
     *
     * @param topicPartitions Map that contains the topic names to be created with the number of partitions
     */
    private void prepareTopic(final Map<String, InternalTopicConfig> topicPartitions) {
        log.debug("Starting to validate internal topics {} in partition assignor.", topicPartitions);

        // first construct the topics to make ready
        final Map<String, InternalTopicConfig> topicsToMakeReady = new HashMap<>();

        for (final InternalTopicConfig topic : topicPartitions.values()) {
            final Optional<Integer> numPartitions = topic.numberOfPartitions();
            if (!numPartitions.isPresent()) {
                throw new StreamsException(
                    String.format("%sTopic [%s] number of partitions not defined",
                                  logPrefix, topic.name())
                );
            }

            topic.setNumberOfPartitions(numPartitions.get());
            topicsToMakeReady.put(topic.name(), topic);
        }

        if (!topicsToMakeReady.isEmpty()) {
            internalTopicManager.makeReady(topicsToMakeReady);
        }

        log.debug("Completed validating internal topics {} in partition assignor.", topicPartitions);
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                                      final Cluster metadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
        }
    }


    // following functions are for test only
    void setInternalTopicManager(final InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

}
