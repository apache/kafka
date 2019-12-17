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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;

public class StreamsPartitionAssignor implements ConsumerPartitionAssignor, Configurable {

    private Logger log;
    private String logPrefix;

    private static class AssignedPartition implements Comparable<AssignedPartition> {

        private final TaskId taskId;
        private final TopicPartition partition;

        AssignedPartition(final TaskId taskId, final TopicPartition partition) {
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

        void addConsumer(final String consumerMemberId, final List<TopicPartition> ownedPartitions) {
            consumers.add(consumerMemberId);
            state.incrementCapacity();
            state.addOwnedPartitions(ownedPartitions, consumerMemberId);
        }

        void addPreviousTasks(final SubscriptionInfo info) {
            state.addPreviousActiveTasks(info.prevTasks());
            state.addPreviousStandbyTasks(info.standbyTasks());
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
    @SuppressWarnings("deprecation")
    private org.apache.kafka.streams.processor.PartitionGrouper partitionGrouper;
    private AtomicInteger assignmentErrorCode;

    protected int usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private RebalanceProtocol rebalanceProtocol;

    protected String userEndPoint() {
        return userEndPoint;
    }

    protected TaskManager taskManger() {
        return taskManager;
    }

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible since the former needs
     * later's cached metadata while sending subscriptions, and the latter needs former's returned assignment when
     * adding tasks.
     *
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(configs);

        logPrefix = assignorConfiguration.logPrefix();
        log = new LogContext(logPrefix).logger(getClass());
        usedSubscriptionMetadataVersion = assignorConfiguration
            .configuredMetadataVersion(usedSubscriptionMetadataVersion);
        taskManager = assignorConfiguration.getTaskManager();
        assignmentErrorCode = assignorConfiguration.getAssignmentErrorCode(configs);
        numStandbyReplicas = assignorConfiguration.getNumStandbyReplicas();
        partitionGrouper = assignorConfiguration.getPartitionGrouper();
        userEndPoint = assignorConfiguration.getUserEndPoint();
        internalTopicManager = assignorConfiguration.getInternalTopicManager();
        copartitionedTopicsEnforcer = assignorConfiguration.getCopartitionedTopicsEnforcer();
        rebalanceProtocol = assignorConfiguration.rebalanceProtocol();
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public List<RebalanceProtocol> supportedProtocols() {
        final List<RebalanceProtocol> supportedProtocols = new ArrayList<>();
        supportedProtocols.add(RebalanceProtocol.EAGER);
        if (rebalanceProtocol == RebalanceProtocol.COOPERATIVE) {
            supportedProtocols.add(rebalanceProtocol);
        }
        return supportedProtocols;
    }

    @Override
    public ByteBuffer subscriptionUserData(final Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.
        final Set<TaskId> standbyTasks = taskManager.cachedTasksIds();
        final Set<TaskId> activeTasks = prepareForSubscription(taskManager,
            topics,
            standbyTasks,
            rebalanceProtocol);
        return new SubscriptionInfo(
            usedSubscriptionMetadataVersion,
            LATEST_SUPPORTED_VERSION,
            taskManager.processId(),
            activeTasks,
            standbyTasks,
            userEndPoint)
            .encode();
    }

    protected static Set<TaskId> prepareForSubscription(final TaskManager taskManager,
        final Set<String> topics,
        final Set<TaskId> standbyTasks,
        final RebalanceProtocol rebalanceProtocol) {
        // Any tasks that are not yet running are counted as standby tasks for assignment purposes,
        // along with any old tasks for which we still found state on disk
        final Set<TaskId> activeTasks;

        switch (rebalanceProtocol) {
            case EAGER:
                // In eager, onPartitionsRevoked is called first and we must get the previously saved running task ids
                activeTasks = taskManager.previousRunningTaskIds();
                standbyTasks.removeAll(activeTasks);
                break;
            case COOPERATIVE:
                // In cooperative, we will use the encoded ownedPartitions to determine the running tasks
                activeTasks = Collections.emptySet();
                standbyTasks.removeAll(taskManager.activeTaskIds());
                break;
            default:
                throw new IllegalStateException("Streams partition assignor's rebalance protocol is unknown");
        }

        taskManager.updateSubscriptionsFromMetadata(topics);
        taskManager.setRebalanceInProgress(true);

        return activeTasks;
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
        final Set<TopicPartition> allOwnedPartitions = new HashSet<>();

        // keep track of any future consumers in a "dummy" Client since we can't decipher their subscription
        final UUID futureId = randomUUID();
        final ClientMetadata futureClient = new ClientMetadata(null);
        clientMetadataMap.put(futureId, futureClient);

        int minReceivedMetadataVersion = LATEST_SUPPORTED_VERSION;
        int minSupportedMetadataVersion = LATEST_SUPPORTED_VERSION;

        int futureMetadataVersion = UNKNOWN;
        for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            final String consumerId = entry.getKey();
            final Subscription subscription = entry.getValue();
            final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            final int usedVersion = info.version();

            minReceivedMetadataVersion = updateMinReceivedVersion(usedVersion, minReceivedMetadataVersion);
            minSupportedMetadataVersion = updateMinSupportedVersion(info.latestSupportedVersion(), minSupportedMetadataVersion);

            final UUID processId;
            if (usedVersion > LATEST_SUPPORTED_VERSION) {
                futureMetadataVersion = usedVersion;
                processId = futureId;
            } else {
                processId = info.processId();
            }

            ClientMetadata clientMetadata = clientMetadataMap.get(processId);

            // create the new client metadata if necessary
            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint());
                clientMetadataMap.put(info.processId(), clientMetadata);
            }

            // add the consumer and any info its its subscription to the client
            clientMetadata.addConsumer(consumerId, subscription.ownedPartitions());
            allOwnedPartitions.addAll(subscription.ownedPartitions());
            clientMetadata.addPreviousTasks(info);
        }

        final boolean versionProbing;
        if (futureMetadataVersion == UNKNOWN) {
            versionProbing = false;
            clientMetadataMap.remove(futureId);
        } else if (minReceivedMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {
            versionProbing = true;
            log.info("Received a future (version probing) subscription (version: {})."
                    + " Sending assignment back (with supported version {}).",
                futureMetadataVersion,
                minSupportedMetadataVersion);

        } else {
            throw new IllegalStateException(
                "Received a future (version probing) subscription (version: " + futureMetadataVersion
                    + ") and an incompatible pre Kafka 2.0 subscription (version: " + minReceivedMetadataVersion
                    + ") at the same time."
            );
        }

        if (minReceivedMetadataVersion < LATEST_SUPPORTED_VERSION) {
            log.info("Downgrade metadata to version {}. Latest supported version is {}.",
                minReceivedMetadataVersion,
                LATEST_SUPPORTED_VERSION);
        }
        if (minSupportedMetadataVersion < LATEST_SUPPORTED_VERSION) {
            log.info("Downgrade latest supported metadata to version {}. Latest supported version is {}.",
                minSupportedMetadataVersion,
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
                        errorAssignment(clientMetadataMap, topic,
                            AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
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
                    final Optional<Integer> maybeNumPartitions = repartitionTopicMetadata.get(topicName)
                        .numberOfPartitions();
                    Integer numPartitions = null;

                    if (!maybeNumPartitions.isPresent()) {
                        // try set the number of partitions for this repartition topic if it is not set yet
                        for (final InternalTopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            final Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (final String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    Integer numPartitionsCandidate = null;
                                    // It is possible the sourceTopic is another internal topic, i.e,
                                    // map().join().join(map())
                                    if (repartitionTopicMetadata.containsKey(sourceTopicName)) {
                                        if (repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().isPresent()) {
                                            numPartitionsCandidate =
                                                repartitionTopicMetadata.get(sourceTopicName).numberOfPartitions().get();
                                        }
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

                                    if (numPartitionsCandidate != null) {
                                        if (numPartitions == null || numPartitionsCandidate > numPartitions) {
                                            numPartitions = numPartitionsCandidate;
                                        }
                                    }
                                }
                            }
                        }

                        // if we still have not found the right number of partitions,
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

        final Map<TopicPartition, TaskId> taskForPartition = new HashMap<>();

        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        final Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        final Map<Integer, Set<TaskId>> tasksByTopicGroup = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            final TaskId id = entry.getKey();
            final Set<TopicPartition> partitions = entry.getValue();

            for (final TopicPartition partition : partitions) {
                taskForPartition.put(partition, id);
                if (allAssignedPartitions.contains(partition)) {
                    log.warn("Partition {} is assigned to more than one tasks: {}", partition, partitionsForTask);
                }
            }
            allAssignedPartitions.addAll(partitions);

            tasksByTopicGroup.computeIfAbsent(id.topicGroupId, k -> new HashSet<>()).add(id);
        }
        for (final String topic : allSourceTopics) {
            final List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
            if (partitionInfoList.isEmpty()) {
                log.warn("No partitions found for topic {}", topic);
            } else {
                for (final PartitionInfo partitionInfo : partitionInfoList) {
                    final TopicPartition partition = new TopicPartition(partitionInfo.topic(),
                        partitionInfo.partition());
                    if (!allAssignedPartitions.contains(partition)) {
                        log.warn("Partition {} is not assigned to any tasks: {}"
                                + " Possible causes of a partition not getting assigned"
                                + " is that another topic defined in the topology has not been"
                                + " created when starting your streams application,"
                                + " resulting in no tasks created for this topology at all.", partition,
                            partitionsForTask);
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

        final Map<UUID, ClientState> states = new HashMap<>();
        for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
            final ClientState state = entry.getValue().state;
            states.put(entry.getKey(), state);

            // Either the active tasks (eager) OR the owned partitions (cooperative) were encoded in the subscription
            // according to the rebalancing protocol, so convert any partitions in a client to tasks where necessary
            if (!state.ownedPartitions().isEmpty()) {
                final Set<TaskId> previousActiveTasks = new HashSet<>();
                for (final Map.Entry<TopicPartition, String> partitionEntry : state.ownedPartitions().entrySet()) {
                    final TopicPartition tp = partitionEntry.getKey();
                    final TaskId task = taskForPartition.get(tp);
                    if (task != null) {
                        previousActiveTasks.add(task);
                    } else {
                        log.error("No task found for topic partition {}", tp);
                    }
                }
                state.addPreviousActiveTasks(previousActiveTasks);
            }
        }

        log.debug("Assigning tasks {} to clients {} with number of replicas {}",
            partitionsForTask.keySet(), states, numStandbyReplicas);

        // assign tasks to clients
        final StickyTaskAssignor<UUID> taskAssignor = new StickyTaskAssignor<>(states, partitionsForTask.keySet());
        taskAssignor.assign(numStandbyReplicas);

        log.info("Assigned tasks to clients as {}{}.", Utils.NL, states.entrySet().stream()
            .map(Map.Entry::toString).collect(Collectors.joining(Utils.NL)));

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost = new HashMap<>();
        if (minReceivedMetadataVersion >= 2) {
            for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
                final HostInfo hostInfo = entry.getValue().hostInfo;

                // if application server is configured, also include host state map
                if (hostInfo != null) {
                    final Set<TopicPartition> topicPartitions = new HashSet<>();
                    final Set<TopicPartition> standbyPartitions = new HashSet<>();
                    final ClientState state = entry.getValue().state;

                    for (final TaskId id : state.activeTasks()) {
                        topicPartitions.addAll(partitionsForTask.get(id));
                    }

                    for (final TaskId id : state.standbyTasks()) {
                        standbyPartitions.addAll(partitionsForTask.get(id));
                    }

                    partitionsByHost.put(hostInfo, topicPartitions);
                    standbyPartitionsByHost.put(hostInfo, standbyPartitions);
                }
            }
        }
        taskManager.setHostPartitionMappings(partitionsByHost, standbyPartitionsByHost);

        final Map<String, Assignment> assignment;
        if (versionProbing) {
            assignment = versionProbingAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHost,
                standbyPartitionsByHost,
                allOwnedPartitions,
                minReceivedMetadataVersion,
                minSupportedMetadataVersion
            );
        } else {
            assignment = computeNewAssignment(
                clientMetadataMap,
                partitionsForTask,
                partitionsByHost,
                standbyPartitionsByHost,
                allOwnedPartitions,
                minReceivedMetadataVersion,
                minSupportedMetadataVersion
            );
        }

        return new GroupAssignment(assignment);
    }

    private Map<String, Assignment> computeNewAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                         final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                         final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                                         final Set<TopicPartition> allOwnedPartitions,
                                                         final int minUserMetadataVersion,
                                                         final int minSupportedMetadataVersion) {
        // keep track of whether a 2nd rebalance is unavoidable so we can skip trying to get a completely sticky assignment
        boolean rebalanceRequired = false;
        final Map<String, Assignment> assignment = new HashMap<>();

        // within the client, distribute tasks to its owned consumers
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            final ClientState state = clientMetadata.state;
            final Set<String> consumers = clientMetadata.consumers;
            Map<String, List<TaskId>> activeTaskAssignments;

            // Try to avoid triggering another rebalance by giving active tasks back to their previous owners within a
            // client, without violating load balance. If we already know another rebalance will be required, or the
            // client had no owned partitions, try to balance the workload as evenly as possible by interleaving the
            // tasks among consumers and hopefully spreading the heavier subtopologies evenly across threads.
            if (rebalanceRequired || state.ownedPartitions().isEmpty()) {
                activeTaskAssignments = interleaveConsumerTasksByGroupId(state.activeTasks(), consumers);
            } else if ((activeTaskAssignments = tryStickyAndBalancedTaskAssignmentWithinClient(state, consumers, partitionsForTask, allOwnedPartitions))
                        .equals(Collections.emptyMap())) {
                rebalanceRequired = true;
                activeTaskAssignments = interleaveConsumerTasksByGroupId(state.activeTasks(), consumers);
            }

            final Map<String, List<TaskId>> interleavedStandby =
                interleaveConsumerTasksByGroupId(state.standbyTasks(), consumers);

            addClientAssignments(
                assignment,
                clientMetadata,
                partitionsForTask,
                partitionsByHostState,
                standbyPartitionsByHost,
                allOwnedPartitions,
                activeTaskAssignments,
                interleavedStandby,
                minUserMetadataVersion,
                minSupportedMetadataVersion);
        }

        return assignment;
    }

    private Map<String, Assignment> versionProbingAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
                                                             final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                             final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                                                             final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                                             final Set<TopicPartition> allOwnedPartitions,
                                                             final int minUserMetadataVersion,
                                                             final int minSupportedMetadataVersion) {
        final Map<String, Assignment> assignment = new HashMap<>();

        // Since we know another rebalance will be triggered anyway, just try and generate a balanced assignment
        // (without violating cooperative protocol) now so that on the second rebalance we can just give tasks
        // back to their previous owners
        // within the client, distribute tasks to its owned consumers
        for (final ClientMetadata clientMetadata : clientsMetadata.values()) {
            final ClientState state = clientMetadata.state;

            final Map<String, List<TaskId>> interleavedActive =
                interleaveConsumerTasksByGroupId(state.activeTasks(), clientMetadata.consumers);
            final Map<String, List<TaskId>> interleavedStandby =
                interleaveConsumerTasksByGroupId(state.standbyTasks(), clientMetadata.consumers);

            addClientAssignments(
                assignment,
                clientMetadata,
                partitionsForTask,
                partitionsByHost,
                standbyPartitionsByHost,
                allOwnedPartitions,
                interleavedActive,
                interleavedStandby,
                minUserMetadataVersion,
                minSupportedMetadataVersion);
        }

        return assignment;
    }

    private void addClientAssignments(final Map<String, Assignment> assignment,
                                      final ClientMetadata clientMetadata,
                                      final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                      final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                      final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                      final Set<TopicPartition> allOwnedPartitions,
                                      final Map<String, List<TaskId>> activeTaskAssignments,
                                      final Map<String, List<TaskId>> standbyTaskAssignments,
                                      final int minUserMetadataVersion,
                                      final int minSupportedMetadataVersion) {

        // Loop through the consumers and build their assignment
        for (final String consumer : clientMetadata.consumers) {
            final List<TaskId> activeTasksForConsumer = activeTaskAssignments.get(consumer);

            // These will be filled in by buildAssignedActiveTaskAndPartitionsList below
            final List<TopicPartition> activePartitionsList = new ArrayList<>();
            final List<TaskId> assignedActiveList = new ArrayList<>();

            buildAssignedActiveTaskAndPartitionsList(consumer,
                                                     clientMetadata.state,
                                                     activeTasksForConsumer,
                                                     partitionsForTask,
                                                     allOwnedPartitions,
                                                     activePartitionsList,
                                                     assignedActiveList);

            final Map<TaskId, Set<TopicPartition>> standbyTaskMap =
                buildStandbyTaskMap(standbyTaskAssignments.get(consumer), partitionsForTask);

            // finally, encode the assignment and insert into map with all assignments
            assignment.put(
                consumer,
                new Assignment(
                    activePartitionsList,
                    new AssignmentInfo(
                        minUserMetadataVersion,
                        minSupportedMetadataVersion,
                        assignedActiveList,
                        standbyTaskMap,
                        partitionsByHostState,
                        standbyPartitionsByHost,
                        AssignorError.NONE.code()
                    ).encode()
                )
            );
        }
    }

    private void buildAssignedActiveTaskAndPartitionsList(final String consumer,
                                                          final ClientState clientState,
                                                          final List<TaskId> activeTasksForConsumer,
                                                          final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                          final Set<TopicPartition> allOwnedPartitions,
                                                          final List<TopicPartition> activePartitionsList,
                                                          final List<TaskId> assignedActiveList) {
        final List<AssignedPartition> assignedPartitions = new ArrayList<>();

        // Build up list of all assigned partition-task pairs
        for (final TaskId taskId : activeTasksForConsumer) {
            final List<AssignedPartition> assignedPartitionsForTask = new ArrayList<>();
            for (final TopicPartition partition : partitionsForTask.get(taskId)) {
                final String oldOwner = clientState.ownedPartitions().get(partition);
                final boolean newPartitionForConsumer = oldOwner == null || !oldOwner.equals(consumer);

                // If the partition is new to this consumer but is still owned by another, remove from the assignment
                // until it has been revoked and can safely be reassigned according the COOPERATIVE protocol
                if (newPartitionForConsumer && allOwnedPartitions.contains(partition)) {
                    log.debug("Removing task {} from assignment until it is safely revoked", taskId);
                    clientState.removeFromAssignment(taskId);
                    // Clear the assigned partitions list for this task if any partition can not safely be assigned,
                    // so as not to encode a partial task
                    assignedPartitionsForTask.clear();
                    break;
                } else {
                    assignedPartitionsForTask.add(new AssignedPartition(taskId, partition));
                }
            }
            // assignedPartitionsForTask will either contain all partitions for the task or be empty, so just add all
            assignedPartitions.addAll(assignedPartitionsForTask);
        }

        // Add one copy of a task for each corresponding partition, so the receiver can determine the task <-> tp mapping
        Collections.sort(assignedPartitions);
        for (final AssignedPartition partition : assignedPartitions) {
            assignedActiveList.add(partition.taskId);
            activePartitionsList.add(partition.partition);
        }
    }

    private static Map<TaskId, Set<TopicPartition>> buildStandbyTaskMap(final Collection<TaskId> standbys,
                                                                        final Map<TaskId, Set<TopicPartition>> partitionsForTask) {
        final Map<TaskId, Set<TopicPartition>> standbyTaskMap = new HashMap<>();
        for (final TaskId task : standbys) {
            standbyTaskMap.put(task, partitionsForTask.get(task));
        }
        return standbyTaskMap;
    }

    /**
     * Generates an assignment that tries to satisfy two conditions: no active task previously owned by a consumer
     * be assigned to another (ie nothing gets revoked), and the number of tasks is evenly distributed throughout
     * the client.
     * <p>
     * If it is impossible to satisfy both constraints we abort early and return an empty map so we can use a
     * different assignment strategy that tries to distribute tasks of a single subtopology across different threads.
     *
     * @param state state for this client
     * @param consumers the consumers in this client
     * @param partitionsForTask mapping from task to its associated partitions
     * @param allOwnedPartitions set of all partitions claimed as owned by the group
     * @return task assignment for the consumers of this client
     *         empty map if it is not possible to generate a balanced assignment without moving a task to a new consumer
     */
    Map<String, List<TaskId>> tryStickyAndBalancedTaskAssignmentWithinClient(final ClientState state,
                                                                             final Set<String> consumers,
                                                                             final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                                             final Set<TopicPartition> allOwnedPartitions) {
        final Map<String, List<TaskId>> assignments = new HashMap<>();
        final LinkedList<TaskId> newTasks = new LinkedList<>();
        final Set<String> unfilledConsumers = new HashSet<>(consumers);

        final int maxTasksPerClient = (int) Math.ceil(((double) state.activeTaskCount()) / consumers.size());

        // initialize task list for consumers
        for (final String consumer : consumers) {
            assignments.put(consumer, new ArrayList<>());
        }

        for (final TaskId task : state.activeTasks()) {
            final Set<String> previousConsumers = previousConsumersOfTaskPartitions(partitionsForTask.get(task), state.ownedPartitions(), allOwnedPartitions);

            // If this task's partitions were owned by different consumers, we can't avoid revoking partitions
            if (previousConsumers.size() > 1) {
                log.warn("The partitions of task {} were claimed as owned by different StreamThreads. " +
                    "This indicates the mapping from partitions to tasks has changed!", task);
                return Collections.emptyMap();
            }

            // If this is a new task, or its old consumer no longer exists, it can be freely (re)assigned
            if (previousConsumers.isEmpty()) {
                log.debug("Task {} was not previously owned by any consumers still in the group. It's owner may " +
                    "have died or it may be a new task", task);
                newTasks.add(task);
            } else {
                final String consumer = previousConsumers.iterator().next();

                // If the previous consumer was from another client, these partitions will have to be revoked
                if (!consumers.contains(consumer)) {
                    log.debug("This client was assigned a task {} whose partition(s) were previously owned by another " +
                        "client, falling back to an interleaved assignment since a rebalance is inevitable.", task);
                    return Collections.emptyMap();
                }

                // If this consumer previously owned more tasks than it has capacity for, some must be revoked
                if (assignments.get(consumer).size() >= maxTasksPerClient) {
                    log.debug("Cannot create a sticky and balanced assignment as this client's consumers owned more " +
                        "previous tasks than it has capacity for during this assignment, falling back to interleaved " +
                        "assignment since a realance is inevitable.");
                    return Collections.emptyMap();
                }

                assignments.get(consumer).add(task);

                // If we have now reached capacity, remove it from set of consumers who still need more tasks
                if (assignments.get(consumer).size() == maxTasksPerClient) {
                    unfilledConsumers.remove(consumer);
                }
            }
        }

        // Interleave any remaining tasks by groupId among the consumers with remaining capacity. For further
        // explanation, see the javadocs for #interleaveConsumerTasksByGroupId
        Collections.sort(newTasks);
        while (!newTasks.isEmpty()) {
            if (unfilledConsumers.isEmpty()) {
                throw new IllegalStateException("Some tasks could not be distributed");
            }

            final Iterator<String> consumerIt = unfilledConsumers.iterator();

            // Loop through the unfilled consumers and distribute tasks until newTasks is empty
            while (consumerIt.hasNext()) {
                final String consumer = consumerIt.next();
                final List<TaskId> consumerAssignment = assignments.get(consumer);
                final TaskId task = newTasks.poll();
                if (task == null) {
                    break;
                }

                consumerAssignment.add(task);
                if (consumerAssignment.size() == maxTasksPerClient) {
                    consumerIt.remove();
                }
            }
        }

        return assignments;
    }

    /**
     * Get the previous consumer for the partitions of a task
     *
     * @param taskPartitions the TopicPartitions for a single given task
     * @param clientOwnedPartitions the partitions owned by all consumers in a client
     * @param allOwnedPartitions all partitions claimed as owned by any consumer in any client
     * @return set of consumer(s) that previously owned the partitions in this task
     *         empty set signals that it is a new task, or its previous owner is no longer in the group
     */
    Set<String> previousConsumersOfTaskPartitions(final Set<TopicPartition> taskPartitions,
                                                  final Map<TopicPartition, String> clientOwnedPartitions,
                                                  final Set<TopicPartition> allOwnedPartitions) {
        // this "foreignConsumer" indicates a partition was owned by someone from another client -- we don't really care who
        final String foreignConsumer = "";
        final Set<String> previousConsumers = new HashSet<>();

        for (final TopicPartition tp : taskPartitions) {
            final String currentPartitionConsumer = clientOwnedPartitions.get(tp);
            if (currentPartitionConsumer != null) {
                previousConsumers.add(currentPartitionConsumer);
            } else if (allOwnedPartitions.contains(tp)) {
                previousConsumers.add(foreignConsumer);
            }
        }

        return previousConsumers;
    }

    /**
     * Generate an assignment that attempts to maximize load balance without regard for stickiness, by spreading
     * tasks of the same groupId (subtopology) over different consumers.
     *
     * @param taskIds the set of tasks to be distributed
     * @param consumers the set of consumers to receive tasks
     * @return a map of task assignments keyed by the consumer id
     */
    static Map<String, List<TaskId>> interleaveConsumerTasksByGroupId(final Collection<TaskId> taskIds,
                                                                      final Set<String> consumers) {
        // First we make a sorted list of the tasks, grouping them by groupId
        final LinkedList<TaskId> sortedTasks = new LinkedList<>(taskIds);
        Collections.sort(sortedTasks);

        // Initialize the assignment map and task list for each consumer. We use a TreeMap here for a consistent
        // ordering of the consumers in the hope they will end up with the same set of tasks in subsequent assignments
        final Map<String, List<TaskId>> taskIdsForConsumerAssignment = new TreeMap<>();
        for (final String consumer : consumers) {
            taskIdsForConsumerAssignment.put(consumer, new ArrayList<>());
        }

        // We loop until the tasks have all been assigned, removing them from the list when they are given to a
        // consumer. To interleave the tasks, we loop through the consumers and give each one task from the head
        // of the list. When we finish going through the list of consumers we start over at the beginning of the
        // consumers list, continuing until we run out of tasks.
        while (!sortedTasks.isEmpty()) {
            for (final Map.Entry<String, List<TaskId>> consumerTaskIds : taskIdsForConsumerAssignment.entrySet()) {
                final List<TaskId> taskIdList = consumerTaskIds.getValue();
                final TaskId taskId = sortedTasks.poll();

                // Check for null here as we may run out of tasks before giving every consumer exactly the same number
                if (taskId == null) {
                    break;
                }
                taskIdList.add(taskId);
            }
        }
        return taskIdsForConsumerAssignment;
    }

    private void validateMetadataVersions(final int receivedAssignmentMetadataVersion,
                                          final int latestCommonlySupportedVersion) {

        if (receivedAssignmentMetadataVersion > usedSubscriptionMetadataVersion) {
            log.error("Leader sent back an assignment with version {} which was greater than our used version {}",
                receivedAssignmentMetadataVersion, usedSubscriptionMetadataVersion);
            throw new TaskAssignmentException(
                "Sent a version " + usedSubscriptionMetadataVersion
                    + " subscription but got an assignment with higher version "
                    + receivedAssignmentMetadataVersion + "."
            );
        }

        if (latestCommonlySupportedVersion > LATEST_SUPPORTED_VERSION) {
            log.error("Leader sent back assignment with commonly supported version {} that is greater than our "
                + "actual latest supported version {}", latestCommonlySupportedVersion, LATEST_SUPPORTED_VERSION);
            throw new TaskAssignmentException("Can't upgrade to metadata version greater than we support");
        }
    }

    // Returns true if subscription version was changed, indicating version probing and need to rebalance again
    protected boolean maybeUpdateSubscriptionVersion(final int receivedAssignmentMetadataVersion,
                                                     final int latestCommonlySupportedVersion) {
        if (receivedAssignmentMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {
            // If the latest commonly supported version is now greater than our used version, this indicates we have just
            // completed the rolling upgrade and can now update our subscription version for the final rebalance
            if (latestCommonlySupportedVersion > usedSubscriptionMetadataVersion) {
                log.info(
                    "Sent a version {} subscription and group's latest commonly supported version is {} (successful "
                        +
                        "version probing and end of rolling upgrade). Upgrading subscription metadata version to " +
                        "{} for next rebalance.",
                    usedSubscriptionMetadataVersion,
                    latestCommonlySupportedVersion,
                    latestCommonlySupportedVersion
                );
                usedSubscriptionMetadataVersion = latestCommonlySupportedVersion;
                return true;
            }

            // If we received a lower version than we sent, someone else in the group still hasn't upgraded. We
            // should downgrade our subscription until everyone is on the latest version
            if (receivedAssignmentMetadataVersion < usedSubscriptionMetadataVersion) {
                log.info(
                    "Sent a version {} subscription and got version {} assignment back (successful version probing). "
                        +
                        "Downgrade subscription metadata to commonly supported version {} and trigger new rebalance.",
                    usedSubscriptionMetadataVersion,
                    receivedAssignmentMetadataVersion,
                    latestCommonlySupportedVersion
                );
                usedSubscriptionMetadataVersion = latestCommonlySupportedVersion;
                return true;
            }
        } else {
            log.debug("Received an assignment version {} that is less than the earliest version that allows version " +
                "probing {}. If this is not during a rolling upgrade from version 2.0 or below, this is an error.",
                receivedAssignmentMetadataVersion, EARLIEST_PROBEABLE_VERSION);
        }

        return false;
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
            setAssignmentErrorCode(info.errCode());
            return;
        }
        /*
         * latestCommonlySupportedVersion belongs to [usedSubscriptionMetadataVersion, LATEST_SUPPORTED_VERSION]
         * receivedAssignmentMetadataVersion belongs to [EARLIEST_PROBEABLE_VERSION, usedSubscriptionMetadataVersion]
         *
         * usedSubscriptionMetadataVersion will be downgraded to receivedAssignmentMetadataVersion during a rolling
         * bounce upgrade with version probing.
         *
         * usedSubscriptionMetadataVersion will be upgraded to latestCommonlySupportedVersion when all members have
         * been bounced and it is safe to use the latest version.
         */
        final int receivedAssignmentMetadataVersion = info.version();
        final int latestCommonlySupportedVersion = info.commonlySupportedVersion();

        validateMetadataVersions(receivedAssignmentMetadataVersion, latestCommonlySupportedVersion);

        // Check if this was a version probing rebalance and check the error code to trigger another rebalance if so
        if (maybeUpdateSubscriptionVersion(receivedAssignmentMetadataVersion, latestCommonlySupportedVersion)) {
            setAssignmentErrorCode(AssignorError.VERSION_PROBING.code());
        }

        // version 1 field
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        // version 2 fields
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost;
        final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost;

        final Map<TopicPartition, TaskId> partitionsToTaskId = new HashMap<>();

        switch (receivedAssignmentMetadataVersion) {
            case 1:
                processVersionOneAssignment(logPrefix, info, partitions, activeTasks, partitionsToTaskId);
                partitionsByHost = Collections.emptyMap();
                standbyPartitionsByHost = Collections.emptyMap();
                break;
            case 2:
            case 3:
            case 4:
            case 5:
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo, partitionsToTaskId);
                partitionsByHost = info.partitionsByHost();
                standbyPartitionsByHost = Collections.emptyMap();
                break;
            case 6:
                processVersionTwoAssignment(logPrefix, info, partitions, activeTasks, topicToPartitionInfo, partitionsToTaskId);
                partitionsByHost = info.partitionsByHost();
                standbyPartitionsByHost = info.standbyPartitionByHost();
                break;
            default:
                throw new IllegalStateException(
                    "This code should never be reached."
                        + " Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/"
                );
        }

        taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
        taskManager.setHostPartitionMappings(partitionsByHost, standbyPartitionsByHost);
        taskManager.setPartitionsToTaskId(partitionsToTaskId);
        taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
        taskManager.updateSubscriptionsFromAssignment(partitions);
        taskManager.setRebalanceInProgress(false);
    }

    private static void processVersionOneAssignment(final String logPrefix,
                                                    final AssignmentInfo info,
                                                    final List<TopicPartition> partitions,
                                                    final Map<TaskId, Set<TopicPartition>> activeTasks,
                                                    final Map<TopicPartition, TaskId> partitionsToTaskId) {
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
            partitionsToTaskId.put(partition, id);
        }
    }

    public static void processVersionTwoAssignment(final String logPrefix,
                                                   final AssignmentInfo info,
                                                   final List<TopicPartition> partitions,
                                                   final Map<TaskId, Set<TopicPartition>> activeTasks,
                                                   final Map<TopicPartition, PartitionInfo> topicToPartitionInfo,
                                                   final Map<TopicPartition, TaskId> partitionsToTaskId) {
        processVersionOneAssignment(logPrefix, info, partitions, activeTasks, partitionsToTaskId);

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

    private int updateMinReceivedVersion(final int usedVersion, final int minReceivedMetadataVersion) {
        return usedVersion < minReceivedMetadataVersion ? usedVersion : minReceivedMetadataVersion;
    }

    private int updateMinSupportedVersion(final int supportedVersion, final int minSupportedMetadataVersion) {
        if (supportedVersion < minSupportedMetadataVersion) {
            log.debug("Downgrade the current minimum supported version {} to the smaller seen supported version {}",
                minSupportedMetadataVersion, supportedVersion);
            return supportedVersion;
        } else {
            log.debug("Current minimum supported version remains at {}, last seen supported version was {}",
                minSupportedMetadataVersion, supportedVersion);
            return minSupportedMetadataVersion;
        }
    }

    protected void setAssignmentErrorCode(final Integer errorCode) {
        assignmentErrorCode.set(errorCode);
    }

    // following functions are for test only
    void setRebalanceProtocol(final RebalanceProtocol rebalanceProtocol) {
        this.rebalanceProtocol = rebalanceProtocol;
    }

    void setInternalTopicManager(final InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

}
