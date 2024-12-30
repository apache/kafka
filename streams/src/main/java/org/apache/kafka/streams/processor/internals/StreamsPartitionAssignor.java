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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.MissingSourceTopicException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignmentUtils;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.AssignmentError;
import org.apache.kafka.streams.processor.assignment.TaskAssignor.TaskAssignment;
import org.apache.kafka.streams.processor.assignment.TaskInfo;
import org.apache.kafka.streams.processor.assignment.TaskTopicPartition;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.apache.kafka.streams.processor.internals.assignment.DefaultApplicationState;
import org.apache.kafka.streams.processor.internals.assignment.DefaultTaskInfo;
import org.apache.kafka.streams.processor.internals.assignment.DefaultTaskTopicPartition;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.LegacyStickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.LegacyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.RackAwareTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.RackUtils;
import org.apache.kafka.streams.processor.internals.assignment.ReferenceContainer;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.state.HostInfo;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static java.util.Map.Entry.comparingByKey;
import static org.apache.kafka.common.utils.Utils.filterMap;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchCommittedOffsets;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchEndOffsetsResult;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.UNKNOWN;
import static org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo.UNKNOWN_OFFSET_SUM;

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

    public static class ClientMetadata {

        private final HostInfo hostInfo;
        private final ClientState state;
        private final SortedSet<String> consumers;
        private final Optional<String> rackId;

        ClientMetadata(final ProcessId processId, final String endPoint, final Map<String, String> clientTags, final Optional<String> rackId) {

            // get the host info, or null if no endpoint is configured (ie endPoint == null)
            hostInfo = HostInfo.buildFromEndpoint(endPoint);

            // initialize the consumer memberIds
            consumers = new TreeSet<>();

            // initialize the client state with client tags
            state = new ClientState(processId, clientTags);

            this.rackId = rackId;
        }

        void addConsumer(final String consumerMemberId, final List<TopicPartition> ownedPartitions) {
            consumers.add(consumerMemberId);
            state.incrementCapacity();
            state.addOwnedPartitions(ownedPartitions, consumerMemberId);
        }

        void addPreviousTasksAndOffsetSums(final String consumerId, final Map<TaskId, Long> taskOffsetSums) {
            state.addPreviousTasksAndOffsetSums(consumerId, taskOffsetSums);
        }

        public ClientState state() {
            return state;
        }

        public HostInfo hostInfo() {
            return hostInfo;
        }

        public Optional<String> rackId() {
            return rackId;
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

    @FunctionalInterface
    public interface UserTaskAssignmentListener {
        void onAssignmentComputed(GroupAssignment assignment, GroupSubscription subscription);
    }

    // keep track of any future consumers in a "dummy" Client since we can't decipher their subscription
    private static final ProcessId FUTURE_ID = ProcessId.randomProcessId();

    protected static final Comparator<TopicPartition> PARTITION_COMPARATOR =
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private String userEndPoint;
    private AssignmentConfigs assignmentConfigs;

    // for the main consumer, we need to use a supplier to break a cyclic setup dependency
    private Supplier<Consumer<byte[], byte[]>> mainConsumerSupplier;
    private Admin adminClient;
    private TaskManager taskManager;
    private StreamsMetadataState streamsMetadataState;
    private PartitionGrouper partitionGrouper;
    private AtomicInteger assignmentErrorCode;
    private AtomicLong nextScheduledRebalanceMs;
    private Queue<StreamsException> nonFatalExceptionsToHandle;
    private Time time;

    protected int usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private RebalanceProtocol rebalanceProtocol;
    private AssignmentListener assignmentListener;

    private Supplier<Optional<org.apache.kafka.streams.processor.assignment.TaskAssignor>>
        customTaskAssignorSupplier;
    private Supplier<LegacyTaskAssignor> legacyTaskAssignorSupplier;
    private byte uniqueField;
    private Map<String, String> clientTags;

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible since the former needs
     * latter's cached metadata while sending subscriptions, and the latter needs former's returned assignment when
     * adding tasks.
     *
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final AssignorConfiguration assignorConfiguration = new AssignorConfiguration(configs);

        logPrefix = assignorConfiguration.logPrefix();
        log = new LogContext(logPrefix).logger(getClass());
        usedSubscriptionMetadataVersion = assignorConfiguration.configuredMetadataVersion(usedSubscriptionMetadataVersion);

        final ReferenceContainer referenceContainer = assignorConfiguration.referenceContainer();
        mainConsumerSupplier = () -> Objects.requireNonNull(referenceContainer.mainConsumer, "Main consumer was not specified");
        adminClient = Objects.requireNonNull(referenceContainer.adminClient, "Admin client was not specified");
        taskManager = Objects.requireNonNull(referenceContainer.taskManager, "TaskManager was not specified");
        streamsMetadataState = Objects.requireNonNull(referenceContainer.streamsMetadataState, "StreamsMetadataState was not specified");
        assignmentErrorCode = referenceContainer.assignmentErrorCode;
        nextScheduledRebalanceMs = referenceContainer.nextScheduledRebalanceMs;
        nonFatalExceptionsToHandle = referenceContainer.nonFatalExceptionsToHandle;
        time = Objects.requireNonNull(referenceContainer.time, "Time was not specified");
        assignmentConfigs = assignorConfiguration.assignmentConfigs();
        partitionGrouper = new PartitionGrouper();
        userEndPoint = assignorConfiguration.userEndPoint();
        internalTopicManager = assignorConfiguration.internalTopicManager();
        copartitionedTopicsEnforcer = assignorConfiguration.copartitionedTopicsEnforcer();
        rebalanceProtocol = assignorConfiguration.rebalanceProtocol();
        customTaskAssignorSupplier = assignorConfiguration::customTaskAssignor;
        legacyTaskAssignorSupplier = assignorConfiguration::taskAssignor;
        assignmentListener = assignorConfiguration.assignmentListener();
        uniqueField = 0;
        clientTags = referenceContainer.clientTags;
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
        // 1. Client ProcessId (a UUID assigned to an instance of KafkaStreams)
        // 2. Map from task id to its overall lag
        // 3. Unique Field to ensure a rebalance when a thread rejoins by forcing the user data to be different

        handleRebalanceStart(topics);
        uniqueField++;

        final Set<String> currentNamedTopologies = taskManager.topologyMetadata().namedTopologiesView();

        // If using NamedTopologies, filter out any that are no longer recognized/have been removed
        final Map<TaskId, Long> taskOffsetSums = taskManager.topologyMetadata().hasNamedTopologies() ?
            filterMap(taskManager.taskOffsetSums(), t -> currentNamedTopologies.contains(t.getKey().topologyName())) :
            taskManager.taskOffsetSums();

        return new SubscriptionInfo(
            usedSubscriptionMetadataVersion,
            LATEST_SUPPORTED_VERSION,
            taskManager.processId(),
            userEndPoint,
            taskOffsetSums,
            uniqueField,
            assignmentErrorCode.get(),
            clientTags
        ).encode();
    }

    private Map<String, Assignment> errorAssignment(final Map<ProcessId, ClientMetadata> clientsMetadata,
                                                    final int errorCode) {
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
     * 0. decode the subscriptions to assemble the metadata for each client and check for version probing
     *
     * 1. check all repartition source topics and use internal topic manager to make sure
     *    they have been created with the right number of partitions. Also verify and/or create
     *    any changelog topics with the correct number of partitions.
     *
     * 2. use the partition grouper to generate tasks along with their assigned partitions, then use
     *    the configured TaskAssignor to construct the mapping of tasks to clients.
     *
     * 3. construct the global mapping of host to partitions to enable query routing.
     *
     * 4. within each client, assign tasks to consumer clients.
     */
    @Override
    public GroupAssignment assign(final Cluster metadata, final GroupSubscription groupSubscription) {
        final Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();

        // ---------------- Step Zero ---------------- //

        // construct the client metadata from the decoded subscription info

        final Map<ProcessId, ClientMetadata> clientMetadataMap = new HashMap<>();
        final Set<TopicPartition> allOwnedPartitions = new HashSet<>();
        final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer = new HashMap<>();

        int minReceivedMetadataVersion = LATEST_SUPPORTED_VERSION;
        int minSupportedMetadataVersion = LATEST_SUPPORTED_VERSION;

        boolean shutdownRequested = false;
        boolean assignmentErrorFound = false;
        int futureMetadataVersion = UNKNOWN;
        for (final Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            final String consumerId = entry.getKey();
            final Subscription subscription = entry.getValue();
            final SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
            final int usedVersion = info.version();
            if (info.errorCode() == AssignorError.SHUTDOWN_REQUESTED.code()) {
                shutdownRequested = true;
            }

            minReceivedMetadataVersion = updateMinReceivedVersion(usedVersion, minReceivedMetadataVersion);
            minSupportedMetadataVersion = updateMinSupportedVersion(info.latestSupportedVersion(), minSupportedMetadataVersion);

            final ProcessId processId;
            if (usedVersion > LATEST_SUPPORTED_VERSION) {
                futureMetadataVersion = usedVersion;
                processId = FUTURE_ID;
                if (!clientMetadataMap.containsKey(FUTURE_ID)) {
                    clientMetadataMap.put(FUTURE_ID, new ClientMetadata(FUTURE_ID, null, Collections.emptyMap(), subscription.rackId()));
                }
            } else {
                processId = info.processId();
            }

            racksForProcessConsumer.computeIfAbsent(processId, kv -> new HashMap<>()).put(consumerId, subscription.rackId());

            ClientMetadata clientMetadata = clientMetadataMap.get(processId);

            // create the new client metadata if necessary
            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.processId(), info.userEndPoint(), info.clientTags(), subscription.rackId());
                clientMetadataMap.put(info.processId(), clientMetadata);
            }

            // add the consumer and any info in its subscription to the client
            clientMetadata.addConsumer(consumerId, subscription.ownedPartitions());
            final int prevSize = allOwnedPartitions.size();
            allOwnedPartitions.addAll(subscription.ownedPartitions());
            if (allOwnedPartitions.size() < prevSize + subscription.ownedPartitions().size()) {
                assignmentErrorFound = true;
            }
            clientMetadata.addPreviousTasksAndOffsetSums(consumerId, info.taskOffsetSums());
        }

        if (assignmentErrorFound) {
            log.warn("The previous assignment contains a partition more than once. " +
                "\t Mapping: {}", subscriptions);
        }

        try {
            log.debug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

            // ---------------- Step One ---------------- //

            if (shutdownRequested) {
                return new GroupAssignment(errorAssignment(clientMetadataMap, AssignorError.SHUTDOWN_REQUESTED.code()));
            }

            // parse the topology to determine the repartition source topics,
            // making sure they are created with the number of partitions as
            // the maximum of the depending sub-topologies source topics' number of partitions
            final RepartitionTopics repartitionTopics = prepareRepartitionTopics(metadata);
            final Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = repartitionTopics.topicPartitionsInfo();

            final Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);
            log.debug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.values());

            // ---------------- Step Two ---------------- //

            // construct the assignment of tasks to clients

            final Map<Subtopology, TopicsInfo> topicGroups =
                taskManager.topologyMetadata().subtopologyTopicsInfoMapExcluding(repartitionTopics.topologiesWithMissingInputTopics());

            final Set<String> allSourceTopics = new HashSet<>();
            final Map<Subtopology, Set<String>> sourceTopicsByGroup = new HashMap<>();
            for (final Map.Entry<Subtopology, TopicsInfo> entry : topicGroups.entrySet()) {
                allSourceTopics.addAll(entry.getValue().sourceTopics);
                sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
            }

            // get the tasks as partition groups from the partition grouper
            final Map<TaskId, Set<TopicPartition>> partitionsForTask =
                partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

            final Set<TaskId> statefulTasks = new HashSet<>();

            final boolean versionProbing =
                checkMetadataVersions(minReceivedMetadataVersion, minSupportedMetadataVersion, futureMetadataVersion);
            final UserTaskAssignmentListener userTaskAssignmentListener = assignTasksToClients(fullMetadata, groupSubscription,
                allSourceTopics, topicGroups, clientMetadataMap, partitionsForTask, racksForProcessConsumer, statefulTasks);

            // ---------------- Step Three ---------------- //

            // construct the global partition assignment per host map

            final Map<HostInfo, Set<TopicPartition>> partitionsByHost = new HashMap<>();
            final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost = new HashMap<>();
            if (minReceivedMetadataVersion >= 2) {
                populatePartitionsByHostMaps(partitionsByHost, standbyPartitionsByHost, partitionsForTask, clientMetadataMap);
            }

            // ---------------- Step Four ---------------- //

            // compute the assignment of tasks to threads within each client and build the final group assignment
            final Map<String, Assignment> assignment = computeNewAssignment(
                statefulTasks,
                clientMetadataMap,
                partitionsForTask,
                partitionsByHost,
                standbyPartitionsByHost,
                allOwnedPartitions,
                minReceivedMetadataVersion,
                minSupportedMetadataVersion,
                versionProbing
            );

            final GroupAssignment groupAssignment = new GroupAssignment(assignment);
            userTaskAssignmentListener.onAssignmentComputed(groupAssignment, groupSubscription);
            return groupAssignment;
        } catch (final MissingSourceTopicException e) {
            log.error("Caught an error in the task assignment. Returning an error assignment.", e);
            return new GroupAssignment(
                errorAssignment(clientMetadataMap, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
            );
        } catch (final TaskAssignmentException e) {
            log.error("Caught an error in the task assignment. Returning an error assignment.", e);
            return new GroupAssignment(
                errorAssignment(clientMetadataMap, AssignorError.ASSIGNMENT_ERROR.code())
            );
        }
    }

    /**
     *
     * @param clientMetadataMap the map of process id to client metadata used to build an immutable
     *                          {@code ApplicationState}
     * @return The {@code ApplicationState} needed by the TaskAssigner to compute new task
     *         assignments.
     */
    private ApplicationState buildApplicationState(final TopologyMetadata topologyMetadata,
                                                   final Map<ProcessId, ClientMetadata> clientMetadataMap,
                                                   final Map<Subtopology, TopicsInfo> topicGroups,
                                                   final Cluster cluster) {
        final Map<Subtopology, Set<String>> sourceTopicsByGroup = new HashMap<>();
        final Map<Subtopology, Set<String>> changelogTopicsByGroup = new HashMap<>();
        for (final Map.Entry<Subtopology, TopicsInfo> entry : topicGroups.entrySet()) {
            final Set<String> sourceTopics = entry.getValue().sourceTopics;
            final Set<String> changelogTopics = entry.getValue().changelogTopics();
            sourceTopicsByGroup.put(entry.getKey(), sourceTopics);
            changelogTopicsByGroup.put(entry.getKey(), changelogTopics);
        }

        final Map<TaskId, Set<TopicPartition>> changelogPartitionsForTask = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> sourcePartitionsForTask =
            partitionGrouper.partitionGroups(sourceTopicsByGroup, changelogTopicsByGroup,
                changelogPartitionsForTask, cluster);

        if (!sourcePartitionsForTask.keySet().equals(changelogPartitionsForTask.keySet())) {
            log.error("Partition grouper returned {} tasks for source topics but {} tasks for changelog topics",
                sourcePartitionsForTask.size(), changelogPartitionsForTask.size());
            throw new TaskAssignmentException("Partition grouper returned conflicting information about the "
                                              + "tasks for source topics vs changelog topics.");
        }

        final Set<DefaultTaskTopicPartition> topicsRequiringRackInfo = new HashSet<>();
        final AtomicBoolean rackInformationFetched = new AtomicBoolean(false);
        final Runnable fetchRackInformation = () -> {
            if (!rackInformationFetched.get()) {
                RackUtils.annotateTopicPartitionsWithRackInfo(cluster, internalTopicManager, topicsRequiringRackInfo);
                rackInformationFetched.set(true);
            }
        };

        final Map<TaskId, Set<TaskTopicPartition>> topicPartitionsForTask = new HashMap<>();
        final Set<TaskId> logicalTaskIds = unmodifiableSet(sourcePartitionsForTask.keySet());
        logicalTaskIds.forEach(taskId -> {
            final Set<TaskTopicPartition> topicPartitions = new HashSet<>();

            for (final TopicPartition topicPartition : sourcePartitionsForTask.get(taskId)) {
                final boolean isSource = true;
                final boolean isChangelog = changelogPartitionsForTask.get(taskId).contains(topicPartition);
                final DefaultTaskTopicPartition racklessTopicPartition = new DefaultTaskTopicPartition(
                    topicPartition, isSource, isChangelog, fetchRackInformation);
                topicsRequiringRackInfo.add(racklessTopicPartition);
                topicPartitions.add(racklessTopicPartition);
            }

            for (final TopicPartition topicPartition : changelogPartitionsForTask.get(taskId)) {
                final boolean isSource = sourcePartitionsForTask.get(taskId).contains(topicPartition);
                final boolean isChangelog = true;
                final DefaultTaskTopicPartition racklessTopicPartition = new DefaultTaskTopicPartition(
                    topicPartition, isSource, isChangelog, fetchRackInformation);
                topicsRequiringRackInfo.add(racklessTopicPartition);
                topicPartitions.add(racklessTopicPartition);
            }

            topicPartitionsForTask.put(taskId, topicPartitions);
        });

        final Map<TaskId, TaskInfo> logicalTasks = logicalTaskIds.stream().collect(Collectors.toMap(
            Function.identity(),
            taskId -> {
                final Set<String> stateStoreNames = topologyMetadata
                    .stateStoreNamesForSubtopology(taskId.topologyName(), taskId.subtopology());
                final Set<TaskTopicPartition> topicPartitions = topicPartitionsForTask.get(taskId);
                return new DefaultTaskInfo(
                    taskId,
                    !stateStoreNames.isEmpty(),
                    stateStoreNames,
                    topicPartitions
                );
            }
        ));

        return new DefaultApplicationState(
            assignmentConfigs,
            logicalTasks,
            clientMetadataMap
        );
    }

    private void processStreamsPartitionAssignment(final org.apache.kafka.streams.processor.assignment.TaskAssignor assignor,
                                                   final TaskAssignment taskAssignment,
                                                   final AssignmentError assignmentError,
                                                   final Map<ProcessId, ClientMetadata> clientMetadataMap,
                                                   final GroupSubscription groupSubscription) {
        if (assignmentError == AssignmentError.UNKNOWN_PROCESS_ID || assignmentError == AssignmentError.UNKNOWN_TASK_ID) {
            assignor.onAssignmentComputed(new GroupAssignment(Collections.emptyMap()), groupSubscription, assignmentError);
            log.error("Rebalance failed due to task assignor returning assignment with error {}, " +
                      "assignor callback will receive empty GroupAssignment due to this error", assignmentError);
            throw new StreamsException("Task assignment with " + assignor.getClass().getName() +
                                       " returned a fatal error: " + assignmentError);
        }

        taskAssignment.assignment().forEach(kafkaStreamsAssignment -> {
            final ProcessId processId = kafkaStreamsAssignment.processId();
            final ClientMetadata clientMetadata = clientMetadataMap.get(processId);
            clientMetadata.state.setAssignedTasks(kafkaStreamsAssignment);
            if (kafkaStreamsAssignment.followupRebalanceDeadline().isPresent()) {
                clientMetadata.state.setFollowupRebalanceDeadline(kafkaStreamsAssignment.followupRebalanceDeadline().get());
            }
        });
    }

    /**
     * Verify the subscription versions are within the expected bounds and check for version probing.
     *
     * @return whether this was a version probing rebalance
     */
    private boolean checkMetadataVersions(final int minReceivedMetadataVersion,
                                          final int minSupportedMetadataVersion,
                                          final int futureMetadataVersion) {
        final boolean versionProbing;

        if (futureMetadataVersion == UNKNOWN) {
            versionProbing = false;
        } else if (minReceivedMetadataVersion >= EARLIEST_PROBEABLE_VERSION) {
            versionProbing = true;
            log.info("Received a future (version probing) subscription (version: {})."
                         + " Sending assignment back (with supported version {}).",
                futureMetadataVersion,
                minSupportedMetadataVersion);

        } else {
            throw new TaskAssignmentException(
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
        return versionProbing;
    }

    /**
     * Computes and assembles all repartition topic metadata then creates the topics if necessary. Also verifies
     * that all user input topics of each topology have been created ahead of time. If any such source topics are
     * missing from a NamedTopology, the assignor will skip distributing its tasks until they have been created
     * and invoke the exception handler (without killing the thread) once for each topology to alert the user of
     * the missing topics.
     * <p>
     * For regular applications without named topologies, the assignor will instead send a shutdown signal to
     * all clients so the user can identify and resolve the problem.
     *
     * @return application metadata such as partition info of repartition topics, missing external topics, etc
     */
    private RepartitionTopics prepareRepartitionTopics(final Cluster metadata) {
        final RepartitionTopics repartitionTopics = new RepartitionTopics(
            taskManager.topologyMetadata(),
            internalTopicManager,
            copartitionedTopicsEnforcer,
            metadata,
            logPrefix
        );
        repartitionTopics.setup();
        final boolean isMissingInputTopics = !repartitionTopics.missingSourceTopicExceptions().isEmpty();
        if (isMissingInputTopics) {
            if (!taskManager.topologyMetadata().hasNamedTopologies()) {
                final String errorMsg = String.format("Missing source topics. %s", repartitionTopics.missingSourceTopics());
                log.error(errorMsg);
                throw new MissingSourceTopicException(errorMsg);
            } else {
                nonFatalExceptionsToHandle.addAll(repartitionTopics.missingSourceTopicExceptions());
            }
        }
        return repartitionTopics;
    }


    /**
     * Populates the taskForPartition and tasksForTopicGroup maps, and checks that partitions are assigned to exactly
     * one task.
     *
     * @param taskForPartition a map from partition to the corresponding task. Populated here.
     * @param tasksForTopicGroup a map from the topicGroupId to the set of corresponding tasks. Populated here.
     * @param allSourceTopics a set of all source topics in the topology
     * @param partitionsForTask a map from task to the set of input partitions
     * @param fullMetadata the cluster metadata
     */
    private void populateTasksForMaps(final Map<TopicPartition, TaskId> taskForPartition,
                                      final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                                      final Set<String> allSourceTopics,
                                      final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                      final Cluster fullMetadata) {
        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        final Set<TopicPartition> allAssignedPartitions = new HashSet<>();
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

            tasksForTopicGroup.computeIfAbsent(new Subtopology(id.subtopology(), id.topologyName()), k -> new HashSet<>()).add(id);
        }

        checkAllPartitions(allSourceTopics, partitionsForTask, allAssignedPartitions, fullMetadata);
    }

    // Logs a warning if any partitions are not assigned to a task, or a task has no assigned partitions
    private void checkAllPartitions(final Set<String> allSourceTopics,
                                    final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                    final Set<TopicPartition> allAssignedPartitions,
                                    final Cluster fullMetadata) {
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
    }

    /**
     * Assigns a set of tasks to each client (Streams instance) using the configured task assignor, and also
     * populate the stateful tasks that have been assigned to the clients
     */
    private UserTaskAssignmentListener assignTasksToClients(final Cluster fullMetadata,
                                                            final GroupSubscription groupSubscription,
                                                            final Set<String> allSourceTopics,
                                                            final Map<Subtopology, TopicsInfo> topicGroups,
                                                            final Map<ProcessId, ClientMetadata> clientMetadataMap,
                                                            final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                            final Map<ProcessId, Map<String, Optional<String>>> racksForProcessConsumer,
                                                            final Set<TaskId> statefulTasks) {
        if (!statefulTasks.isEmpty()) {
            throw new TaskAssignmentException("The stateful tasks should not be populated before assigning tasks to clients");
        }

        final Map<TopicPartition, TaskId> taskForPartition = new HashMap<>();
        final Map<Subtopology, Set<TaskId>> tasksForTopicGroup = new HashMap<>();
        populateTasksForMaps(taskForPartition, tasksForTopicGroup, allSourceTopics, partitionsForTask, fullMetadata);

        final ChangelogTopics changelogTopics = new ChangelogTopics(
            internalTopicManager,
            topicGroups,
            tasksForTopicGroup,
            logPrefix
        );
        changelogTopics.setup();

        final Map<ProcessId, ClientState> clientStates = new HashMap<>();
        final boolean lagComputationSuccessful =
            populateClientStatesMap(clientStates, clientMetadataMap, taskForPartition, changelogTopics);


        log.info("{} client nodes and {} consumers participating in this rebalance: \n{}.",
                 clientStates.size(),
                 clientStates.values().stream().map(ClientState::capacity).reduce(Integer::sum).orElse(0),
                 clientStates.entrySet().stream()
                     .sorted(comparingByKey())
                     .map(entry -> entry.getKey() + ": " + entry.getValue().consumers())
                     .collect(Collectors.joining(Utils.NL)));

        final Set<TaskId> allTasks = partitionsForTask.keySet();
        statefulTasks.addAll(changelogTopics.statefulTaskIds());

        log.info("Assigning stateful tasks: {}\n"
                     + "and stateless tasks: {}",
                 statefulTasks,
                 allTasks.stream().filter(t -> !statefulTasks.contains(t)).collect(Collectors.toSet()));
        log.debug("Assigning tasks and {} standby replicas to client nodes {}",
                  numStandbyReplicas(), clientStates);

        final Optional<org.apache.kafka.streams.processor.assignment.TaskAssignor> userTaskAssignor =
            customTaskAssignorSupplier.get();
        final UserTaskAssignmentListener customTaskAssignmentListener;
        if (userTaskAssignor.isPresent()) {
            final ApplicationState applicationState = buildApplicationState(
                taskManager.topologyMetadata(),
                clientMetadataMap,
                topicGroups,
                fullMetadata
            );
            final org.apache.kafka.streams.processor.assignment.TaskAssignor assignor = userTaskAssignor.get();
            final TaskAssignment taskAssignment = assignor.assign(applicationState);
            final AssignmentError assignmentError = TaskAssignmentUtils.validateTaskAssignment(applicationState, taskAssignment);
            processStreamsPartitionAssignment(assignor, taskAssignment, assignmentError, clientMetadataMap, groupSubscription);
            customTaskAssignmentListener = (assignment, subscription) -> {
                assignor.onAssignmentComputed(assignment, subscription, assignmentError);
                if (assignmentError != AssignmentError.NONE) {
                    log.error("Rebalance failed due to task assignor returning assignment with error {}", assignmentError);
                    throw new StreamsException("Task assignment with " + assignor.getClass().getName() +
                                               " returned an error: " + assignmentError);
                }
            };
        } else {
            customTaskAssignmentListener = (assignment, subscription) -> { };
            final LegacyTaskAssignor taskAssignor = createTaskAssignor(lagComputationSuccessful);
            final RackAwareTaskAssignor rackAwareTaskAssignor = new RackAwareTaskAssignor(
                fullMetadata,
                partitionsForTask,
                changelogTopics.changelogPartionsForTask(),
                tasksForTopicGroup,
                racksForProcessConsumer,
                internalTopicManager,
                assignmentConfigs,
                time
            );
            final boolean probingRebalanceNeeded = taskAssignor.assign(clientStates,
                allTasks,
                statefulTasks,
                rackAwareTaskAssignor,
                assignmentConfigs);
            if (probingRebalanceNeeded) {
                // Arbitrarily choose the leader's client to be responsible for triggering the probing rebalance,
                // note once we pick the first consumer within the process to trigger probing rebalance, other consumer
                // would not set to trigger any more.
                final ClientMetadata rebalanceClientMetadata = clientMetadataMap.get(taskManager.processId());
                if (rebalanceClientMetadata != null) {
                    final Instant rebalanceDeadline = Instant.ofEpochMilli(time.milliseconds() + probingRebalanceIntervalMs());
                    rebalanceClientMetadata.state.setFollowupRebalanceDeadline(rebalanceDeadline);
                }
            }
        }

        // Break this up into multiple logs to make sure the summary info gets through, which helps avoid
        // info loss for example due to long line truncation with large apps
        log.info("Assigned {} total tasks including {} stateful tasks to {} client nodes.",
                 allTasks.size(),
                 statefulTasks.size(),
                 clientStates.size());
        log.info("Assignment of tasks to nodes: {}",
                 clientStates.entrySet().stream()
                     .sorted(comparingByKey())
                     .map(entry -> entry.getKey() + "=" + entry.getValue().currentAssignment())
                     .collect(Collectors.joining(Utils.NL)));
        return customTaskAssignmentListener;
    }

    private LegacyTaskAssignor createTaskAssignor(final boolean lagComputationSuccessful) {
        final LegacyTaskAssignor taskAssignor = legacyTaskAssignorSupplier.get();
        if (taskAssignor instanceof LegacyStickyTaskAssignor) {
            // special case: to preserve pre-existing behavior, we invoke the LegacyStickyTaskAssignor
            // whether or not lag computation failed.
            return taskAssignor;
        } else if (lagComputationSuccessful) {
            return taskAssignor;
        } else {
            log.info("Failed to fetch end offsets for changelogs, will return previous assignment to clients and "
                         + "trigger another rebalance to retry.");
            return new FallbackPriorTaskAssignor();
        }
    }

    /**
     * Builds a map from client to state, and readies each ClientState for assignment by adding any missing prev tasks
     * and computing the per-task overall lag based on the fetched end offsets for each changelog.
     *
     * @param clientStates a map from each client to its state, including offset lags. Populated by this method.
     * @param clientMetadataMap a map from each client to its full metadata
     * @param taskForPartition map from topic partition to its corresponding task
     * @param changelogTopics object that manages changelog topics
     *
     * @return whether we were able to successfully fetch the changelog end offsets and compute each client's lag
     */
    private boolean populateClientStatesMap(final Map<ProcessId, ClientState> clientStates,
                                            final Map<ProcessId, ClientMetadata> clientMetadataMap,
                                            final Map<TopicPartition, TaskId> taskForPartition,
                                            final ChangelogTopics changelogTopics) {
        boolean fetchEndOffsetsSuccessful;
        Map<TaskId, Long> allTaskEndOffsetSums;
        try {
            // Make the listOffsets request first so it can  fetch the offsets for non-source changelogs
            // asynchronously while we use the blocking Consumer#committed call to fetch source-changelog offsets;
            // note that we would need to wrap all exceptions as Streams exception with partition-level fine-grained
            // error messages
            final ListOffsetsResult endOffsetsResult =
                fetchEndOffsetsResult(changelogTopics.preExistingNonSourceTopicBasedPartitions(), adminClient);

            final Map<TopicPartition, Long> sourceChangelogEndOffsets =
                fetchCommittedOffsets(changelogTopics.preExistingSourceTopicBasedPartitions(), mainConsumerSupplier.get());

            final Map<TopicPartition, ListOffsetsResultInfo> endOffsets = ClientUtils.getEndOffsets(
                endOffsetsResult, changelogTopics.preExistingNonSourceTopicBasedPartitions());

            allTaskEndOffsetSums = computeEndOffsetSumsByTask(
                endOffsets,
                sourceChangelogEndOffsets,
                changelogTopics
            );
            fetchEndOffsetsSuccessful = true;
        } catch (final StreamsException | TimeoutException e) {
            log.info("Failed to retrieve all end offsets for changelogs, and hence could not calculate the per-task lag; " +
                "this is not a fatal error but would cause the assignor to fallback to a naive algorithm", e);
            allTaskEndOffsetSums = changelogTopics.statefulTaskIds().stream().collect(Collectors.toMap(t -> t, t -> UNKNOWN_OFFSET_SUM));
            fetchEndOffsetsSuccessful = false;
        }

        for (final Map.Entry<ProcessId, ClientMetadata> entry : clientMetadataMap.entrySet()) {
            final ProcessId processId = entry.getKey();
            final ClientState state = entry.getValue().state;
            state.initializePrevTasks(taskForPartition, taskManager.topologyMetadata().hasNamedTopologies());

            state.computeTaskLags(processId, allTaskEndOffsetSums);
            clientStates.put(processId, state);
        }

        return fetchEndOffsetsSuccessful;
    }

    /**
     * @param endOffsets the listOffsets result from the adminClient
     * @param sourceChangelogEndOffsets the end (committed) offsets of optimized source changelogs
     * @param changelogTopics object that manages changelog topics
     *
     * @return Map from stateful task to its total end offset summed across all changelog partitions
     */
    private Map<TaskId, Long> computeEndOffsetSumsByTask(final Map<TopicPartition, ListOffsetsResultInfo> endOffsets,
                                                         final Map<TopicPartition, Long> sourceChangelogEndOffsets,
                                                         final ChangelogTopics changelogTopics) {

        final Map<TaskId, Long> taskEndOffsetSums = new HashMap<>();
        for (final TaskId taskId : changelogTopics.statefulTaskIds()) {
            taskEndOffsetSums.put(taskId, 0L);
            for (final TopicPartition changelogPartition : changelogTopics.preExistingPartitionsFor(taskId)) {
                final long changelogPartitionEndOffset;
                if (sourceChangelogEndOffsets.containsKey(changelogPartition)) {
                    changelogPartitionEndOffset = sourceChangelogEndOffsets.get(changelogPartition);
                } else if (endOffsets.containsKey(changelogPartition)) {
                    changelogPartitionEndOffset = endOffsets.get(changelogPartition).offset();
                } else {
                    log.debug("Fetched offsets did not contain the changelog {} of task {}", changelogPartition, taskId);
                    throw new IllegalStateException("Could not get end offset for " + changelogPartition);
                }
                final long newEndOffsetSum = taskEndOffsetSums.get(taskId) + changelogPartitionEndOffset;
                if (newEndOffsetSum < 0) {
                    taskEndOffsetSums.put(taskId, Long.MAX_VALUE);
                    break;
                } else {
                    taskEndOffsetSums.put(taskId, newEndOffsetSum);
                }
            }
        }
        return taskEndOffsetSums;
    }

    /**
     * Populates the global partitionsByHost and standbyPartitionsByHost maps that are sent to each member
     *
     * @param partitionsByHost a map from host to the set of partitions hosted there. Populated here.
     * @param standbyPartitionsByHost a map from host to the set of standby partitions hosted there. Populated here.
     * @param partitionsForTask a map from task to its set of assigned partitions
     * @param clientMetadataMap a map from client to its metadata and state
     */
    private void populatePartitionsByHostMaps(final Map<HostInfo, Set<TopicPartition>> partitionsByHost,
                                              final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                              final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                              final Map<ProcessId, ClientMetadata> clientMetadataMap) {
        for (final Map.Entry<ProcessId, ClientMetadata> entry : clientMetadataMap.entrySet()) {
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

    /**
     * Computes the assignment of tasks to threads within each client and assembles the final assignment to send out.
     *
     * @return the final assignment for each StreamThread consumer
     */
    private Map<String, Assignment> computeNewAssignment(final Set<TaskId> statefulTasks,
                                                         final Map<ProcessId, ClientMetadata> clientsMetadata,
                                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                         final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                         final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                                         final Set<TopicPartition> allOwnedPartitions,
                                                         final int minUserMetadataVersion,
                                                         final int minSupportedMetadataVersion,
                                                         final boolean versionProbing) {
        boolean rebalanceRequired = versionProbing;
        final Map<String, Assignment> assignment = new HashMap<>();

        // within the client, distribute tasks to its owned consumers
        for (final Map.Entry<ProcessId, ClientMetadata> clientEntry : clientsMetadata.entrySet()) {
            final ProcessId clientId = clientEntry.getKey();
            final ClientMetadata clientMetadata = clientEntry.getValue();
            final ClientState state = clientMetadata.state;
            final SortedSet<String> consumers = clientMetadata.consumers;
            final Map<String, Integer> threadTaskCounts = new HashMap<>();

            final Map<String, List<TaskId>> activeTaskStatefulAssignment = assignTasksToThreads(
                state.statefulActiveTasks(),
                true,
                consumers,
                state,
                threadTaskCounts
            );

            final Map<String, List<TaskId>> standbyTaskAssignment = assignTasksToThreads(
                state.standbyTasks(),
                true,
                consumers,
                state,
                threadTaskCounts
            );

            final Map<String, List<TaskId>> activeTaskStatelessAssignment = assignTasksToThreads(
                state.statelessActiveTasks(),
                false,
                consumers,
                state,
                threadTaskCounts
            );

            // Combine activeTaskStatefulAssignment and activeTaskStatelessAssignment together into
            // activeTaskStatelessAssignment
            final Map<String, List<TaskId>> activeTaskAssignment = activeTaskStatefulAssignment;
            for (final Map.Entry<String, List<TaskId>> threadEntry : activeTaskStatelessAssignment.entrySet()) {
                activeTaskAssignment.get(threadEntry.getKey()).addAll(threadEntry.getValue());
            }

            final boolean isNextProbingRebalanceEncoded = clientMetadata.state.followupRebalanceDeadline().isPresent();

            final boolean tasksRevoked = addClientAssignments(
                statefulTasks,
                assignment,
                clientMetadata,
                partitionsForTask,
                partitionsByHostState,
                standbyPartitionsByHost,
                allOwnedPartitions,
                activeTaskAssignment,
                standbyTaskAssignment,
                minUserMetadataVersion,
                minSupportedMetadataVersion
            );

            if (tasksRevoked || isNextProbingRebalanceEncoded) {
                rebalanceRequired = true;
                log.debug("Requested client {} to schedule a followup rebalance", clientId);
            }

            log.info("Client {} per-consumer assignment:\n" +
                "\tprev owned active {}\n" +
                "\tprev owned standby {}\n" +
                "\tassigned active {}\n" +
                "\trevoking active {}\n" +
                "\tassigned standby {}\n",
                clientId,
                clientMetadata.state.prevOwnedActiveTasksByConsumer(),
                clientMetadata.state.prevOwnedStandbyByConsumer(),
                clientMetadata.state.assignedActiveTasksByConsumer(),
                clientMetadata.state.revokingActiveTasksByConsumer(),
                clientMetadata.state.assignedStandbyTasksByConsumer());
        }

        if (rebalanceRequired) {
            assignmentListener.onAssignmentComplete(false);
            log.info("Finished unstable assignment of tasks, a followup rebalance will be scheduled.");
        } else {
            assignmentListener.onAssignmentComplete(true);
            log.info("Finished stable assignment of tasks, no followup rebalances required.");
        }

        return assignment;
    }

    /**
     * Adds the encoded assignment for each StreamThread consumer in the client to the overall assignment map
     * @return true if a followup rebalance will be required due to revoked tasks
     */
    private boolean addClientAssignments(final Set<TaskId> statefulTasks,
                                         final Map<String, Assignment> assignment,
                                         final ClientMetadata clientMetadata,
                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                         final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                         final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                         final Set<TopicPartition> allOwnedPartitions,
                                         final Map<String, List<TaskId>> activeTaskAssignments,
                                         final Map<String, List<TaskId>> standbyTaskAssignments,
                                         final int minUserMetadataVersion,
                                         final int minSupportedMetadataVersion) {
        boolean followupRebalanceRequiredForRevokedTasks = false;

        // We only want to encode a scheduled probing rebalance for a single member in this client
        final Optional<Instant> followupRebalanceDeadline = clientMetadata.state.followupRebalanceDeadline();
        boolean shouldEncodeProbingRebalance = followupRebalanceDeadline.isPresent();

        // Loop through the consumers and build their assignment
        for (final String consumer : clientMetadata.consumers) {
            final List<TaskId> activeTasksForConsumer = activeTaskAssignments.get(consumer);

            // These will be filled in by populateActiveTaskAndPartitionsLists below
            final List<TopicPartition> activePartitionsList = new ArrayList<>();
            final List<TaskId> assignedActiveList = new ArrayList<>();

            final Set<TaskId> activeTasksRemovedPendingRevokation = populateActiveTaskAndPartitionsLists(
                activePartitionsList,
                assignedActiveList,
                consumer,
                clientMetadata.state,
                activeTasksForConsumer,
                partitionsForTask,
                allOwnedPartitions
            );

            final Map<TaskId, Set<TopicPartition>> standbyTaskMap = buildStandbyTaskMap(
                    consumer,
                    standbyTaskAssignments.get(consumer),
                    activeTasksRemovedPendingRevokation,
                    statefulTasks,
                    partitionsForTask,
                    clientMetadata.state
                );

            final AssignmentInfo info = new AssignmentInfo(
                minUserMetadataVersion,
                minSupportedMetadataVersion,
                assignedActiveList,
                standbyTaskMap,
                partitionsByHostState,
                standbyPartitionsByHost,
                AssignorError.NONE.code()
            );

            if (!activeTasksRemovedPendingRevokation.isEmpty()) {
                // TODO: once KAFKA-10078 is resolved we can leave it to the client to trigger this rebalance
                log.info("Requesting followup rebalance be scheduled immediately by {} due to tasks changing ownership.", consumer);
                info.setNextRebalanceTime(0L);
                followupRebalanceRequiredForRevokedTasks = true;
                // Don't bother to schedule a probing rebalance if an immediate one is already scheduled
                shouldEncodeProbingRebalance = false;
            } else if (shouldEncodeProbingRebalance) {
                final long nextRebalanceTimeMs = followupRebalanceDeadline.get().toEpochMilli();
                log.info("Requesting followup rebalance be scheduled by {} for {} to probe for caught-up replica tasks.",
                        consumer, Utils.toLogDateTimeFormat(nextRebalanceTimeMs));
                info.setNextRebalanceTime(nextRebalanceTimeMs);
                shouldEncodeProbingRebalance = false;
            }

            assignment.put(
                consumer,
                new Assignment(
                    activePartitionsList,
                    info.encode()
                )
            );
        }
        return followupRebalanceRequiredForRevokedTasks;
    }

    /**
     * Populates the lists of active tasks and active task partitions for the consumer with a 1:1 mapping between them
     * such that the nth task corresponds to the nth partition in the list. This means tasks with multiple partitions
     * will be repeated in the list.
     */
    private Set<TaskId> populateActiveTaskAndPartitionsLists(final List<TopicPartition> activePartitionsList,
                                                             final List<TaskId> assignedActiveList,
                                                             final String consumer,
                                                             final ClientState clientState,
                                                             final List<TaskId> activeTasksForConsumer,
                                                             final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                             final Set<TopicPartition> allOwnedPartitions) {
        final List<AssignedPartition> assignedPartitions = new ArrayList<>();
        final Set<TaskId> removedActiveTasks = new TreeSet<>();

        for (final TaskId taskId : activeTasksForConsumer) {
            // Populate the consumer for assigned tasks without considering revocation,
            // this is for debugging purposes only
            clientState.assignActiveToConsumer(taskId, consumer);

            final List<AssignedPartition> assignedPartitionsForTask = new ArrayList<>();
            for (final TopicPartition partition : partitionsForTask.get(taskId)) {
                final String oldOwner = clientState.previousOwnerForPartition(partition);
                final boolean newPartitionForConsumer = oldOwner == null || !oldOwner.equals(consumer);

                // If the partition is new to this consumer but is still owned by another, remove from the assignment
                // until it has been revoked and can safely be reassigned according to the COOPERATIVE protocol
                if (newPartitionForConsumer && allOwnedPartitions.contains(partition)) {
                    log.info(
                        "Removing task {} from {} active assignment until it is safely revoked in followup rebalance",
                        taskId,
                        consumer
                    );
                    removedActiveTasks.add(taskId);

                    clientState.revokeActiveFromConsumer(taskId, consumer);

                    // Clear the assigned partitions list for this task if any partition can not safely be assigned,
                    // so as not to encode a partial task
                    assignedPartitionsForTask.clear();

                    // This has no effect on the assignment, as we'll never consult the ClientState again, but
                    // it does perform a useful assertion that the task was actually assigned.
                    clientState.unassignActive(taskId);
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
        return removedActiveTasks;
    }

    /**
     * @return map from task id to its assigned partitions for all standby tasks
     */
    private Map<TaskId, Set<TopicPartition>> buildStandbyTaskMap(final String consumer,
                                                                 final Iterable<TaskId> standbyTasks,
                                                                 final Iterable<TaskId> revokedTasks,
                                                                 final Set<TaskId> allStatefulTasks,
                                                                 final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                                 final ClientState clientState) {
        final Map<TaskId, Set<TopicPartition>> standbyTaskMap = new HashMap<>();

        for (final TaskId task : standbyTasks) {
            clientState.assignStandbyToConsumer(task, consumer);
            standbyTaskMap.put(task, partitionsForTask.get(task));
        }

        for (final TaskId task : revokedTasks) {
            // If this task is stateful and already owned by the consumer, but can't (yet) be assigned as an active
            // task during this rebalance as it must be revoked from another consumer first, place a temporary
            // standby task here until it can receive the active task to avoid closing the state store (and losing
            // all of the accumulated state in the case of in-memory stores)
            if (clientState.previouslyOwnedStandby(task) && allStatefulTasks.contains(task)) {
                log.info("Adding removed stateful active task {} as a standby for {} until it is revoked and can "
                             + "be transitioned to active in a followup rebalance", task, consumer);

                // This has no effect on the assignment, as we'll never consult the ClientState again, but
                // it does perform a useful assertion that the it's legal to assign this task as a standby to this instance
                clientState.assignStandbyToConsumer(task, consumer);
                clientState.assignStandby(task);

                standbyTaskMap.put(task, partitionsForTask.get(task));
            }
        }
        return standbyTaskMap;
    }

    /**
     * Generate an assignment that tries to preserve thread-level stickiness for stateful tasks without violating
     * balance. The tasks are balanced across threads. Stateful tasks without previous owners will be interleaved by
     * group id to spread subtopologies across threads and further balance the workload.
     * Stateless tasks are simply spread across threads without taking into account previous ownership.
     * threadLoad is a map that keeps track of task load per thread across multiple calls so active and standby
     * tasks are evenly distributed
     */
    static Map<String, List<TaskId>> assignTasksToThreads(final Collection<TaskId> tasksToAssign,
                                                          final boolean isStateful,
                                                          final SortedSet<String> consumers,
                                                          final ClientState state,
                                                          final Map<String, Integer> threadLoad) {
        final Map<String, List<TaskId>> assignment = new HashMap<>();
        for (final String consumer : consumers) {
            assignment.put(consumer, new ArrayList<>());
        }

        final int totalTasks = threadLoad.values().stream().reduce(tasksToAssign.size(), Integer::sum);

        final int minTasksPerThread = (int) Math.floor(((double) totalTasks) / consumers.size());
        final PriorityQueue<TaskId> unassignedTasks = new PriorityQueue<>(tasksToAssign);

        final Queue<String> consumersToFill = new LinkedList<>();
        // keep track of tasks that we have to skip during the first pass in case we can reassign them later
        // using tree-map to make sure the iteration ordering over keys are preserved
        final Map<TaskId, String> unassignedTaskToPreviousOwner = new TreeMap<>();

        if (!unassignedTasks.isEmpty()) {
            // First assign tasks to previous owner, up to the min expected tasks/thread if these are stateful
            for (final String consumer : consumers) {
                final List<TaskId> threadAssignment = assignment.get(consumer);
                // The number of tasks we have to assign here to hit minTasksPerThread
                final int tasksTargetCount = minTasksPerThread - threadLoad.getOrDefault(consumer, 0);

                if (isStateful) {
                    for (final TaskId task : state.prevTasksByLag(consumer)) {
                        if (unassignedTasks.contains(task)) {
                            if (threadAssignment.size() < tasksTargetCount) {
                                threadAssignment.add(task);
                                unassignedTasks.remove(task);
                            } else {
                                unassignedTaskToPreviousOwner.put(task, consumer);
                            }
                        }
                    }
                }

                if (threadAssignment.size() < tasksTargetCount) {
                    consumersToFill.offer(consumer);
                }
            }

            // Next interleave remaining unassigned tasks amongst unfilled consumers
            while (!consumersToFill.isEmpty()) {
                final TaskId task = unassignedTasks.poll();
                if (task != null) {
                    final String consumer = consumersToFill.poll();
                    final List<TaskId> threadAssignment = assignment.get(consumer);
                    threadAssignment.add(task);
                    final int threadTaskCount = threadAssignment.size() + threadLoad.getOrDefault(consumer, 0);
                    if (threadTaskCount < minTasksPerThread) {
                        consumersToFill.offer(consumer);
                    }
                } else {
                    throw new TaskAssignmentException("Ran out of unassigned stateful tasks but some members were not at capacity");
                }
            }

            // At this point all consumers are at the min or min + 1 capacity.
            // The min + 1 case can occur for standbys where there's fewer standbys than consumers and after assigning
            // the active tasks some consumers already have min + 1 one tasks assigned.
            // The tasks still remaining should now be distributed over the consumers that are still at min capacity
            if (!unassignedTasks.isEmpty()) {
                for (final String consumer : consumers) {
                    final int taskCount = assignment.get(consumer).size() + threadLoad.getOrDefault(consumer, 0);
                    if (taskCount == minTasksPerThread) {
                        consumersToFill.add(consumer);
                    }
                }

                // Go over the tasks we skipped earlier and assign them to their previous owner when possible
                for (final Map.Entry<TaskId, String> taskEntry : unassignedTaskToPreviousOwner.entrySet()) {
                    final TaskId task = taskEntry.getKey();
                    final String consumer = taskEntry.getValue();
                    if (consumersToFill.contains(consumer) && unassignedTasks.contains(task)) {
                        assignment.get(consumer).add(task);
                        unassignedTasks.remove(task);
                        // Remove this consumer since we know it is now at minCapacity + 1
                        consumersToFill.remove(consumer);
                    }
                }

                // Now just distribute the remaining unassigned stateful tasks over the consumers still at min capacity
                for (final TaskId task : unassignedTasks) {
                    final String consumer = consumersToFill.poll();
                    final List<TaskId> threadAssignment = assignment.get(consumer);
                    threadAssignment.add(task);
                }
            }
        }
        // Update threadLoad
        for (final Map.Entry<String, List<TaskId>> taskEntry : assignment.entrySet()) {
            final String consumer = taskEntry.getKey();
            final int totalCount = threadLoad.getOrDefault(consumer, 0) + taskEntry.getValue().size();
            threadLoad.put(consumer, totalCount);
        }

        return assignment;
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

        // version 1 field
        final Map<TaskId, Set<TopicPartition>> activeTasks;
        // version 2 fields
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo;
        final Map<HostInfo, Set<TopicPartition>> partitionsByHost;
        final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost;
        final long encodedNextScheduledRebalanceMs;

        switch (receivedAssignmentMetadataVersion) {
            case 1:
                validateActiveTaskEncoding(partitions, info, logPrefix);

                activeTasks = getActiveTasks(partitions, info);
                partitionsByHost = Collections.emptyMap();
                standbyPartitionsByHost = Collections.emptyMap();
                topicToPartitionInfo = Collections.emptyMap();
                encodedNextScheduledRebalanceMs = Long.MAX_VALUE;
                break;
            case 2:
            case 3:
            case 4:
            case 5:
                validateActiveTaskEncoding(partitions, info, logPrefix);

                activeTasks = getActiveTasks(partitions, info);
                partitionsByHost = info.partitionsByHost();
                standbyPartitionsByHost = Collections.emptyMap();
                topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                encodedNextScheduledRebalanceMs = Long.MAX_VALUE;
                break;
            case 6:
                validateActiveTaskEncoding(partitions, info, logPrefix);

                activeTasks = getActiveTasks(partitions, info);
                partitionsByHost = info.partitionsByHost();
                standbyPartitionsByHost = info.standbyPartitionByHost();
                topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                encodedNextScheduledRebalanceMs = Long.MAX_VALUE;
                break;
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
                validateActiveTaskEncoding(partitions, info, logPrefix);

                activeTasks = getActiveTasks(partitions, info);
                partitionsByHost = info.partitionsByHost();
                standbyPartitionsByHost = info.standbyPartitionByHost();
                topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                encodedNextScheduledRebalanceMs = info.nextRebalanceMs();
                break;
            default:
                throw new IllegalStateException(
                    "This code should never be reached."
                        + " Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/"
                );
        }

        maybeScheduleFollowupRebalance(
            encodedNextScheduledRebalanceMs,
            receivedAssignmentMetadataVersion,
            latestCommonlySupportedVersion,
            partitionsByHost.keySet()
        );

        streamsMetadataState.onChange(partitionsByHost, standbyPartitionsByHost, topicToPartitionInfo);

        // we do not capture any exceptions but just let the exception thrown from consumer.poll directly
        // since when stream thread captures it, either we close all tasks as dirty or we close thread
        taskManager.handleAssignment(activeTasks, info.standbyTasks());
    }

    private void maybeScheduleFollowupRebalance(final long encodedNextScheduledRebalanceMs,
                                                final int receivedAssignmentMetadataVersion,
                                                final int latestCommonlySupportedVersion,
                                                final Set<HostInfo> groupHostInfo) {
        if (maybeUpdateSubscriptionVersion(receivedAssignmentMetadataVersion, latestCommonlySupportedVersion)) {
            log.info("Requested to schedule immediate rebalance due to version probing.");
            nextScheduledRebalanceMs.set(0L);
        } else if (!verifyHostInfo(groupHostInfo)) {
            log.info("Requested to schedule immediate rebalance to update group with new host endpoint = {}.", userEndPoint);
            nextScheduledRebalanceMs.set(0L);
        } else if (encodedNextScheduledRebalanceMs == 0L) {
            log.info("Requested to schedule immediate rebalance for new tasks to be safely revoked from current owner.");
            nextScheduledRebalanceMs.set(0L);
        } else if (encodedNextScheduledRebalanceMs < Long.MAX_VALUE) {
            log.info(
                "Requested to schedule next probing rebalance at {} to try for a more balanced assignment.",
                Utils.toLogDateTimeFormat(encodedNextScheduledRebalanceMs)
            );
            nextScheduledRebalanceMs.set(encodedNextScheduledRebalanceMs);
        } else {
            log.info("No followup rebalance was requested, resetting the rebalance schedule.");
            nextScheduledRebalanceMs.set(Long.MAX_VALUE);
        }
    }

    /**
     * Verify that this client's host info was included in the map returned in the assignment, and trigger a
     * rebalance if not. This may be necessary when using static membership, as a rejoining client will be handed
     * back its original assignment to avoid an unnecessary rebalance. If the client's endpoint has changed, we need
     * to force a rebalance for the other members in the group to get the updated host info for this client.
     *
     * @param groupHostInfo the HostInfo of all clients in the group
     * @return false if the current host info does not match that in the group assignment
     */
    private boolean verifyHostInfo(final Set<HostInfo> groupHostInfo) {
        if (userEndPoint != null && !groupHostInfo.isEmpty()) {
            final HostInfo myHostInfo = HostInfo.buildFromEndpoint(userEndPoint);

            return groupHostInfo.contains(myHostInfo);
        } else {
            return true;
        }
    }

    // protected for upgrade test
    protected static Map<TaskId, Set<TopicPartition>> getActiveTasks(final List<TopicPartition> partitions, final AssignmentInfo info) {
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            final TopicPartition partition = partitions.get(i);
            final TaskId id = info.activeTasks().get(i);
            activeTasks.computeIfAbsent(id, k1 -> new HashSet<>()).add(partition);
        }
        return activeTasks;
    }

    static Map<TopicPartition, PartitionInfo> getTopicPartitionInfo(final Map<HostInfo, Set<TopicPartition>> partitionsByHost) {
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
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
        return topicToPartitionInfo;
    }

    private static void validateActiveTaskEncoding(final List<TopicPartition> partitions, final AssignmentInfo info, final String logPrefix) {
        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks().size()) {
            throw new TaskAssignmentException(
                String.format(
                    "%sNumber of assigned partitions %d is not equal to "
                        + "the number of active taskIds %d, assignmentInfo=%s",
                    logPrefix, partitions.size(),
                    info.activeTasks().size(), info
                )
            );
        }
    }

    private int updateMinReceivedVersion(final int usedVersion, final int minReceivedMetadataVersion) {
        return Math.min(usedVersion, minReceivedMetadataVersion);
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

    // following functions are for test only
    void setInternalTopicManager(final InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

    RebalanceProtocol rebalanceProtocol() {
        return rebalanceProtocol;
    }

    protected String userEndPoint() {
        return userEndPoint;
    }

    protected TaskManager taskManager() {
        return taskManager;
    }

    protected byte uniqueField() {
        return uniqueField;
    }

    protected Map<String, String> clientTags() {
        return clientTags;
    }

    protected void handleRebalanceStart(final Set<String> topics) {
        taskManager.handleRebalanceStart(topics);
    }

    long acceptableRecoveryLag() {
        return assignmentConfigs.acceptableRecoveryLag();
    }

    int maxWarmupReplicas() {
        return assignmentConfigs.maxWarmupReplicas();
    }

    int numStandbyReplicas() {
        return assignmentConfigs.numStandbyReplicas();
    }

    long probingRebalanceIntervalMs() {
        return assignmentConfigs.probingRebalanceIntervalMs();
    }

}
