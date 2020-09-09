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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.processor.internals.assignment.AssignorError;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.CopartitionedTopicsEnforcer;
import org.apache.kafka.streams.processor.internals.assignment.FallbackPriorTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchCommittedOffsets;
import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchEndOffsetsFuture;
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

    private static class ClientMetadata {

        private final HostInfo hostInfo;
        private final ClientState state;
        private final SortedSet<String> consumers;

        ClientMetadata(final String endPoint) {

            // get the host info, or null if no endpoint is configured (ie endPoint == null)
            hostInfo = HostInfo.buildFromEndpoint(endPoint);

            // initialize the consumer memberIds
            consumers = new TreeSet<>();

            // initialize the client state
            state = new ClientState();
        }

        void addConsumer(final String consumerMemberId, final List<TopicPartition> ownedPartitions) {
            consumers.add(consumerMemberId);
            state.incrementCapacity();
            state.addOwnedPartitions(ownedPartitions, consumerMemberId);
        }

        void addPreviousTasksAndOffsetSums(final String consumerId, final Map<TaskId, Long> taskOffsetSums) {
            state.addPreviousTasksAndOffsetSums(consumerId, taskOffsetSums);
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

    private static class NumOfRepartitionsCalculator {
        /**
         * A TopicNode is a node that contains topic information and upstream/downstream TopicsInfo. Graph built of TopicNode and TopicsInfoNode is useful
         * when in certain cases traverse is needed. For example, {@link #setRepartitionTopicMetadataNumberOfPartitions(Map, Map, Cluster)}
         * internally do a DFS search along with the graph.
         *
             TopicNode("t1")      TopicNode("t2")                                    TopicNode("t6")             TopicNode("t7")
                    \           /                                                            \                           /
                 TopicsInfoNode(source = (t1,t2), sink = (t3,t4))                           TopicsInfoNode(source = (t6,t7), sink = (t4))
                                    /           \                                                                        /
                                 /                 \                                                          /
                            /                        \                                           /
                        /                                \                           /
                    /                                       \            /
         TopicNode("t3")                                     TopicNode("t4")
                \
         TopicsInfoNode(source = (t3), sink = ())

         t3 = max(t1,t2)
         t4 = max(max(t1,t2), max(t6,t7))
         */
        private static class TopicNode {
            public final String topicName;
            public final Set<TopicsInfoNode> upStreams; // upStream TopicsInfo's sinkTopics contains this
            public Optional<Integer> numOfRepartitions;
            TopicNode(final String topicName) {
                this.topicName = topicName;
                this.upStreams = new HashSet<>();
                this.numOfRepartitions = Optional.empty();
            }

            public void addUpStreamTopicsInfo(final TopicsInfo topicsInfo) {
                this.upStreams.add(new TopicsInfoNode(topicsInfo));
            }
        }

        // Node wrapper for TopicsInfo, which can be used together with TopicNode to build a graph to calculate partition
        // number of repartition topics, and numOfRepartitions of underlying TopicsInfo is used for memoization.
        private static class TopicsInfoNode {
            public TopicsInfo topicsInfo;
            private Optional<Integer> numOfRepartitions;
            TopicsInfoNode(final TopicsInfo topicsInfo) {
                this.topicsInfo = topicsInfo;
                this.numOfRepartitions = Optional.empty();
            }

            public void setNumOfRepartitions(final int numOfRepartitions) {
                if (numOfRepartitions < 0) throw new IllegalArgumentException("numOfRepartitions of a TopicsInfoNode should be non-negative");
                this.numOfRepartitions = Optional.of(numOfRepartitions);
            }

            public Optional<Integer> numOfRepartitions() {
                return this.numOfRepartitions;
            }

            public Set<String> sourceTopics() {
                return this.topicsInfo.sourceTopics;
            }
        }

        public static void setRepartitionTopicMetadataNumberOfPartitions(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                                         final Map<Integer, InternalTopologyBuilder.TopicsInfo> topicGroups,
                                                                         final Cluster metadata) {
            final Set<String> allRepartitionSourceTopics = new HashSet<>();
            final Map<String, TopicNode> builtTopicNodes = new HashMap<>();
            // 1. Build a graph containing the TopicsInfoNode and TopicsNode
            for (final InternalTopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                allRepartitionSourceTopics.addAll(topicsInfo.repartitionSourceTopics.keySet());
                for (final String sourceTopic : topicsInfo.sourceTopics) {
                    builtTopicNodes.computeIfAbsent(sourceTopic, topic -> new TopicNode(topic));
                }

                for (final String sinkTopic : topicsInfo.sinkTopics) {
                    builtTopicNodes.computeIfAbsent(sinkTopic, topic -> new TopicNode(topic));
                    builtTopicNodes.get(sinkTopic).addUpStreamTopicsInfo(topicsInfo);
                }
            }

            // 2. Use DFS along with memoization to calculate repartition number of all repartitionSourceTopics
            for (final String topic : allRepartitionSourceTopics) {
                calcNumOfRepartitionsForTopicNode(topic, repartitionTopicMetadata, metadata, builtTopicNodes);
            }
        }

        private static int calcNumOfRepartitionsForTopicNode(final String topic,
                                                             final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                             final Cluster metadata,
                                                             final Map<String, TopicNode> builtTopicNodes) {
            final TopicNode topicNode = builtTopicNodes.get(topic);
            if (topicNode.numOfRepartitions.isPresent()) {
                return topicNode.numOfRepartitions.get();
            }
            Integer numOfRepartitionsCandidate = null;
            if (repartitionTopicMetadata.containsKey(topic)) {
                final Optional<Integer> maybeNumberPartitions = repartitionTopicMetadata.get(topic).numberOfPartitions();
                // if numberOfPartitions already calculated, return directly
                if (maybeNumberPartitions.isPresent()) {
                    numOfRepartitionsCandidate = maybeNumberPartitions.get();
                } else {
                    // calculate the max numOfRepartitions of its upStream TopicsInfoNodes and set the repartitionTopicMetadata for memoization before return
                    for (final TopicsInfoNode upstream : topicNode.upStreams) {
                        final Integer upStreamRepartitionNum = calcNumOfRepartitionsForTopicInfoNode(upstream, repartitionTopicMetadata, metadata, builtTopicNodes);
                        numOfRepartitionsCandidate = numOfRepartitionsCandidate == null ? upStreamRepartitionNum :
                                (upStreamRepartitionNum > numOfRepartitionsCandidate ? upStreamRepartitionNum : numOfRepartitionsCandidate);
                    }
                    repartitionTopicMetadata.get(topic).setNumberOfPartitions(numOfRepartitionsCandidate);
                }
            } else {
                final Integer count = metadata.partitionCountForTopic(topic);
                if (count == null) {
                    throw new IllegalStateException(
                            "No partition count found for source topic "
                                    + topic
                                    + ", but it should have been."
                    );
                }
                numOfRepartitionsCandidate = count;
            }
            topicNode.numOfRepartitions = Optional.of(numOfRepartitionsCandidate);
            return numOfRepartitionsCandidate;
        }

        private static int calcNumOfRepartitionsForTopicInfoNode(final TopicsInfoNode topicsInfoNode,
                                                                 final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                                 final Cluster metadata,
                                                                 final Map<String, TopicNode> builtTopicNode) {
            Integer numOfRepartitionsCandidate = null;
            if (topicsInfoNode.numOfRepartitions().isPresent()) {
                return topicsInfoNode.numOfRepartitions().get();
            } else {
                for (final String sourceTopic : topicsInfoNode.sourceTopics()) {
                    final Integer sourceTopicNumPartitions = calcNumOfRepartitionsForTopicNode(sourceTopic, repartitionTopicMetadata, metadata, builtTopicNode);
                    numOfRepartitionsCandidate = numOfRepartitionsCandidate == null ? sourceTopicNumPartitions :
                            (sourceTopicNumPartitions > numOfRepartitionsCandidate ? sourceTopicNumPartitions : numOfRepartitionsCandidate);
                }
                topicsInfoNode.setNumOfRepartitions(numOfRepartitionsCandidate);
                return numOfRepartitionsCandidate;
            }
        }
    }

    // keep track of any future consumers in a "dummy" Client since we can't decipher their subscription
    private static final UUID FUTURE_ID = randomUUID();

    protected static final Comparator<TopicPartition> PARTITION_COMPARATOR =
        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    private String userEndPoint;
    private AssignmentConfigs assignmentConfigs;

    private TaskManager taskManager;
    private StreamsMetadataState streamsMetadataState;
    @SuppressWarnings("deprecation")
    private org.apache.kafka.streams.processor.PartitionGrouper partitionGrouper;
    private AtomicInteger assignmentErrorCode;
    private AtomicLong nextScheduledRebalanceMs;
    private Time time;

    protected int usedSubscriptionMetadataVersion = LATEST_SUPPORTED_VERSION;

    private Admin adminClient;
    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
    private RebalanceProtocol rebalanceProtocol;
    private AssignmentListener assignmentListener;

    private Supplier<TaskAssignor> taskAssignorSupplier;

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
        taskManager = assignorConfiguration.taskManager();
        streamsMetadataState = assignorConfiguration.streamsMetadataState();
        assignmentErrorCode = assignorConfiguration.assignmentErrorCode();
        nextScheduledRebalanceMs = assignorConfiguration.nextScheduledRebalanceMs();
        time = assignorConfiguration.time();
        assignmentConfigs = assignorConfiguration.assignmentConfigs();
        partitionGrouper = assignorConfiguration.partitionGrouper();
        userEndPoint = assignorConfiguration.userEndPoint();
        adminClient = assignorConfiguration.adminClient();
        internalTopicManager = assignorConfiguration.internalTopicManager();
        copartitionedTopicsEnforcer = assignorConfiguration.copartitionedTopicsEnforcer();
        rebalanceProtocol = assignorConfiguration.rebalanceProtocol();
        taskAssignorSupplier = assignorConfiguration::taskAssignor;
        assignmentListener = assignorConfiguration.assignmentListener();
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
        // 2. Map from task id to its overall lag

        handleRebalanceStart(topics);

        return new SubscriptionInfo(
            usedSubscriptionMetadataVersion,
            LATEST_SUPPORTED_VERSION,
            taskManager.processId(),
            userEndPoint,
            taskManager.getTaskOffsetSums())
                .encode();
    }

    private Map<String, Assignment> errorAssignment(final Map<UUID, ClientMetadata> clientsMetadata,
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

        final Map<UUID, ClientMetadata> clientMetadataMap = new HashMap<>();
        final Set<TopicPartition> allOwnedPartitions = new HashSet<>();

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
                processId = FUTURE_ID;
                if (!clientMetadataMap.containsKey(FUTURE_ID)) {
                    clientMetadataMap.put(FUTURE_ID, new ClientMetadata(null));
                }
            } else {
                processId = info.processId();
            }

            ClientMetadata clientMetadata = clientMetadataMap.get(processId);

            // create the new client metadata if necessary
            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint());
                clientMetadataMap.put(info.processId(), clientMetadata);
            }

            // add the consumer and any info in its subscription to the client
            clientMetadata.addConsumer(consumerId, subscription.ownedPartitions());
            allOwnedPartitions.addAll(subscription.ownedPartitions());
            clientMetadata.addPreviousTasksAndOffsetSums(consumerId, info.taskOffsetSums());
        }

        final boolean versionProbing =
            checkMetadataVersions(minReceivedMetadataVersion, minSupportedMetadataVersion, futureMetadataVersion);

        log.debug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

        // ---------------- Step One ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        final Map<Integer, TopicsInfo> topicGroups = taskManager.builder().topicGroups();

        final Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions;
        try {
            allRepartitionTopicPartitions = prepareRepartitionTopics(topicGroups, metadata);
        } catch (final TaskAssignmentException | TimeoutException e) {
            return new GroupAssignment(
                errorAssignment(clientMetadataMap,
                    AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
            );
        }

        final Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);

        log.debug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.values());

        // ---------------- Step Two ---------------- //

        // construct the assignment of tasks to clients

        final Set<String> allSourceTopics = new HashSet<>();
        final Map<Integer, Set<String>> sourceTopicsByGroup = new HashMap<>();
        for (final Map.Entry<Integer, TopicsInfo> entry : topicGroups.entrySet()) {
            allSourceTopics.addAll(entry.getValue().sourceTopics);
            sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
        }

        // get the tasks as partition groups from the partition grouper
        final Map<TaskId, Set<TopicPartition>> partitionsForTask =
            partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

        final Set<TaskId> statefulTasks = new HashSet<>();

        final boolean probingRebalanceNeeded;
        try {
            probingRebalanceNeeded = assignTasksToClients(fullMetadata, allSourceTopics, topicGroups, clientMetadataMap, partitionsForTask, statefulTasks);
        } catch (final TaskAssignmentException | TimeoutException e) {
            return new GroupAssignment(
                errorAssignment(clientMetadataMap,
                    AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
            );
        }

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map

        final Map<HostInfo, Set<TopicPartition>> partitionsByHost = new HashMap<>();
        final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost = new HashMap<>();
        if (minReceivedMetadataVersion >= 2) {
            populatePartitionsByHostMaps(partitionsByHost, standbyPartitionsByHost, partitionsForTask, clientMetadataMap);
        }
        streamsMetadataState.onChange(partitionsByHost, standbyPartitionsByHost, fullMetadata);

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
                versionProbing,
                probingRebalanceNeeded
        );

        return new GroupAssignment(assignment);
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
        return versionProbing;
    }

    /**
     * @return a map of repartition topics and their metadata
     */
    private Map<String, InternalTopicConfig> computeRepartitionTopicMetadata(final Map<Integer, TopicsInfo> topicGroups,
                                                                             final Cluster metadata) {
        final Map<String, InternalTopicConfig> repartitionTopicMetadata = new HashMap<>();
        for (final TopicsInfo topicsInfo : topicGroups.values()) {
            for (final String topic : topicsInfo.sourceTopics) {
                if (!topicsInfo.repartitionSourceTopics.containsKey(topic) &&
                        !metadata.topics().contains(topic)) {
                    log.error("Source topic {} is missing/unknown during rebalance, please make sure all source topics " +
                                  "have been pre-created before starting the Streams application. Returning error {}",
                                  topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.name());
                    throw new TaskAssignmentException("Missing source topic during assignment.");
                }
            }
            for (final InternalTopicConfig topic : topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), topic);
            }
        }
        return repartitionTopicMetadata;
    }

    /**
     * Computes and assembles all repartition topic metadata then creates the topics if necessary.
     *
     * @return map from repartition topic to its partition info
     */
    private Map<TopicPartition, PartitionInfo> prepareRepartitionTopics(final Map<Integer, TopicsInfo> topicGroups,
                                                                           final Cluster metadata) {
        final Map<String, InternalTopicConfig> repartitionTopicMetadata = computeRepartitionTopicMetadata(topicGroups, metadata);

        setRepartitionTopicMetadataNumberOfPartitions(repartitionTopicMetadata, topicGroups, metadata);

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(taskManager.builder().copartitionGroups(), repartitionTopicMetadata, metadata);

        // make sure the repartition source topics exist with the right number of partitions,
        // create these topics if necessary
        internalTopicManager.makeReady(repartitionTopicMetadata);

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
        return allRepartitionTopicPartitions;
    }

    /**
     * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
     */
    // visible for testing
    void setRepartitionTopicMetadataNumberOfPartitions(final Map<String, InternalTopicConfig> repartitionTopicMetadata,
                                                               final Map<Integer, TopicsInfo> topicGroups,
                                                               final Cluster metadata) {
        NumOfRepartitionsCalculator.setRepartitionTopicMetadataNumberOfPartitions(repartitionTopicMetadata, topicGroups, metadata);
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
                                      final Map<Integer, Set<TaskId>> tasksForTopicGroup,
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

            tasksForTopicGroup.computeIfAbsent(id.topicGroupId, k -> new HashSet<>()).add(id);
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
     * Resolve changelog topic metadata and create them if necessary. Fills in the changelogsByStatefulTask map and
     * the optimizedSourceChangelogs set and returns the set of changelogs which were newly created.
     */
    private Set<String> prepareChangelogTopics(final Map<Integer, TopicsInfo> topicGroups,
                                               final Map<Integer, Set<TaskId>> tasksForTopicGroup,
                                               final Map<TaskId, Set<TopicPartition>> changelogsByStatefulTask,
                                               final Set<String> optimizedSourceChangelogs) {
        // add tasks to state change log topic subscribers
        final Map<String, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
        for (final Map.Entry<Integer, TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final TopicsInfo topicsInfo = entry.getValue();

            final Set<TaskId> topicGroupTasks = tasksForTopicGroup.get(topicGroupId);
            if (topicGroupTasks == null) {
                log.debug("No tasks found for topic group {}", topicGroupId);
                continue;
            } else if (topicsInfo.stateChangelogTopics.isEmpty()) {
                continue;
            }

            for (final TaskId task : topicGroupTasks) {
                changelogsByStatefulTask.put(
                    task,
                    topicsInfo.stateChangelogTopics
                        .keySet()
                        .stream()
                        .map(topic -> new TopicPartition(topic, task.partition))
                        .collect(Collectors.toSet()));
            }

            for (final InternalTopicConfig topicConfig : topicsInfo.nonSourceChangelogTopics()) {
                 // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                for (final TaskId task : topicGroupTasks) {
                    if (numPartitions < task.partition + 1) {
                        numPartitions = task.partition + 1;
                    }
                }
                topicConfig.setNumberOfPartitions(numPartitions);
                changelogTopicMetadata.put(topicConfig.name(), topicConfig);
            }

            optimizedSourceChangelogs.addAll(topicsInfo.sourceTopicChangelogs());
        }

        final Set<String> newlyCreatedTopics = internalTopicManager.makeReady(changelogTopicMetadata);
        log.debug("Created state changelog topics {} from the parsed topology.", changelogTopicMetadata.values());
        return newlyCreatedTopics;
    }

    /**
     * Assigns a set of tasks to each client (Streams instance) using the configured task assignor, and also
     * populate the stateful tasks that have been assigned to the clients
     * @return true if a probing rebalance should be triggered
     */
    private boolean assignTasksToClients(final Cluster fullMetadata,
                                         final Set<String> allSourceTopics,
                                         final Map<Integer, TopicsInfo> topicGroups,
                                         final Map<UUID, ClientMetadata> clientMetadataMap,
                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                         final Set<TaskId> statefulTasks) {
        if (!statefulTasks.isEmpty())
            throw new IllegalArgumentException("The stateful tasks should not be populated before assigning tasks to clients");

        final Map<TopicPartition, TaskId> taskForPartition = new HashMap<>();
        final Map<Integer, Set<TaskId>> tasksForTopicGroup = new HashMap<>();
        populateTasksForMaps(taskForPartition, tasksForTopicGroup, allSourceTopics, partitionsForTask, fullMetadata);

        final Map<TaskId, Set<TopicPartition>> changelogsByStatefulTask = new HashMap<>();
        final Set<String> optimizedSourceChangelogs = new HashSet<>();
        final Set<String> newlyCreatedChangelogs =
            prepareChangelogTopics(topicGroups, tasksForTopicGroup, changelogsByStatefulTask, optimizedSourceChangelogs);

        final Map<UUID, ClientState> clientStates = new HashMap<>();
        final boolean lagComputationSuccessful =
            populateClientStatesMap(clientStates,
                clientMetadataMap,
                taskForPartition,
                changelogsByStatefulTask,
                newlyCreatedChangelogs,
                optimizedSourceChangelogs
            );

        final Set<TaskId> allTasks = partitionsForTask.keySet();
        statefulTasks.addAll(changelogsByStatefulTask.keySet());

        log.debug("Assigning tasks {} to clients {} with number of replicas {}",
            allTasks, clientStates, numStandbyReplicas());

        final TaskAssignor taskAssignor = createTaskAssignor(lagComputationSuccessful);

        final boolean probingRebalanceNeeded = taskAssignor.assign(clientStates,
                                                                   allTasks,
                                                                   statefulTasks,
                                                                   assignmentConfigs);

        log.info("Assigned tasks {} including stateful {} to clients as: \n{}.",
                allTasks, statefulTasks, clientStates.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue().currentAssignment())
                        .collect(Collectors.joining(Utils.NL)));

        return probingRebalanceNeeded;
    }

    private TaskAssignor createTaskAssignor(final boolean lagComputationSuccessful) {
        final TaskAssignor taskAssignor = taskAssignorSupplier.get();
        if (taskAssignor instanceof StickyTaskAssignor) {
            // special case: to preserve pre-existing behavior, we invoke the StickyTaskAssignor
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
     * @param changelogsByStatefulTask map from each stateful task to its set of changelog topic partitions
     *
     * @return whether we were able to successfully fetch the changelog end offsets and compute each client's lag
     */
    private boolean populateClientStatesMap(final Map<UUID, ClientState> clientStates,
                                            final Map<UUID, ClientMetadata> clientMetadataMap,
                                            final Map<TopicPartition, TaskId> taskForPartition,
                                            final Map<TaskId, Set<TopicPartition>> changelogsByStatefulTask,
                                            final Set<String> newlyCreatedChangelogs,
                                            final Set<String> optimizedSourceChangelogs) {
        boolean fetchEndOffsetsSuccessful;
        Map<TaskId, Long> allTaskEndOffsetSums;
        try {
            final Collection<TopicPartition> allChangelogPartitions =
                changelogsByStatefulTask.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            final Set<TopicPartition> preexistingChangelogPartitions = new HashSet<>();
            final Set<TopicPartition> preexistingSourceChangelogPartitions = new HashSet<>();
            final Set<TopicPartition> newlyCreatedChangelogPartitions = new HashSet<>();
            for (final TopicPartition changelog : allChangelogPartitions) {
                if (newlyCreatedChangelogs.contains(changelog.topic())) {
                    newlyCreatedChangelogPartitions.add(changelog);
                } else if (optimizedSourceChangelogs.contains(changelog.topic())) {
                    preexistingSourceChangelogPartitions.add(changelog);
                } else {
                    preexistingChangelogPartitions.add(changelog);
                }
            }

            // Make the listOffsets request first so it can  fetch the offsets for non-source changelogs
            // asynchronously while we use the blocking Consumer#committed call to fetch source-changelog offsets
            final KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> endOffsetsFuture =
                fetchEndOffsetsFuture(preexistingChangelogPartitions, adminClient);

            final Map<TopicPartition, Long> sourceChangelogEndOffsets =
                fetchCommittedOffsets(preexistingSourceChangelogPartitions, taskManager.mainConsumer());

            final Map<TopicPartition, ListOffsetsResultInfo> endOffsets = ClientUtils.getEndOffsets(endOffsetsFuture);

            allTaskEndOffsetSums = computeEndOffsetSumsByTask(
                changelogsByStatefulTask,
                endOffsets,
                sourceChangelogEndOffsets,
                newlyCreatedChangelogPartitions);
            fetchEndOffsetsSuccessful = true;
        } catch (final StreamsException | TimeoutException e) {
            allTaskEndOffsetSums = changelogsByStatefulTask.keySet().stream().collect(Collectors.toMap(t -> t, t -> UNKNOWN_OFFSET_SUM));
            fetchEndOffsetsSuccessful = false;
        }

        for (final Map.Entry<UUID, ClientMetadata> entry : clientMetadataMap.entrySet()) {
            final UUID uuid = entry.getKey();
            final ClientState state = entry.getValue().state;
            state.initializePrevTasks(taskForPartition);

            state.computeTaskLags(uuid, allTaskEndOffsetSums);
            clientStates.put(uuid, state);
        }
        return fetchEndOffsetsSuccessful;
    }

    /**
     * @param changelogsByStatefulTask map from stateful task to its set of changelog topic partitions
     * @param endOffsets the listOffsets result from the adminClient
     * @param sourceChangelogEndOffsets the end (committed) offsets of optimized source changelogs
     * @param newlyCreatedChangelogPartitions any changelogs that were just created duringthis assignment
     *
     * @return Map from stateful task to its total end offset summed across all changelog partitions
     */
    private Map<TaskId, Long> computeEndOffsetSumsByTask(final Map<TaskId, Set<TopicPartition>> changelogsByStatefulTask,
                                                         final Map<TopicPartition, ListOffsetsResultInfo> endOffsets,
                                                         final Map<TopicPartition, Long> sourceChangelogEndOffsets,
                                                         final Collection<TopicPartition> newlyCreatedChangelogPartitions) {
        final Map<TaskId, Long> taskEndOffsetSums = new HashMap<>();
        for (final Map.Entry<TaskId, Set<TopicPartition>> taskEntry : changelogsByStatefulTask.entrySet()) {
            final TaskId task = taskEntry.getKey();
            final Set<TopicPartition> changelogs = taskEntry.getValue();

            taskEndOffsetSums.put(task, 0L);
            for (final TopicPartition changelog : changelogs) {
                final long changelogEndOffset;
                if (newlyCreatedChangelogPartitions.contains(changelog)) {
                    changelogEndOffset = 0L;
                } else if (sourceChangelogEndOffsets.containsKey(changelog)) {
                    changelogEndOffset = sourceChangelogEndOffsets.get(changelog);
                } else if (endOffsets.containsKey(changelog)) {
                    changelogEndOffset = endOffsets.get(changelog).offset();
                } else {
                    log.debug("Fetched offsets did not contain the changelog {} of task {}", changelog, task);
                    throw new IllegalStateException("Could not get end offset for " + changelog);
                }
                final long newEndOffsetSum = taskEndOffsetSums.get(task) + changelogEndOffset;
                if (newEndOffsetSum < 0) {
                    taskEndOffsetSums.put(task, Long.MAX_VALUE);
                    break;
                } else {
                    taskEndOffsetSums.put(task, newEndOffsetSum);
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
                                              final Map<UUID, ClientMetadata> clientMetadataMap) {
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

    /**
     * Computes the assignment of tasks to threads within each client and assembles the final assignment to send out.
     *
     * @return the final assignment for each StreamThread consumer
     */
    private Map<String, Assignment> computeNewAssignment(final Set<TaskId> statefulTasks,
                                                         final Map<UUID, ClientMetadata> clientsMetadata,
                                                         final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                                         final Map<HostInfo, Set<TopicPartition>> partitionsByHostState,
                                                         final Map<HostInfo, Set<TopicPartition>> standbyPartitionsByHost,
                                                         final Set<TopicPartition> allOwnedPartitions,
                                                         final int minUserMetadataVersion,
                                                         final int minSupportedMetadataVersion,
                                                         final boolean versionProbing,
                                                         final boolean shouldTriggerProbingRebalance) {
        boolean rebalanceRequired = shouldTriggerProbingRebalance || versionProbing;
        final Map<String, Assignment> assignment = new HashMap<>();

        // within the client, distribute tasks to its owned consumers
        for (final Map.Entry<UUID, ClientMetadata> clientEntry : clientsMetadata.entrySet()) {
            final UUID clientId = clientEntry.getKey();
            final ClientMetadata clientMetadata = clientEntry.getValue();
            final ClientState state = clientMetadata.state;
            final SortedSet<String> consumers = clientMetadata.consumers;

            final Map<String, List<TaskId>> activeTaskAssignment = assignTasksToThreads(
                state.statefulActiveTasks(),
                state.statelessActiveTasks(),
                consumers,
                state
            );

            final Map<String, List<TaskId>> standbyTaskAssignment = assignTasksToThreads(
                state.standbyTasks(),
                Collections.emptySet(),
                consumers,
                state
            );

            // Arbitrarily choose the leader's client to be responsible for triggering the probing rebalance,
            // note once we pick the first consumer within the process to trigger probing rebalance, other consumer
            // would not set to trigger any more.
            final boolean encodeNextProbingRebalanceTime = shouldTriggerProbingRebalance && clientId.equals(taskManager.processId());

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
                minSupportedMetadataVersion,
                encodeNextProbingRebalanceTime
            );

            if (tasksRevoked || encodeNextProbingRebalanceTime) {
                rebalanceRequired = true;
                log.debug("Requested client {} to schedule a followup rebalance", clientId);
            }

            log.info("Client {} per-consumer assignment:\n" +
                "\tprev owned active {}\n" +
                "\tprev owned standby {}\n" +
                "\tassigned active {}\n" +
                "\trevoking active {}" +
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
                                         final int minSupportedMetadataVersion,
                                         final boolean probingRebalanceNeeded) {
        boolean followupRebalanceRequiredForRevokedTasks = false;

        // We only want to encode a scheduled probing rebalance for a single member in this client
        boolean shouldEncodeProbingRebalance = probingRebalanceNeeded;

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
                log.info("Requesting {} followup rebalance be scheduled immediately due to tasks changing ownership.", consumer);
                info.setNextRebalanceTime(0L);
                followupRebalanceRequiredForRevokedTasks = true;
                // Don't bother to schedule a probing rebalance if an immediate one is already scheduled
                shouldEncodeProbingRebalance = false;
            } else if (shouldEncodeProbingRebalance) {
                final long nextRebalanceTimeMs = time.milliseconds() + probingRebalanceIntervalMs();
                log.info("Requesting {} followup rebalance be scheduled for {} ms to probe for caught-up replica tasks.",
                        consumer, nextRebalanceTimeMs);
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
            if (allStatefulTasks.contains(task)) {
                log.info("Adding removed stateful active task {} as a standby for {} before it is revoked in followup rebalance",
                        task, consumer);

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
     * Generate an assignment that tries to preserve thread-level stickiness of stateful tasks without violating
     * balance. The stateful and total task load are both balanced across threads. Tasks without previous owners
     * will be interleaved by group id to spread subtopologies across threads and further balance the workload.
     */
    static Map<String, List<TaskId>> assignTasksToThreads(final Collection<TaskId> statefulTasksToAssign,
                                                          final Collection<TaskId> statelessTasksToAssign,
                                                          final SortedSet<String> consumers,
                                                          final ClientState state) {
        final Map<String, List<TaskId>> assignment = new HashMap<>();
        for (final String consumer : consumers) {
            assignment.put(consumer, new ArrayList<>());
        }

        final List<TaskId> unassignedStatelessTasks = new ArrayList<>(statelessTasksToAssign);
        Collections.sort(unassignedStatelessTasks);

        final Iterator<TaskId> unassignedStatelessTasksIter = unassignedStatelessTasks.iterator();

        final int minStatefulTasksPerThread = (int) Math.floor(((double) statefulTasksToAssign.size()) / consumers.size());
        final PriorityQueue<TaskId> unassignedStatefulTasks = new PriorityQueue<>(statefulTasksToAssign);

        final Queue<String> consumersToFill = new LinkedList<>();
        // keep track of tasks that we have to skip during the first pass in case we can reassign them later
        // using tree-map to make sure the iteration ordering over keys are preserved
        final Map<TaskId, String> unassignedTaskToPreviousOwner = new TreeMap<>();

        if (!unassignedStatefulTasks.isEmpty()) {
            // First assign stateful tasks to previous owner, up to the min expected tasks/thread
            for (final String consumer : consumers) {
                final List<TaskId> threadAssignment = assignment.get(consumer);

                for (final TaskId task : getPreviousTasksByLag(state, consumer)) {
                    if (unassignedStatefulTasks.contains(task)) {
                        if (threadAssignment.size() < minStatefulTasksPerThread) {
                            threadAssignment.add(task);
                            unassignedStatefulTasks.remove(task);
                        } else {
                            unassignedTaskToPreviousOwner.put(task, consumer);
                        }
                    }
                }

                if (threadAssignment.size() < minStatefulTasksPerThread) {
                    consumersToFill.offer(consumer);
                }
            }

            // Next interleave remaining unassigned tasks amongst unfilled consumers
            while (!consumersToFill.isEmpty()) {
                final TaskId task = unassignedStatefulTasks.poll();
                if (task != null) {
                    final String consumer = consumersToFill.poll();
                    final List<TaskId> threadAssignment = assignment.get(consumer);
                    threadAssignment.add(task);
                    if (threadAssignment.size() < minStatefulTasksPerThread) {
                        consumersToFill.offer(consumer);
                    }
                } else {
                    throw new IllegalStateException("Ran out of unassigned stateful tasks but some members were not at capacity");
                }
            }

            // At this point all consumers are at the min capacity, so there may be up to N - 1 unassigned
            // stateful tasks still remaining that should now be distributed over the consumers
            if (!unassignedStatefulTasks.isEmpty()) {
                consumersToFill.addAll(consumers);

                // Go over the tasks we skipped earlier and assign them to their previous owner when possible
                for (final Map.Entry<TaskId, String> taskEntry : unassignedTaskToPreviousOwner.entrySet()) {
                    final TaskId task = taskEntry.getKey();
                    final String consumer = taskEntry.getValue();
                    if (consumersToFill.contains(consumer) && unassignedStatefulTasks.contains(task)) {
                        assignment.get(consumer).add(task);
                        unassignedStatefulTasks.remove(task);
                        // Remove this consumer since we know it is now at minCapacity + 1
                        consumersToFill.remove(consumer);
                    }
                }

                // Now just distribute the remaining unassigned stateful tasks over the consumers still at min capacity
                for (final TaskId task : unassignedStatefulTasks) {
                    final String consumer = consumersToFill.poll();
                    final List<TaskId> threadAssignment = assignment.get(consumer);
                    threadAssignment.add(task);
                }


                // There must be at least one consumer still at min capacity while all the others are at min
                // capacity + 1, so start distributing stateless tasks to get all consumers back to the same count
                while (unassignedStatelessTasksIter.hasNext()) {
                    final String consumer = consumersToFill.poll();
                    if (consumer != null) {
                        final TaskId task = unassignedStatelessTasksIter.next();
                        unassignedStatelessTasksIter.remove();
                        assignment.get(consumer).add(task);
                    } else {
                        break;
                    }
                }
            }
        }

        // Now just distribute tasks while circling through all the consumers
        consumersToFill.addAll(consumers);

        while (unassignedStatelessTasksIter.hasNext()) {
            final TaskId task = unassignedStatelessTasksIter.next();
            final String consumer = consumersToFill.poll();
            assignment.get(consumer).add(task);
            consumersToFill.offer(consumer);
        }

        return assignment;
    }

    private static SortedSet<TaskId> getPreviousTasksByLag(final ClientState state, final String consumer) {
        final SortedSet<TaskId> prevTasksByLag = new TreeSet<>(comparingLong(state::lagFor).thenComparing(TaskId::compareTo));
        prevTasksByLag.addAll(state.prevOwnedStatefulTasksByConsumer(consumer));
        return prevTasksByLag;
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

        final Cluster fakeCluster = Cluster.empty().withPartitions(topicToPartitionInfo);
        streamsMetadataState.onChange(partitionsByHost, standbyPartitionsByHost, fakeCluster);

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
            log.info("Requested to schedule probing rebalance for {} ms.", encodedNextScheduledRebalanceMs);
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
                    info.activeTasks().size(), info.toString()
                )
            );
        }
    }

    private void ensureCopartitioning(final Collection<Set<String>> copartitionGroups,
                                      final Map<String, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                                      final Cluster metadata) {
        for (final Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsEnforcer.enforce(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
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

    protected void handleRebalanceStart(final Set<String> topics) {
        taskManager.handleRebalanceStart(topics);
    }

    long acceptableRecoveryLag() {
        return assignmentConfigs.acceptableRecoveryLag;
    }

    int maxWarmupReplicas() {
        return assignmentConfigs.maxWarmupReplicas;
    }

    int numStandbyReplicas() {
        return assignmentConfigs.numStandbyReplicas;
    }

    long probingRebalanceIntervalMs() {
        return assignmentConfigs.probingRebalanceIntervalMs;
    }

}
