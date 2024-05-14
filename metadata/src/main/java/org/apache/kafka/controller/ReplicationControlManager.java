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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfigCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;
import org.apache.kafka.metadata.placement.ClusterDescriber;
import org.apache.kafka.metadata.placement.PartitionAssignment;
import org.apache.kafka.metadata.placement.PlacementSpec;
import org.apache.kafka.metadata.placement.TopicAssignment;
import org.apache.kafka.metadata.placement.UsableBroker;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.common.protocol.Errors.FENCED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.Errors.INELIGIBLE_REPLICA;
import static org.apache.kafka.common.protocol.Errors.INVALID_REQUEST;
import static org.apache.kafka.common.protocol.Errors.INVALID_UPDATE_VERSION;
import static org.apache.kafka.common.protocol.Errors.NEW_LEADER_ELECTED;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.NOT_CONTROLLER;
import static org.apache.kafka.common.protocol.Errors.NO_REASSIGNMENT_IN_PROGRESS;
import static org.apache.kafka.common.protocol.Errors.OPERATION_NOT_ATTEMPTED;
import static org.apache.kafka.common.protocol.Errors.TOPIC_AUTHORIZATION_FAILED;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.controller.PartitionReassignmentReplicas.isReassignmentInProgress;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;


/**
 * The ReplicationControlManager is the part of the controller which deals with topics
 * and partitions. It is responsible for managing the in-sync replica set and leader
 * of each partition, as well as administrative tasks like creating or deleting topics.
 */
public class ReplicationControlManager {
    static final int MAX_ELECTIONS_PER_IMBALANCE = 1_000;

    static class Builder {
        private SnapshotRegistry snapshotRegistry = null;
        private LogContext logContext = null;
        private short defaultReplicationFactor = (short) 3;
        private int defaultNumPartitions = 1;

        private int defaultMinIsr = 1;
        private int maxElectionsPerImbalance = MAX_ELECTIONS_PER_IMBALANCE;
        private ConfigurationControlManager configurationControl = null;
        private ClusterControlManager clusterControl = null;
        private Optional<CreateTopicPolicy> createTopicPolicy = Optional.empty();
        private FeatureControlManager featureControl = null;
        private boolean eligibleLeaderReplicasEnabled = false;

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setDefaultReplicationFactor(short defaultReplicationFactor) {
            this.defaultReplicationFactor = defaultReplicationFactor;
            return this;
        }

        Builder setDefaultNumPartitions(int defaultNumPartitions) {
            this.defaultNumPartitions = defaultNumPartitions;
            return this;
        }

        Builder setDefaultMinIsr(int defaultMinIsr) {
            this.defaultMinIsr = defaultMinIsr;
            return this;
        }

        Builder setEligibleLeaderReplicasEnabled(boolean eligibleLeaderReplicasEnabled) {
            this.eligibleLeaderReplicasEnabled = eligibleLeaderReplicasEnabled;
            return this;
        }

        Builder setMaxElectionsPerImbalance(int maxElectionsPerImbalance) {
            this.maxElectionsPerImbalance = maxElectionsPerImbalance;
            return this;
        }

        Builder setConfigurationControl(ConfigurationControlManager configurationControl) {
            this.configurationControl = configurationControl;
            return this;
        }

        Builder setClusterControl(ClusterControlManager clusterControl) {
            this.clusterControl = clusterControl;
            return this;
        }

        Builder setCreateTopicPolicy(Optional<CreateTopicPolicy> createTopicPolicy) {
            this.createTopicPolicy = createTopicPolicy;
            return this;
        }

        public Builder setFeatureControl(FeatureControlManager featureControl) {
            this.featureControl = featureControl;
            return this;
        }

        ReplicationControlManager build() {
            if (configurationControl == null) {
                throw new IllegalStateException("Configuration control must be set before building");
            } else if (clusterControl == null) {
                throw new IllegalStateException("Cluster control must be set before building");
            }
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = configurationControl.snapshotRegistry();
            if (featureControl == null) {
                throw new IllegalStateException("FeatureControlManager must not be null");
            }
            return new ReplicationControlManager(snapshotRegistry,
                logContext,
                defaultReplicationFactor,
                defaultNumPartitions,
                defaultMinIsr,
                maxElectionsPerImbalance,
                eligibleLeaderReplicasEnabled,
                configurationControl,
                clusterControl,
                createTopicPolicy,
                featureControl);
        }
    }

    class KRaftClusterDescriber implements ClusterDescriber {
        @Override
        public Iterator<UsableBroker> usableBrokers() {
            return clusterControl.usableBrokers();
        }

        @Override
        public Uuid defaultDir(int brokerId) {
            if (featureControl.metadataVersion().isDirectoryAssignmentSupported()) {
                return clusterControl.defaultDir(brokerId);
            } else {
                return DirectoryId.MIGRATING;
            }
        }
    }

    static class TopicControlInfo {
        private final String name;
        private final Uuid id;
        private final TimelineHashMap<Integer, PartitionRegistration> parts;

        TopicControlInfo(String name, SnapshotRegistry snapshotRegistry, Uuid id) {
            this.name = name;
            this.id = id;
            this.parts = new TimelineHashMap<>(snapshotRegistry, 0);
        }

        public String name() {
            return name;
        }

        public Uuid topicId() {
            return id;
        }

        public int numPartitions(long epoch) {
            return parts.size(epoch);
        }
    }

    /**
     * Translate a CreateableTopicConfigCollection to a map from string to string.
     */
    static Map<String, String> translateCreationConfigs(CreateableTopicConfigCollection collection) {
        HashMap<String, String> result = new HashMap<>();
        collection.forEach(config -> result.put(config.name(), config.value()));
        return Collections.unmodifiableMap(result);
    }

    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;

    /**
     * The KIP-464 default replication factor that is used if a CreateTopics request does
     * not specify one.
     */
    private final short defaultReplicationFactor;

    /**
     * The KIP-464 default number of partitions that is used if a CreateTopics request does
     * not specify a number of partitions.
     */
    private final int defaultNumPartitions;

    /**
     * The default min ISR that is used if a CreateTopics request does not specify one.
     */
    private final int defaultMinIsr;

    /**
     * True if eligible leader replicas is enabled.
     */
    private final boolean eligibleLeaderReplicasEnabled;

    /**
     * Maximum number of leader elections to perform during one partition leader balancing operation.
     */
    private final int maxElectionsPerImbalance;

    /**
     * A reference to the controller's configuration control manager.
     */
    private final ConfigurationControlManager configurationControl;

    /**
     * A reference to the controller's cluster control manager.
     */
    private final ClusterControlManager clusterControl;

    /**
     * The policy to use to validate that topic assignments are valid, if one is present.
     */
    private final Optional<CreateTopicPolicy> createTopicPolicy;

    /**
     * The feature control manager.
     */
    private final FeatureControlManager featureControl;

    /**
     * Maps topic names to topic UUIDs.
     */
    private final TimelineHashMap<String, Uuid> topicsByName;

    /**
     * We try to prevent topics from being created if their names would collide with
     * existing topics when periods in the topic name are replaced with underscores.
     * The reason for this is that some per-topic metrics do replace periods with
     * underscores, and would therefore be ambiguous otherwise.
     *
     * This map is from normalized topic name to a set of topic names. So if we had two
     * topics named foo.bar and foo_bar this map would contain
     * a mapping from foo_bar to a set containing foo.bar and foo_bar.
     *
     * Since we reject topic creations that would collide, under normal conditions the
     * sets in this map should only have a size of 1. However, if the cluster was
     * upgraded from a version prior to KAFKA-13743, it may be possible to have more
     * values here, since colliding topic names will be "grandfathered in."
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> topicsWithCollisionChars;

    /**
     * Maps topic UUIDs to structures containing topic information, including partitions.
     */
    private final TimelineHashMap<Uuid, TopicControlInfo> topics;

    /**
     * A map of broker IDs to the partitions that the broker is in the ISR for.
     */
    private final BrokersToIsrs brokersToIsrs;

    /**
     * A map of broker IDs to the partitions that the broker is in the ELR for.
     * Note that, a broker should not be in both brokersToIsrs and brokersToElrs.
     */
    private final BrokersToElrs brokersToElrs;

    /**
     * A map from topic IDs to the partitions in the topic which are reassigning.
     */
    private final TimelineHashMap<Uuid, int[]> reassigningTopics;

    /**
     * The set of topic partitions for which the leader is not the preferred leader.
     */
    private final TimelineHashSet<TopicIdPartition> imbalancedPartitions;

    /**
     * A map from registered directory IDs to the partitions that are stored in that directory.
     */
    private final TimelineHashMap<Uuid, TimelineHashSet<TopicIdPartition>> directoriesToPartitions;

    /**
     * A ClusterDescriber which supplies cluster information to our ReplicaPlacer.
     */
    final KRaftClusterDescriber clusterDescriber = new KRaftClusterDescriber();

    private ReplicationControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        short defaultReplicationFactor,
        int defaultNumPartitions,
        int defaultMinIsr,
        int maxElectionsPerImbalance,
        boolean eligibleLeaderReplicasEnabled,
        ConfigurationControlManager configurationControl,
        ClusterControlManager clusterControl,
        Optional<CreateTopicPolicy> createTopicPolicy,
        FeatureControlManager featureControl
    ) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(ReplicationControlManager.class);
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.defaultNumPartitions = defaultNumPartitions;
        this.defaultMinIsr = defaultMinIsr;
        this.maxElectionsPerImbalance = maxElectionsPerImbalance;
        this.eligibleLeaderReplicasEnabled = eligibleLeaderReplicasEnabled;
        this.configurationControl = configurationControl;
        this.createTopicPolicy = createTopicPolicy;
        this.featureControl = featureControl;
        this.clusterControl = clusterControl;
        this.topicsByName = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topicsWithCollisionChars = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
        this.brokersToElrs = new BrokersToElrs(snapshotRegistry);
        this.reassigningTopics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.imbalancedPartitions = new TimelineHashSet<>(snapshotRegistry, 0);
        this.directoriesToPartitions = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public void replay(TopicRecord record) {
        Uuid existingUuid = topicsByName.put(record.name(), record.topicId());
        if (existingUuid != null) {
            // We don't currently support sending a second TopicRecord for the same topic name...
            // unless, of course, there is a RemoveTopicRecord in between.
            if (existingUuid.equals(record.topicId())) {
                throw new RuntimeException("Found duplicate TopicRecord for " + record.name() +
                        " with topic ID " + record.topicId());
            } else {
                throw new RuntimeException("Found duplicate TopicRecord for " + record.name() +
                        " with a different ID than before. Previous ID was " + existingUuid +
                        " and new ID is " + record.topicId());
            }
        }
        if (Topic.hasCollisionChars(record.name())) {
            String normalizedName = Topic.unifyCollisionChars(record.name());
            TimelineHashSet<String> topicNames = topicsWithCollisionChars.get(normalizedName);
            if (topicNames == null) {
                topicNames = new TimelineHashSet<>(snapshotRegistry, 1);
                topicsWithCollisionChars.put(normalizedName, topicNames);
            }
            topicNames.add(record.name());
        }
        topics.put(record.topicId(),
            new TopicControlInfo(record.name(), snapshotRegistry, record.topicId()));
        log.info("Replayed TopicRecord for topic {} with topic ID {}.", record.name(), record.topicId());
    }

    public void replay(PartitionRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionRegistration newPartInfo = new PartitionRegistration(record);
        PartitionRegistration prevPartInfo = topicInfo.parts.get(record.partitionId());
        String description = topicInfo.name + "-" + record.partitionId() +
            " with topic ID " + record.topicId();
        if (prevPartInfo == null) {
            log.info("Replayed PartitionRecord for new partition {} and {}.", description,
                    newPartInfo);
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            updatePartitionInfo(record.topicId(), record.partitionId(), null, newPartInfo);
            updatePartitionDirectories(record.topicId(), record.partitionId(), null, newPartInfo.directories);
            updateReassigningTopicsIfNeeded(record.topicId(), record.partitionId(),
                    false,  isReassignmentInProgress(newPartInfo));
        } else if (!newPartInfo.equals(prevPartInfo)) {
            log.info("Replayed PartitionRecord for existing partition {} and {}.", description,
                    newPartInfo);
            newPartInfo.maybeLogPartitionChange(log, description, prevPartInfo);
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            updatePartitionInfo(record.topicId(), record.partitionId(), prevPartInfo, newPartInfo);
            updatePartitionDirectories(record.topicId(), record.partitionId(), prevPartInfo.directories, newPartInfo.directories);
            updateReassigningTopicsIfNeeded(record.topicId(), record.partitionId(),
                    isReassignmentInProgress(prevPartInfo), isReassignmentInProgress(newPartInfo));
        }

        if (newPartInfo.hasPreferredLeader()) {
            imbalancedPartitions.remove(new TopicIdPartition(record.topicId(), record.partitionId()));
        } else {
            imbalancedPartitions.add(new TopicIdPartition(record.topicId(), record.partitionId()));
        }
    }

    private void updateReassigningTopicsIfNeeded(Uuid topicId, int partitionId,
                                                 boolean wasReassigning, boolean isReassigning) {
        if (!wasReassigning) {
            if (isReassigning) {
                int[] prevReassigningParts = reassigningTopics.getOrDefault(topicId, Replicas.NONE);
                reassigningTopics.put(topicId, Replicas.copyWith(prevReassigningParts, partitionId));
            }
        } else if (!isReassigning) {
            int[] prevReassigningParts = reassigningTopics.getOrDefault(topicId, Replicas.NONE);
            int[] newReassigningParts = Replicas.copyWithout(prevReassigningParts, partitionId);
            if (newReassigningParts.length == 0) {
                reassigningTopics.remove(topicId);
            } else {
                reassigningTopics.put(topicId, newReassigningParts);
            }
        }
    }

    public void replay(PartitionChangeRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionRegistration prevPartitionInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartitionInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no partition with that id was found.");
        }
        PartitionRegistration newPartitionInfo = prevPartitionInfo.merge(record);
        updateReassigningTopicsIfNeeded(record.topicId(), record.partitionId(),
                isReassignmentInProgress(prevPartitionInfo), isReassignmentInProgress(newPartitionInfo));
        topicInfo.parts.put(record.partitionId(), newPartitionInfo);
        updatePartitionInfo(record.topicId(), record.partitionId(), prevPartitionInfo, newPartitionInfo);
        updatePartitionDirectories(record.topicId(), record.partitionId(), prevPartitionInfo.directories, newPartitionInfo.directories);
        String topicPart = topicInfo.name + "-" + record.partitionId() + " with topic ID " +
            record.topicId();
        newPartitionInfo.maybeLogPartitionChange(log, topicPart, prevPartitionInfo);

        if (newPartitionInfo.hasPreferredLeader()) {
            imbalancedPartitions.remove(new TopicIdPartition(record.topicId(), record.partitionId()));
        } else {
            imbalancedPartitions.add(new TopicIdPartition(record.topicId(), record.partitionId()));
        }

        if (record.removingReplicas() != null || record.addingReplicas() != null) {
            log.info("Replayed partition assignment change {} for topic {}", record, topicInfo.name);
        } else if (log.isDebugEnabled()) {
            log.debug("Replayed partition change {} for topic {}", record, topicInfo.name);
        }
    }

    public void replay(RemoveTopicRecord record) {
        // Remove this topic from the topics map and the topicsByName map.
        TopicControlInfo topic = topics.remove(record.topicId());
        if (topic == null) {
            throw new UnknownTopicIdException("Can't find topic with ID " + record.topicId() +
                " to remove.");
        }
        topicsByName.remove(topic.name);
        if (Topic.hasCollisionChars(topic.name)) {
            String normalizedName = Topic.unifyCollisionChars(topic.name);
            TimelineHashSet<String> colliding = topicsWithCollisionChars.get(normalizedName);
            if (colliding != null) {
                colliding.remove(topic.name);
                if (colliding.isEmpty()) {
                    topicsWithCollisionChars.remove(normalizedName);
                }
            }
        }
        reassigningTopics.remove(record.topicId());

        // Delete the configurations associated with this topic.
        configurationControl.deleteTopicConfigs(topic.name);

        for (Map.Entry<Integer, PartitionRegistration> entry : topic.parts.entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration partition = entry.getValue();

            // Remove the entries for this topic in brokersToIsrs.
            for (int i = 0; i < partition.isr.length; i++) {
                brokersToIsrs.removeTopicEntryForBroker(topic.id, partition.isr[i]);
                updatePartitionDirectories(topic.id, partitionId, partition.directories, null);
            }

            for (int elrMember : partition.elr) {
                brokersToElrs.removeTopicEntryForBroker(topic.id, elrMember);
            }

            imbalancedPartitions.remove(new TopicIdPartition(record.topicId(), partitionId));
        }
        brokersToIsrs.removeTopicEntryForBroker(topic.id, NO_LEADER);

        log.info("Replayed RemoveTopicRecord for topic {} with ID {}.", topic.name, record.topicId());
    }

    ControllerResult<CreateTopicsResponseData> createTopics(
        ControllerRequestContext context,
        CreateTopicsRequestData request,
        Set<String> describable
    ) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);

        // Check the topic names.
        validateNewTopicNames(topicErrors, request.topics(), topicsWithCollisionChars);

        // Identify topics that already exist and mark them with the appropriate error
        request.topics().stream().filter(creatableTopic -> topicsByName.containsKey(creatableTopic.name()))
                .forEach(t -> topicErrors.put(t.name(), new ApiError(Errors.TOPIC_ALREADY_EXISTS,
                    "Topic '" + t.name() + "' already exists.")));

        // Verify that the configurations for the new topics are OK, and figure out what
        // configurations should be created.
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges =
            computeConfigChanges(topicErrors, request.topics());

        // Try to create whatever topics are needed.
        Map<String, CreatableTopicResult> successes = new HashMap<>();
        for (CreatableTopic topic : request.topics()) {
            if (topicErrors.containsKey(topic.name())) continue;
            // Figure out what ConfigRecords should be created, if any.
            ConfigResource configResource = new ConfigResource(TOPIC, topic.name());
            Map<String, Entry<OpType, String>> keyToOps = configChanges.get(configResource);
            List<ApiMessageAndVersion> configRecords;
            if (keyToOps != null) {
                ControllerResult<ApiError> configResult =
                    configurationControl.incrementalAlterConfig(configResource, keyToOps, true);
                if (configResult.response().isFailure()) {
                    topicErrors.put(topic.name(), configResult.response());
                    continue;
                } else {
                    configRecords = configResult.records();
                }
            } else {
                configRecords = Collections.emptyList();
            }
            ApiError error;
            try {
                error = createTopic(context, topic, records, successes, configRecords, describable.contains(topic.name()));
            } catch (ApiException e) {
                error = ApiError.fromThrowable(e);
            }
            if (error.isFailure()) {
                topicErrors.put(topic.name(), error);
            }
        }

        // Create responses for all topics.
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        StringBuilder resultsBuilder = new StringBuilder();
        String resultsPrefix = "";
        for (CreatableTopic topic : request.topics()) {
            ApiError error = topicErrors.get(topic.name());
            if (error != null) {
                data.topics().add(new CreatableTopicResult().
                    setName(topic.name()).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
                resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                    append(error.error()).append(" (").append(error.message()).append(")");
                resultsPrefix = ", ";
                continue;
            }
            CreatableTopicResult result = successes.get(topic.name());
            data.topics().add(result);
            resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                append("SUCCESS");
            resultsPrefix = ", ";
        }
        if (request.validateOnly()) {
            log.info("Validate-only CreateTopics result(s): {}", resultsBuilder);
            return ControllerResult.atomicOf(Collections.emptyList(), data);
        } else {
            log.info("CreateTopics result(s): {}", resultsBuilder);
            return ControllerResult.atomicOf(records, data);
        }
    }

    private ApiError createTopic(ControllerRequestContext context,
                                 CreatableTopic topic,
                                 List<ApiMessageAndVersion> records,
                                 Map<String, CreatableTopicResult> successes,
                                 List<ApiMessageAndVersion> configRecords,
                                 boolean authorizedToReturnConfigs) {
        Map<String, String> creationConfigs = translateCreationConfigs(topic.configs());
        Map<Integer, PartitionRegistration> newParts = new HashMap<>();
        if (!topic.assignments().isEmpty()) {
            if (topic.replicationFactor() != -1) {
                return new ApiError(INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication " +
                    "factor was not set to -1.");
            }
            if (topic.numPartitions() != -1) {
                return new ApiError(INVALID_REQUEST,
                    "A manual partition assignment was specified, but numPartitions " +
                        "was not set to -1.");
            }
            OptionalInt replicationFactor = OptionalInt.empty();
            for (CreatableReplicaAssignment assignment : topic.assignments()) {
                if (newParts.containsKey(assignment.partitionIndex())) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                        "Found multiple manual partition assignments for partition " +
                            assignment.partitionIndex());
                }
                PartitionAssignment partitionAssignment = new PartitionAssignment(assignment.brokerIds(), clusterDescriber);
                validateManualPartitionAssignment(partitionAssignment, replicationFactor);
                replicationFactor = OptionalInt.of(assignment.brokerIds().size());
                List<Integer> isr = assignment.brokerIds().stream().
                    filter(clusterControl::isActive).collect(Collectors.toList());
                if (isr.isEmpty()) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                        "All brokers specified in the manual partition assignment for " +
                        "partition " + assignment.partitionIndex() + " are fenced or in controlled shutdown.");
                }
                newParts.put(
                    assignment.partitionIndex(),
                    buildPartitionRegistration(partitionAssignment, isr)
                );
            }
            for (int i = 0; i < newParts.size(); i++) {
                if (!newParts.containsKey(i)) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "partitions should be a consecutive 0-based integer sequence");
                }
            }
            ApiError error = maybeCheckCreateTopicPolicy(() -> {
                Map<Integer, List<Integer>> assignments = new HashMap<>();
                newParts.forEach((key, value) -> assignments.put(key, Replicas.toList(value.replicas)));
                return new CreateTopicPolicy.RequestMetadata(
                    topic.name(), null, null, assignments, creationConfigs);
            });
            if (error.isFailure()) return error;
        } else if (topic.replicationFactor() < -1 || topic.replicationFactor() == 0) {
            return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                "Replication factor must be larger than 0, or -1 to use the default value.");
        } else if (topic.numPartitions() < -1 || topic.numPartitions() == 0) {
            return new ApiError(Errors.INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.");
        } else {
            int numPartitions = topic.numPartitions() == -1 ?
                defaultNumPartitions : topic.numPartitions();
            short replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            try {
                TopicAssignment topicAssignment = clusterControl.replicaPlacer().place(new PlacementSpec(
                    0,
                    numPartitions,
                    replicationFactor
                ), clusterDescriber);
                for (int partitionId = 0; partitionId < topicAssignment.assignments().size(); partitionId++) {
                    PartitionAssignment partitionAssignment = topicAssignment.assignments().get(partitionId);
                    List<Integer> isr = partitionAssignment.replicas().stream().
                        filter(clusterControl::isActive).collect(Collectors.toList());
                    // If the ISR is empty, it means that all brokers are fenced or
                    // in controlled shutdown. To be consistent with the replica placer,
                    // we reject the create topic request with INVALID_REPLICATION_FACTOR.
                    if (isr.isEmpty()) {
                        return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                            "Unable to replicate the partition " + replicationFactor +
                                " time(s): All brokers are currently fenced or in controlled shutdown.");
                    }
                    newParts.put(
                        partitionId,
                        buildPartitionRegistration(partitionAssignment, isr)
                    );
                }
            } catch (InvalidReplicationFactorException e) {
                return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                    "Unable to replicate the partition " + replicationFactor +
                        " time(s): " + e.getMessage());
            }
            ApiError error = maybeCheckCreateTopicPolicy(() -> new CreateTopicPolicy.RequestMetadata(
                topic.name(), numPartitions, replicationFactor, null, creationConfigs));
            if (error.isFailure()) return error;
        }
        int numPartitions = newParts.size();
        try {
            context.applyPartitionChangeQuota(numPartitions); // check controller mutation quota
        } catch (ThrottlingQuotaExceededException e) {
            log.debug("Topic creation of {} partitions not allowed because quota is violated. Delay time: {}",
                numPartitions, e.throttleTimeMs());
            return ApiError.fromThrowable(e);
        }
        Uuid topicId = Uuid.randomUuid();
        CreatableTopicResult result = new CreatableTopicResult().
            setName(topic.name()).
            setTopicId(topicId).
            setErrorCode(NONE.code()).
            setErrorMessage(null);
        if (authorizedToReturnConfigs) {
            Map<String, ConfigEntry> effectiveConfig = configurationControl.
                computeEffectiveTopicConfigs(creationConfigs);
            List<String> configNames = new ArrayList<>(effectiveConfig.keySet());
            configNames.sort(String::compareTo);
            for (String configName : configNames) {
                ConfigEntry entry = effectiveConfig.get(configName);
                result.configs().add(new CreateTopicsResponseData.CreatableTopicConfigs().
                    setName(entry.name()).
                    setValue(entry.isSensitive() ? null : entry.value()).
                    setReadOnly(entry.isReadOnly()).
                    setConfigSource(KafkaConfigSchema.translateConfigSource(entry.source()).id()).
                    setIsSensitive(entry.isSensitive()));
            }
            result.setNumPartitions(numPartitions);
            result.setReplicationFactor((short) newParts.values().iterator().next().replicas.length);
            result.setTopicConfigErrorCode(NONE.code());
        } else {
            result.setTopicConfigErrorCode(TOPIC_AUTHORIZATION_FAILED.code());
        }
        successes.put(topic.name(), result);
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        // ConfigRecords go after TopicRecord but before PartitionRecord(s).
        records.addAll(configRecords);
        for (Entry<Integer, PartitionRegistration> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionRegistration info = partEntry.getValue();
            records.add(info.toRecord(topicId, partitionIndex, new ImageWriterOptions.Builder().
                    setMetadataVersion(featureControl.metadataVersion()).
                    build()));
        }
        return ApiError.NONE;
    }

    private static PartitionRegistration buildPartitionRegistration(
        PartitionAssignment partitionAssignment,
        List<Integer> isr
    ) {
        return new PartitionRegistration.Builder().
            setReplicas(Replicas.toArray(partitionAssignment.replicas())).
            setDirectories(Uuid.toArray(partitionAssignment.directories())).
            setIsr(Replicas.toArray(isr)).
            setLeader(isr.get(0)).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(0).
            setPartitionEpoch(0).
            build();
    }

    private ApiError maybeCheckCreateTopicPolicy(Supplier<CreateTopicPolicy.RequestMetadata> supplier) {
        if (createTopicPolicy.isPresent()) {
            try {
                createTopicPolicy.get().validate(supplier.get());
            } catch (PolicyViolationException e) {
                return new ApiError(Errors.POLICY_VIOLATION, e.getMessage());
            }
        }
        return ApiError.NONE;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics,
                                      Map<String, ? extends Set<String>> topicsWithCollisionChars) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (InvalidTopicException e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_TOPIC_EXCEPTION, e.getMessage()));
            }
            if (Topic.hasCollisionChars(topic.name())) {
                String normalizedName = Topic.unifyCollisionChars(topic.name());
                Set<String> colliding = topicsWithCollisionChars.get(normalizedName);
                if (colliding != null) {
                    topicErrors.put(topic.name(), new ApiError(Errors.INVALID_TOPIC_EXCEPTION,
                        "Topic '" + topic.name() + "' collides with existing topic: " +
                            colliding.iterator().next()));
                }
            }
        }
    }

    static Map<ConfigResource, Map<String, Entry<OpType, String>>>
            computeConfigChanges(Map<String, ApiError> topicErrors,
                                 CreatableTopicCollection topics) {
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges = new HashMap<>();
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            Map<String, Entry<OpType, String>> topicConfigs = new HashMap<>();
            List<String> nullConfigs = new ArrayList<>();
            for (CreateTopicsRequestData.CreateableTopicConfig config : topic.configs()) {
                if (config.value() == null) {
                    nullConfigs.add(config.name());
                } else {
                    topicConfigs.put(config.name(), new SimpleImmutableEntry<>(SET, config.value()));
                }
            }
            if (!nullConfigs.isEmpty()) {
                topicErrors.put(topic.name(), new ApiError(Errors.INVALID_CONFIG,
                    "Null value not supported for topic configs: " + String.join(",", nullConfigs)));
            } else if (!topicConfigs.isEmpty()) {
                configChanges.put(new ConfigResource(TOPIC, topic.name()), topicConfigs);
            }
        }
        return configChanges;
    }

    Map<String, ResultOrError<Uuid>> findTopicIds(long offset, Collection<String> names) {
        Map<String, ResultOrError<Uuid>> results = new HashMap<>(names.size());
        for (String name : names) {
            if (name == null) {
                results.put(null, new ResultOrError<>(INVALID_REQUEST, "Invalid null topic name."));
            } else {
                Uuid id = topicsByName.get(name, offset);
                if (id == null) {
                    results.put(name, new ResultOrError<>(
                        new ApiError(UNKNOWN_TOPIC_OR_PARTITION)));
                } else {
                    results.put(name, new ResultOrError<>(id));
                }
            }
        }
        return results;
    }

    Map<String, Uuid> findAllTopicIds(long offset) {
        HashMap<String, Uuid> result = new HashMap<>(topicsByName.size(offset));
        for (Entry<String, Uuid> entry : topicsByName.entrySet(offset)) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    Map<Uuid, ResultOrError<String>> findTopicNames(long offset, Collection<Uuid> ids) {
        Map<Uuid, ResultOrError<String>> results = new HashMap<>(ids.size());
        for (Uuid id : ids) {
            if (id == null || id.equals(Uuid.ZERO_UUID)) {
                results.put(id, new ResultOrError<>(new ApiError(INVALID_REQUEST,
                    "Attempt to find topic with invalid topicId " + id)));
            } else {
                TopicControlInfo topic = topics.get(id, offset);
                if (topic == null) {
                    results.put(id, new ResultOrError<>(new ApiError(UNKNOWN_TOPIC_ID)));
                } else {
                    results.put(id, new ResultOrError<>(topic.name));
                }
            }
        }
        return results;
    }

    ControllerResult<Map<Uuid, ApiError>> deleteTopics(ControllerRequestContext context, Collection<Uuid> ids) {
        Map<Uuid, ApiError> results = new HashMap<>(ids.size());
        List<ApiMessageAndVersion> records =
                BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP, ids.size());
        for (Uuid id : ids) {
            try {
                deleteTopic(context, id, records);
                results.put(id, ApiError.NONE);
            } catch (ApiException e) {
                results.put(id, ApiError.fromThrowable(e));
            } catch (Exception e) {
                log.error("Unexpected deleteTopics error for {}", id, e);
                results.put(id, ApiError.fromThrowable(e));
            }
        }
        return ControllerResult.atomicOf(records, results);
    }

    void deleteTopic(ControllerRequestContext context, Uuid id, List<ApiMessageAndVersion> records) {
        TopicControlInfo topic = topics.get(id);
        if (topic == null) {
            throw new UnknownTopicIdException(UNKNOWN_TOPIC_ID.message());
        }
        int numPartitions = topic.parts.size();
        try {
            context.applyPartitionChangeQuota(numPartitions); // check controller mutation quota
        } catch (ThrottlingQuotaExceededException e) {
            // log a message and rethrow the exception
            log.debug("Topic deletion of {} partitions not allowed because quota is violated. Delay time: {}",
                numPartitions, e.throttleTimeMs());
            throw e;
        }
        records.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(id), (short) 0));
    }

    // VisibleForTesting
    PartitionRegistration getPartition(Uuid topicId, int partitionId) {
        TopicControlInfo topic = topics.get(topicId);
        if (topic == null) {
            return null;
        }
        return topic.parts.get(partitionId);
    }

    // VisibleForTesting
    TopicControlInfo getTopic(Uuid topicId) {
        return topics.get(topicId);
    }

    Uuid getTopicId(String name) {
        return topicsByName.get(name);
    }

    // VisibleForTesting
    BrokersToIsrs brokersToIsrs() {
        return brokersToIsrs;
    }

    // VisibleForTesting
    BrokersToElrs brokersToElrs() {
        return brokersToElrs;
    }

    // VisibleForTesting
    TimelineHashSet<TopicIdPartition> imbalancedPartitions() {
        return imbalancedPartitions;
    }

    boolean isElrEnabled() {
        return eligibleLeaderReplicasEnabled && featureControl.metadataVersion().isElrSupported();
    }

    ControllerResult<AlterPartitionResponseData> alterPartition(
        ControllerRequestContext context,
        AlterPartitionRequestData request
    ) {
        short requestVersion = context.requestHeader().requestApiVersion();
        clusterControl.checkBrokerEpoch(request.brokerId(), request.brokerEpoch());
        AlterPartitionResponseData response = new AlterPartitionResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (AlterPartitionRequestData.TopicData topicData : request.topics()) {
            AlterPartitionResponseData.TopicData responseTopicData =
                new AlterPartitionResponseData.TopicData().
                    setTopicName(topicData.topicName()).
                    setTopicId(topicData.topicId());
            response.topics().add(responseTopicData);

            Uuid topicId = requestVersion > 1 ? topicData.topicId() : topicsByName.get(topicData.topicName());
            if (topicId == null || topicId.equals(Uuid.ZERO_UUID) || !topics.containsKey(topicId)) {
                Errors error = requestVersion > 1 ? UNKNOWN_TOPIC_ID : UNKNOWN_TOPIC_OR_PARTITION;
                for (AlterPartitionRequestData.PartitionData partitionData : topicData.partitions()) {
                    responseTopicData.partitions().add(new AlterPartitionResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(error.code()));
                }
                log.info("Rejecting AlterPartition request for unknown topic ID {} or name {}.",
                    topicData.topicId(), topicData.topicName());
                continue;
            }

            TopicControlInfo topic = topics.get(topicId);
            for (AlterPartitionRequestData.PartitionData partitionData : topicData.partitions()) {
                if (requestVersion < 3) {
                    partitionData.setNewIsrWithEpochs(
                        AlterPartitionRequest.newIsrToSimpleNewIsrWithBrokerEpochs(partitionData.newIsr())
                    );
                }

                int partitionId = partitionData.partitionIndex();
                PartitionRegistration partition = topic.parts.get(partitionId);

                Errors validationError = validateAlterPartitionData(
                    request.brokerId(),
                    topic,
                    partitionId,
                    partition,
                    context.requestHeader().requestApiVersion(),
                    partitionData);

                if (validationError != Errors.NONE) {
                    responseTopicData.partitions().add(
                        new AlterPartitionResponseData.PartitionData()
                            .setPartitionIndex(partitionId)
                            .setErrorCode(validationError.code())
                    );

                    continue;
                }

                PartitionChangeBuilder builder = new PartitionChangeBuilder(
                    partition,
                    topic.id,
                    partitionId,
                    new LeaderAcceptor(clusterControl, partition),
                    featureControl.metadataVersion(),
                    getTopicEffectiveMinIsr(topic.name)
                )
                    .setZkMigrationEnabled(clusterControl.zkRegistrationAllowed())
                    .setEligibleLeaderReplicasEnabled(isElrEnabled());
                if (configurationControl.uncleanLeaderElectionEnabledForTopic(topic.name())) {
                    builder.setElection(PartitionChangeBuilder.Election.UNCLEAN);
                }
                Optional<ApiMessageAndVersion> record = builder
                    .setTargetIsrWithBrokerStates(partitionData.newIsrWithEpochs())
                    .setTargetLeaderRecoveryState(LeaderRecoveryState.of(partitionData.leaderRecoveryState()))
                    .setDefaultDirProvider(clusterDescriber)
                    .build();
                if (record.isPresent()) {
                    records.add(record.get());
                    PartitionChangeRecord change = (PartitionChangeRecord) record.get().message();
                    partition = partition.merge(change);
                    if (log.isDebugEnabled()) {
                        log.debug("Node {} has altered ISR for {}-{} to {}.",
                            request.brokerId(), topic.name, partitionId, change.isr());
                    }
                    if (change.leader() != request.brokerId() &&
                            change.leader() != NO_LEADER_CHANGE) {
                        // Normally, an AlterPartition request, which is made by the partition
                        // leader itself, is not allowed to modify the partition leader.
                        // However, if there is an ongoing partition reassignment and the
                        // ISR change completes it, then the leader may change as part of
                        // the changes made during reassignment cleanup.
                        //
                        // In this case, we report back NEW_LEADER_ELECTED to the leader
                        // which made the AlterPartition request. This lets it know that it must
                        // fetch new metadata before trying again. This return code is
                        // unusual because we both return an error and generate a new
                        // metadata record. We usually only do one or the other.
                        // FENCED_LEADER_EPOCH is used for request version below or equal to 1.
                        Errors error = requestVersion > 1 ? NEW_LEADER_ELECTED : FENCED_LEADER_EPOCH;
                        log.info("AlterPartition request from node {} for {}-{} completed " +
                            "the ongoing partition reassignment and triggered a " +
                            "leadership change. Returning {}.",
                            request.brokerId(), topic.name, partitionId, error);
                        responseTopicData.partitions().add(new AlterPartitionResponseData.PartitionData().
                            setPartitionIndex(partitionId).
                            setErrorCode(error.code()));
                        continue;
                    } else if (isReassignmentInProgress(partition)) {
                        log.info("AlterPartition request from node {} for {}-{} completed " +
                            "the ongoing partition reassignment.", request.brokerId(),
                            topic.name, partitionId);
                    }
                }

                /* Setting the LeaderRecoveryState field is always safe because it will always be the
                 * same as the value set in the request. For version 0, that is always the default
                 * RECOVERED which is ignored when serializing to version 0. For any other version, the
                 * LeaderRecoveryState field is supported.
                 */
                responseTopicData.partitions().add(new AlterPartitionResponseData.PartitionData().
                    setPartitionIndex(partitionId).
                    setErrorCode(Errors.NONE.code()).
                    setLeaderId(partition.leader).
                    setIsr(Replicas.toList(partition.isr)).
                    setLeaderRecoveryState(partition.leaderRecoveryState.value()).
                    setLeaderEpoch(partition.leaderEpoch).
                    setPartitionEpoch(partition.partitionEpoch));
            }
        }

        return ControllerResult.of(records, response);
    }

    /**
     * Validate the partition information included in the alter partition request.
     *
     * @param brokerId id of the broker requesting the alter partition
     * @param topic current topic information store by the replication manager
     * @param partitionId partition id being altered
     * @param partition current partition registration for the partition being altered
     * @param partitionData partition data from the alter partition request
     *
     * @return Errors.NONE for valid alter partition data; otherwise the validation error
     */
    private Errors validateAlterPartitionData(
        int brokerId,
        TopicControlInfo topic,
        int partitionId,
        PartitionRegistration partition,
        short requestApiVersion,
        AlterPartitionRequestData.PartitionData partitionData
    ) {
        if (partition == null) {
            log.info("Rejecting AlterPartition request for unknown partition {}-{}.",
                    topic.name, partitionId);

            return UNKNOWN_TOPIC_OR_PARTITION;
        }

        // If the partition leader has a higher leader/partition epoch, then it is likely
        // that this node is no longer the active controller. We return NOT_CONTROLLER in
        // this case to give the leader an opportunity to find the new controller.
        if (partitionData.leaderEpoch() > partition.leaderEpoch) {
            log.debug("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the current leader epoch is {}, which is greater than the local value {}.",
                brokerId, topic.name, partitionId, partition.leaderEpoch, partitionData.leaderEpoch());
            return NOT_CONTROLLER;
        }
        if (partitionData.partitionEpoch() > partition.partitionEpoch) {
            log.debug("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the current partition epoch is {}, which is greater than the local value {}.",
                brokerId, topic.name, partitionId, partition.partitionEpoch, partitionData.partitionEpoch());
            return NOT_CONTROLLER;
        }
        if (partitionData.leaderEpoch() < partition.leaderEpoch) {
            log.debug("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the current leader epoch is {}, not {}.", brokerId, topic.name,
                    partitionId, partition.leaderEpoch, partitionData.leaderEpoch());

            return FENCED_LEADER_EPOCH;
        }
        if (brokerId != partition.leader) {
            log.info("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the current leader is {}.", brokerId, topic.name,
                    partitionId, partition.leader);

            return INVALID_REQUEST;
        }
        if (partitionData.partitionEpoch() < partition.partitionEpoch) {
            log.info("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the current partition epoch is {}, not {}.", brokerId,
                    topic.name, partitionId, partition.partitionEpoch,
                    partitionData.partitionEpoch());

            return INVALID_UPDATE_VERSION;
        }

        int[] newIsr = partitionData.newIsrWithEpochs().stream()
            .mapToInt(brokerState -> brokerState.brokerId()).toArray();

        if (!Replicas.validateIsr(partition.replicas, newIsr)) {
            log.error("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "it specified an invalid ISR {}.", brokerId,
                    topic.name, partitionId, partitionData.newIsrWithEpochs());

            return INVALID_REQUEST;
        }
        if (!Replicas.contains(newIsr, partition.leader)) {
            // The ISR must always include the current leader.
            log.error("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "it specified an invalid ISR {} that doesn't include itself.",
                    brokerId, topic.name, partitionId, partitionData.newIsrWithEpochs());

            return INVALID_REQUEST;
        }
        LeaderRecoveryState leaderRecoveryState = LeaderRecoveryState.of(partitionData.leaderRecoveryState());
        if (leaderRecoveryState == LeaderRecoveryState.RECOVERING && newIsr.length > 1) {
            log.info("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the ISR {} had more than one replica while the leader was still " +
                    "recovering from an unclean leader election {}.",
                    brokerId, topic.name, partitionId, partitionData.newIsrWithEpochs(),
                    leaderRecoveryState);

            return INVALID_REQUEST;
        }
        if (partition.leaderRecoveryState == LeaderRecoveryState.RECOVERED &&
                leaderRecoveryState == LeaderRecoveryState.RECOVERING) {
            log.info("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "the leader recovery state cannot change from RECOVERED to RECOVERING.",
                    brokerId, topic.name, partitionId);

            return INVALID_REQUEST;
        }

        List<IneligibleReplica> ineligibleReplicas = ineligibleReplicasForIsr(partitionData.newIsrWithEpochs());
        if (!ineligibleReplicas.isEmpty()) {
            log.info("Rejecting AlterPartition request from node {} for {}-{} because " +
                    "it specified ineligible replicas {} in the new ISR {}.",
                    brokerId, topic.name, partitionId, ineligibleReplicas, partitionData.newIsrWithEpochs());

            if (requestApiVersion > 1) {
                return INELIGIBLE_REPLICA;
            } else {
                return OPERATION_NOT_ATTEMPTED;
            }
        }

        return Errors.NONE;
    }

    private List<IneligibleReplica> ineligibleReplicasForIsr(List<BrokerState> brokerStates) {
        List<IneligibleReplica> ineligibleReplicas = new ArrayList<>(0);
        for (BrokerState brokerState : brokerStates) {
            int brokerId = brokerState.brokerId();
            BrokerRegistration registration = clusterControl.registration(brokerId);
            if (registration == null) {
                ineligibleReplicas.add(new IneligibleReplica(brokerId, "not registered"));
            } else if (registration.inControlledShutdown()) {
                ineligibleReplicas.add(new IneligibleReplica(brokerId, "shutting down"));
            } else if (registration.fenced()) {
                ineligibleReplicas.add(new IneligibleReplica(brokerId, "fenced"));
            } else if (brokerState.brokerEpoch() != -1 && registration.epoch() != brokerState.brokerEpoch()) {
                // The given broker epoch should match with the broker epoch in the broker registration, except the
                // given broker epoch is -1 which means skipping the broker epoch verification.
                ineligibleReplicas.add(new IneligibleReplica(brokerId,
                    "broker epoch mismatch: requested=" + brokerState.brokerEpoch()
                        + " VS expected=" + registration.epoch()));
            }
        }
        return ineligibleReplicas;
    }

    /**
     * Generate the appropriate records to handle a broker being fenced.
     *
     * First, we remove this broker from any ISR. Then we generate a
     * FenceBrokerRecord.
     *
     * @param brokerId      The broker id.
     * @param records       The record list to append to.
     */
    void handleBrokerFenced(int brokerId, List<ApiMessageAndVersion> records) {
        BrokerRegistration brokerRegistration = clusterControl.brokerRegistrations().get(brokerId);
        if (brokerRegistration == null) {
            throw new RuntimeException("Can't find broker registration for broker " + brokerId);
        }
        generateLeaderAndIsrUpdates("handleBrokerFenced", brokerId, NO_LEADER, NO_LEADER, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        if (featureControl.metadataVersion().isBrokerRegistrationChangeRecordSupported()) {
            records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                    setBrokerId(brokerId).setBrokerEpoch(brokerRegistration.epoch()).
                    setFenced(BrokerRegistrationFencingChange.FENCE.value()),
                    (short) 0));
        } else {
            records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
                    setId(brokerId).setEpoch(brokerRegistration.epoch()),
                    (short) 0));
        }
    }

    /**
     * Generate the appropriate records to handle a broker being unregistered.
     *
     * First, we remove this broker from any ISR or ELR. Then we generate an
     * UnregisterBrokerRecord.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnregistered(int brokerId, long brokerEpoch,
                                  List<ApiMessageAndVersion> records) {
        generateLeaderAndIsrUpdates("handleBrokerUnregistered", brokerId, NO_LEADER, NO_LEADER, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        generateLeaderAndIsrUpdates("handleBrokerUnregistered", brokerId, NO_LEADER, NO_LEADER, records,
            brokersToElrs.partitionsWithBrokerInElr(brokerId));
        records.add(new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(brokerId).setBrokerEpoch(brokerEpoch),
            (short) 0));
    }

    /**
     * Generate the appropriate records to handle a broker becoming unfenced.
     *
     * First, we create an UnfenceBrokerRecord. Then, we check if there are any
     * partitions that don't currently have a leader that should be led by the newly
     * unfenced broker.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnfenced(int brokerId, long brokerEpoch, List<ApiMessageAndVersion> records) {
        if (featureControl.metadataVersion().isBrokerRegistrationChangeRecordSupported()) {
            records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).
                setFenced(BrokerRegistrationFencingChange.UNFENCE.value()),
                (short) 0));
        } else {
            records.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().setId(brokerId).
                setEpoch(brokerEpoch), (short) 0));
        }
        generateLeaderAndIsrUpdates("handleBrokerUnfenced", NO_LEADER, brokerId, NO_LEADER, records,
            brokersToIsrs.partitionsWithNoLeader());
    }

    /**
     * Generate the appropriate records to handle a broker starting a controlled shutdown.
     *
     * First, we create an BrokerRegistrationChangeRecord. Then, we remove this broker
     * from any ISR and elect new leaders for partitions led by this
     * broker.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerInControlledShutdown(int brokerId, long brokerEpoch, List<ApiMessageAndVersion> records) {
        if (featureControl.metadataVersion().isInControlledShutdownStateSupported()
                && !clusterControl.inControlledShutdown(brokerId)) {
            records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).
                setInControlledShutdown(BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value()),
                (short) 1));
        }
        generateLeaderAndIsrUpdates("enterControlledShutdown[" + brokerId + "]",
            brokerId, NO_LEADER, NO_LEADER, records, brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
    }

    /**
     * Create partition change records to remove replicas from any ISR or ELR for brokers doing unclean shutdown.
     *
     * @param brokerId      The broker id.
     * @param records       The record list to append to.
     */
    void handleBrokerUncleanShutdown(int brokerId, List<ApiMessageAndVersion> records) {
        if (!featureControl.metadataVersion().isElrSupported()) return;
        generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", NO_LEADER, NO_LEADER, brokerId, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        generateLeaderAndIsrUpdates("handleBrokerUncleanShutdown", NO_LEADER, NO_LEADER, brokerId, records,
            brokersToElrs.partitionsWithBrokerInElr(brokerId));
    }

    /**
     * Generates the appropriate records to handle a list of directories being reported offline.
     *
     * If the reported directories include directories that were previously online, this includes
     * a BrokerRegistrationChangeRecord and any number of PartitionChangeRecord to update
     * leadership and ISR for partitions in those directories that were previously online.
     *
     * @param brokerId    The broker id.
     * @param brokerEpoch The broker epoch.
     * @param offlineDirs The list of directories that are offline.
     * @param records     The record list to append to.
     */
    void handleDirectoriesOffline(
        int brokerId,
        long brokerEpoch,
        List<Uuid> offlineDirs,
        List<ApiMessageAndVersion> records
    ) {
        BrokerRegistration registration = clusterControl.registration(brokerId);
        List<Uuid> newOfflineDirs = registration.directoryIntersection(offlineDirs);
        if (!newOfflineDirs.isEmpty()) {
            for (Uuid newOfflineDir : newOfflineDirs) {
                TimelineHashSet<TopicIdPartition> parts = directoriesToPartitions.get(newOfflineDir);
                Iterator<TopicIdPartition> iterator = (parts == null) ?
                        Collections.emptyIterator() : parts.iterator();
                generateLeaderAndIsrUpdates(
                        "handleDirectoriesOffline[" + brokerId + ":" + newOfflineDir + "]",
                        brokerId, NO_LEADER, NO_LEADER, records, iterator);
            }
            List<Uuid> newOnlineDirs = registration.directoryDifference(offlineDirs);
            records.add(new ApiMessageAndVersion(new BrokerRegistrationChangeRecord().
                    setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).
                    setLogDirs(newOnlineDirs),
                    (short) 2));
            log.warn("Directories {} in broker {} marked offline, remaining directories: {}",
                    newOfflineDirs, brokerId, newOnlineDirs);
        }
    }

    ControllerResult<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
        ElectionType electionType = electionType(request.electionType());
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        ElectLeadersResponseData response = new ElectLeadersResponseData();
        if (request.topicPartitions() == null) {
            // If topicPartitions is null, we try to elect a new leader for every partition.  There
            // are some obvious issues with this wire protocol.  For example, what if we have too
            // many partitions to fit the results in a single RPC?  This behavior should probably be
            // removed from the protocol.  For now, however, we have to implement this for
            // compatibility with the old controller.
            for (Entry<String, Uuid> topicEntry : topicsByName.entrySet()) {
                String topicName = topicEntry.getKey();
                ReplicaElectionResult topicResults =
                    new ReplicaElectionResult().setTopic(topicName);
                response.replicaElectionResults().add(topicResults);
                TopicControlInfo topic = topics.get(topicEntry.getValue());
                if (topic != null) {
                    for (int partitionId : topic.parts.keySet()) {
                        ApiError error = electLeader(topicName, partitionId, electionType, records);

                        // When electing leaders for all partitions, we do not return
                        // partitions which already have the desired leader.
                        if (error.error() != Errors.ELECTION_NOT_NEEDED) {
                            topicResults.partitionResult().add(new PartitionResult().
                                setPartitionId(partitionId).
                                setErrorCode(error.error().code()).
                                setErrorMessage(error.message()));
                        }
                    }
                }
            }
        } else {
            for (TopicPartitions topic : request.topicPartitions()) {
                ReplicaElectionResult topicResults =
                    new ReplicaElectionResult().setTopic(topic.topic());
                response.replicaElectionResults().add(topicResults);
                for (int partitionId : topic.partitions()) {
                    ApiError error = electLeader(topic.topic(), partitionId, electionType, records);
                    topicResults.partitionResult().add(new PartitionResult().
                        setPartitionId(partitionId).
                        setErrorCode(error.error().code()).
                        setErrorMessage(error.message()));
                }
            }
        }
        return ControllerResult.of(records, response);
    }

    private static ElectionType electionType(byte electionType) {
        try {
            return ElectionType.valueOf(electionType);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Unknown election type " + (int) electionType);
        }
    }

    ApiError electLeader(String topic, int partitionId, ElectionType electionType,
                         List<ApiMessageAndVersion> records) {
        Uuid topicId = topicsByName.get(topic);
        if (topicId == null) {
            return new ApiError(UNKNOWN_TOPIC_OR_PARTITION,
                "No such topic as " + topic);
        }
        TopicControlInfo topicInfo = topics.get(topicId);
        if (topicInfo == null) {
            return new ApiError(UNKNOWN_TOPIC_OR_PARTITION,
                "No such topic id as " + topicId);
        }
        PartitionRegistration partition = topicInfo.parts.get(partitionId);
        if (partition == null) {
            return new ApiError(UNKNOWN_TOPIC_OR_PARTITION,
                "No such partition as " + topic + "-" + partitionId);
        }
        if ((electionType == ElectionType.PREFERRED && partition.hasPreferredLeader())
            || (electionType == ElectionType.UNCLEAN && partition.hasLeader())) {
            return new ApiError(Errors.ELECTION_NOT_NEEDED);
        }

        PartitionChangeBuilder.Election election = PartitionChangeBuilder.Election.PREFERRED;
        if (electionType == ElectionType.UNCLEAN) {
            election = PartitionChangeBuilder.Election.UNCLEAN;
        }
        Optional<ApiMessageAndVersion> record = new PartitionChangeBuilder(
            partition,
            topicId,
            partitionId,
            new LeaderAcceptor(clusterControl, partition),
            featureControl.metadataVersion(),
            getTopicEffectiveMinIsr(topic)
        )
            .setElection(election)
            .setZkMigrationEnabled(clusterControl.zkRegistrationAllowed())
            .setEligibleLeaderReplicasEnabled(isElrEnabled())
            .setDefaultDirProvider(clusterDescriber)
            .build();
        if (!record.isPresent()) {
            if (electionType == ElectionType.PREFERRED) {
                return new ApiError(Errors.PREFERRED_LEADER_NOT_AVAILABLE);
            } else {
                return new ApiError(Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE);
            }
        }
        records.add(record.get());
        return ApiError.NONE;
    }

    ControllerResult<BrokerHeartbeatReply> processBrokerHeartbeat(
        BrokerHeartbeatRequestData request,
        long registerBrokerRecordOffset
    ) {
        int brokerId = request.brokerId();
        long brokerEpoch = request.brokerEpoch();
        clusterControl.checkBrokerEpoch(brokerId, brokerEpoch);
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        BrokerControlStates states = heartbeatManager.calculateNextBrokerState(brokerId,
            request, registerBrokerRecordOffset, () -> brokersToIsrs.hasLeaderships(brokerId));
        List<ApiMessageAndVersion> records = new ArrayList<>();
        if (states.current() != states.next()) {
            switch (states.next()) {
                case FENCED:
                    handleBrokerFenced(brokerId, records);
                    break;
                case UNFENCED:
                    handleBrokerUnfenced(brokerId, brokerEpoch, records);
                    break;
                case CONTROLLED_SHUTDOWN:
                    handleBrokerInControlledShutdown(brokerId, brokerEpoch, records);
                    break;
                case SHUTDOWN_NOW:
                    handleBrokerFenced(brokerId, records);
                    break;
            }
        }
        heartbeatManager.touch(brokerId,
            states.next().fenced(),
            request.currentMetadataOffset());
        if (featureControl.metadataVersion().isDirectoryAssignmentSupported()) {
            handleDirectoriesOffline(brokerId, brokerEpoch, request.offlineLogDirs(), records);
        }
        boolean isCaughtUp = request.currentMetadataOffset() >= registerBrokerRecordOffset;
        BrokerHeartbeatReply reply = new BrokerHeartbeatReply(isCaughtUp,
                states.next().fenced(),
                states.next().inControlledShutdown(),
                states.next().shouldShutDown());
        return ControllerResult.of(records, reply);
    }

    /**
     * Process a broker heartbeat which has been sitting on the queue for too long, and has
     * expired. With default settings, this would happen after 1 second. We process expired
     * heartbeats by updating the lastSeenNs of the broker, so that the broker won't get fenced
     * incorrectly. However, we don't perform any state changes that we normally would, such as
     * unfencing a fenced broker, etc.
     */
    void processExpiredBrokerHeartbeat(BrokerHeartbeatRequestData request) {
        int brokerId = request.brokerId();
        clusterControl.checkBrokerEpoch(brokerId, request.brokerEpoch());
        clusterControl.heartbeatManager().touch(brokerId,
                clusterControl.brokerRegistrations().get(brokerId).fenced(),
                request.currentMetadataOffset());
        log.error("processExpiredBrokerHeartbeat: controller event queue overloaded. Timed out " +
                "heartbeat from broker {}.", brokerId);
    }

    public ControllerResult<Void> unregisterBroker(int brokerId) {
        BrokerRegistration registration = clusterControl.brokerRegistrations().get(brokerId);
        if (registration == null) {
            throw new BrokerIdNotRegisteredException("Broker ID " + brokerId +
                " is not currently registered");
        }
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        handleBrokerUnregistered(brokerId, registration.epoch(), records);
        return ControllerResult.of(records, null);
    }

    ControllerResult<Void> maybeFenceOneStaleBroker() {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        heartbeatManager.findOneStaleBroker().ifPresent(brokerId -> {
            // Even though multiple brokers can go stale at a time, we will process
            // fencing one at a time so that the effect of fencing each broker is visible
            // to the system prior to processing the next one
            log.info("Fencing broker {} because its session has timed out.", brokerId);
            handleBrokerFenced(brokerId, records);
            heartbeatManager.fence(brokerId);
        });
        return ControllerResult.of(records, null);
    }

    boolean arePartitionLeadersImbalanced() {
        return !imbalancedPartitions.isEmpty();
    }

    /**
     * Attempt to elect a preferred leader for all topic partitions which have a leader that is not the preferred replica.
     *
     * The response() method in the return object is true if this method returned without electing all possible preferred replicas.
     * The quorum controller should reschedule this operation immediately if it is true.
     *
     * @return All of the election records and if there may be more available preferred replicas to elect as leader
     */
    ControllerResult<Boolean> maybeBalancePartitionLeaders() {
        List<ApiMessageAndVersion> records = new ArrayList<>();

        boolean rescheduleImmediately = false;
        for (TopicIdPartition topicPartition : imbalancedPartitions) {
            if (records.size() >= maxElectionsPerImbalance) {
                rescheduleImmediately = true;
                break;
            }

            TopicControlInfo topic = topics.get(topicPartition.topicId());
            if (topic == null) {
                log.error("Skipping unknown imbalanced topic {}", topicPartition);
                continue;
            }

            PartitionRegistration partition = topic.parts.get(topicPartition.partitionId());
            if (partition == null) {
                log.error("Skipping unknown imbalanced partition {}", topicPartition);
                continue;
            }

            // Attempt to perform a preferred leader election
            new PartitionChangeBuilder(
                partition,
                topicPartition.topicId(),
                topicPartition.partitionId(),
                new LeaderAcceptor(clusterControl, partition),
                featureControl.metadataVersion(),
                getTopicEffectiveMinIsr(topic.name)
            )
                .setElection(PartitionChangeBuilder.Election.PREFERRED)
                .setZkMigrationEnabled(clusterControl.zkRegistrationAllowed())
                .setEligibleLeaderReplicasEnabled(isElrEnabled())
                .setDefaultDirProvider(clusterDescriber)
                .build().ifPresent(records::add);
        }

        return ControllerResult.of(records, rescheduleImmediately);
    }

    ControllerResult<List<CreatePartitionsTopicResult>> createPartitions(
        ControllerRequestContext context,
        List<CreatePartitionsTopic> topics
    ) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        List<CreatePartitionsTopicResult> results = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        for (CreatePartitionsTopic topic : topics) {
            ApiError apiError = ApiError.NONE;
            try {
                createPartitions(context, topic, records);
            } catch (ApiException e) {
                apiError = ApiError.fromThrowable(e);
            } catch (Exception e) {
                log.error("Unexpected createPartitions error for {}", topic, e);
                apiError = ApiError.fromThrowable(e);
            }
            results.add(new CreatePartitionsTopicResult().
                setName(topic.name()).
                setErrorCode(apiError.error().code()).
                setErrorMessage(apiError.message()));
        }
        return ControllerResult.atomicOf(records, results);
    }

    void createPartitions(ControllerRequestContext context,
                          CreatePartitionsTopic topic,
                          List<ApiMessageAndVersion> records) {
        Uuid topicId = topicsByName.get(topic.name());
        if (topicId == null) {
            throw new UnknownTopicOrPartitionException();
        }
        TopicControlInfo topicInfo = topics.get(topicId);
        if (topicInfo == null) {
            throw new UnknownTopicOrPartitionException();
        }
        if (topic.count() == topicInfo.parts.size()) {
            throw new InvalidPartitionsException("Topic already has " +
                topicInfo.parts.size() + " partition(s).");
        } else if (topic.count() < topicInfo.parts.size()) {
            throw new InvalidPartitionsException("The topic " + topic.name() + " currently " +
                "has " + topicInfo.parts.size() + " partition(s); " + topic.count() +
                " would not be an increase.");
        }
        int additional = topic.count() - topicInfo.parts.size();
        if (topic.assignments() != null) {
            if (topic.assignments().size() != additional) {
                throw new InvalidReplicaAssignmentException("Attempted to add " + additional +
                    " additional partition(s), but only " + topic.assignments().size() +
                    " assignment(s) were specified.");
            }
        }
        try {
            context.applyPartitionChangeQuota(additional); // check controller mutation quota
        } catch (ThrottlingQuotaExceededException e) {
            // log a message and rethrow the exception
            log.debug("Partition creation of {} partitions not allowed because quota is violated. Delay time: {}",
                additional, e.throttleTimeMs());
            throw e;
        }
        Iterator<PartitionRegistration> iterator = topicInfo.parts.values().iterator();
        if (!iterator.hasNext()) {
            throw new UnknownServerException("Invalid state: topic " + topic.name() +
                " appears to have no partitions.");
        }
        PartitionRegistration partitionInfo = iterator.next();
        if (partitionInfo.replicas.length > Short.MAX_VALUE) {
            throw new UnknownServerException("Invalid replication factor " +
                partitionInfo.replicas.length + ": expected a number equal to less than " +
                Short.MAX_VALUE);
        }
        short replicationFactor = (short) partitionInfo.replicas.length;
        int startPartitionId = topicInfo.parts.size();

        List<PartitionAssignment> partitionAssignments;
        List<List<Integer>> isrs;
        if (topic.assignments() != null) {
            partitionAssignments = new ArrayList<>();
            isrs = new ArrayList<>();
            for (int i = 0; i < topic.assignments().size(); i++) {
                List<Integer> replicas = topic.assignments().get(i).brokerIds();
                PartitionAssignment partitionAssignment = new PartitionAssignment(replicas, clusterDescriber);
                validateManualPartitionAssignment(partitionAssignment, OptionalInt.of(replicationFactor));
                partitionAssignments.add(partitionAssignment);
                List<Integer> isr = partitionAssignment.replicas().stream().
                    filter(clusterControl::isActive).collect(Collectors.toList());
                if (isr.isEmpty()) {
                    throw new InvalidReplicaAssignmentException(
                        "All brokers specified in the manual partition assignment for " +
                            "partition " + (startPartitionId + i) + " are fenced or in controlled shutdown.");
                }
                isrs.add(isr);
            }
        } else {
            partitionAssignments = clusterControl.replicaPlacer().place(
                new PlacementSpec(startPartitionId, additional, replicationFactor),
                clusterDescriber
            ).assignments();
            isrs = partitionAssignments.stream().map(PartitionAssignment::replicas).collect(Collectors.toList());
        }
        int partitionId = startPartitionId;
        for (int i = 0; i < partitionAssignments.size(); i++) {
            PartitionAssignment partitionAssignment = partitionAssignments.get(i);
            List<Integer> isr = isrs.get(i).stream().
                filter(clusterControl::isActive).collect(Collectors.toList());
            // If the ISR is empty, it means that all brokers are fenced or
            // in controlled shutdown. To be consistent with the replica placer,
            // we reject the create topic request with INVALID_REPLICATION_FACTOR.
            if (isr.isEmpty()) {
                throw new InvalidReplicationFactorException(
                    "Unable to replicate the partition " + replicationFactor +
                        " time(s): All brokers are currently fenced or in controlled shutdown.");
            }
            records.add(buildPartitionRegistration(partitionAssignment, isr)
                .toRecord(topicId, partitionId, new ImageWriterOptions.Builder().
                        setMetadataVersion(featureControl.metadataVersion()).
                        build()));
            partitionId++;
        }
    }

    void validateManualPartitionAssignment(
        PartitionAssignment assignment,
        OptionalInt replicationFactor
    ) {
        if (assignment.replicas().isEmpty()) {
            throw new InvalidReplicaAssignmentException("The manual partition " +
                "assignment includes an empty replica list.");
        }
        List<Integer> sortedBrokerIds = new ArrayList<>(assignment.replicas());
        sortedBrokerIds.sort(Integer::compare);
        Integer prevBrokerId = null;
        for (Integer brokerId : sortedBrokerIds) {
            if (!clusterControl.brokerRegistrations().containsKey(brokerId)) {
                throw new InvalidReplicaAssignmentException("The manual partition " +
                    "assignment includes broker " + brokerId + ", but no such broker is " +
                    "registered.");
            }
            if (brokerId.equals(prevBrokerId)) {
                throw new InvalidReplicaAssignmentException("The manual partition " +
                    "assignment includes the broker " + prevBrokerId + " more than " +
                    "once.");
            }
            prevBrokerId = brokerId;
        }
        if (replicationFactor.isPresent() &&
                sortedBrokerIds.size() != replicationFactor.getAsInt()) {
            throw new InvalidReplicaAssignmentException("The manual partition " +
                "assignment includes a partition with " + sortedBrokerIds.size() +
                " replica(s), but this is not consistent with previous " +
                "partitions, which have " + replicationFactor.getAsInt() + " replica(s).");
        }
    }

    /**
     * Iterate over a sequence of partitions and generate ISR/ELR changes and/or leader
     * changes if necessary.
     *
     * @param context           A human-readable context string used in log4j logging.
     * @param brokerToRemove    NO_LEADER if no broker is being removed; the ID of the
     *                          broker to remove from the ISR and leadership, otherwise.
     * @param brokerToAdd       NO_LEADER if no broker is being added; the ID of the
     *                          broker which is now eligible to be a leader, otherwise.
     * @param brokerWithUncleanShutdown
     *                          NO_LEADER if no broker has unclean shutdown; the ID of the
     *                          broker which is now removed from the ISR, ELR and
     *                          leadership, otherwise.
     * @param records           A list of records which we will append to.
     * @param iterator          The iterator containing the partitions to examine.
     */
    void generateLeaderAndIsrUpdates(String context,
                                     int brokerToRemove,
                                     int brokerToAdd,
                                     int brokerWithUncleanShutdown,
                                     List<ApiMessageAndVersion> records,
                                     Iterator<TopicIdPartition> iterator) {
        int oldSize = records.size();

        // If the caller passed a valid broker ID for brokerToAdd, rather than passing
        // NO_LEADER, that node will be considered an acceptable leader even if it is
        // currently fenced. This is useful when handling unfencing. The reason is that
        // while we're generating the records to handle unfencing, the ClusterControlManager
        // still shows the node as fenced.
        //
        // Similarly, if the caller passed a valid broker ID for brokerToRemove, rather
        // than passing NO_LEADER, that node will never be considered an acceptable leader.
        // This is useful when handling a newly fenced node. We also exclude brokerToRemove
        // from the target ISR, but we need to exclude it here too, to handle the case
        // where there is an unclean leader election which chooses a leader from outside
        // the ISR.
        //
        // If the caller passed a valid broker ID for brokerWithUncleanShutdown, rather than
        // passing NO_LEADER, this node should not be an acceptable leader. We also exclude
        // brokerWithUncleanShutdown from ELR and ISR.
        IntPredicate isAcceptableLeader =
            r -> (r != brokerToRemove && r != brokerWithUncleanShutdown)
                && (r == brokerToAdd || clusterControl.isActive(r));

        while (iterator.hasNext()) {
            TopicIdPartition topicIdPart = iterator.next();
            TopicControlInfo topic = topics.get(topicIdPart.topicId());
            if (topic == null) {
                throw new RuntimeException("Topic ID " + topicIdPart.topicId() +
                    " existed in isrMembers, but not in the topics map.");
            }
            PartitionRegistration partition = topic.parts.get(topicIdPart.partitionId());
            if (partition == null) {
                throw new RuntimeException("Partition " + topicIdPart +
                    " existed in isrMembers, but not in the partitions map.");
            }
            PartitionChangeBuilder builder = new PartitionChangeBuilder(
                partition,
                topicIdPart.topicId(),
                topicIdPart.partitionId(),
                new LeaderAcceptor(clusterControl, partition, isAcceptableLeader),
                featureControl.metadataVersion(),
                getTopicEffectiveMinIsr(topic.name)
            );
            builder.setZkMigrationEnabled(clusterControl.zkRegistrationAllowed());
            builder.setEligibleLeaderReplicasEnabled(isElrEnabled());
            if (configurationControl.uncleanLeaderElectionEnabledForTopic(topic.name)) {
                builder.setElection(PartitionChangeBuilder.Election.UNCLEAN);
            }
            if (brokerWithUncleanShutdown != NO_LEADER) {
                builder.setUncleanShutdownReplicas(Arrays.asList(brokerWithUncleanShutdown));
            }

            // Note: if brokerToRemove and brokerWithUncleanShutdown were passed as NO_LEADER, this is a no-op (the new
            // target ISR will be the same as the old one).
            builder.setTargetIsr(Replicas.toList(
                Replicas.copyWithout(partition.isr, new int[] {brokerToRemove, brokerWithUncleanShutdown})));

            builder.setDefaultDirProvider(clusterDescriber)
                    .build().ifPresent(records::add);
        }
        if (records.size() != oldSize) {
            if (log.isDebugEnabled()) {
                StringBuilder bld = new StringBuilder();
                String prefix = "";
                for (ListIterator<ApiMessageAndVersion> iter = records.listIterator(oldSize);
                     iter.hasNext(); ) {
                    ApiMessageAndVersion apiMessageAndVersion = iter.next();
                    PartitionChangeRecord record = (PartitionChangeRecord) apiMessageAndVersion.message();
                    bld.append(prefix).append(topics.get(record.topicId()).name).append("-").
                        append(record.partitionId());
                    prefix = ", ";
                }
                log.debug("{}: changing partition(s): {}", context, bld);
            } else if (log.isInfoEnabled()) {
                log.info("{}: changing {} partition(s)", context, records.size() - oldSize);
            }
        }
    }

    ControllerResult<AlterPartitionReassignmentsResponseData>
            alterPartitionReassignments(AlterPartitionReassignmentsRequestData request) {
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        AlterPartitionReassignmentsResponseData result =
                new AlterPartitionReassignmentsResponseData().setErrorMessage(null);
        int successfulAlterations = 0, totalAlterations = 0;
        for (ReassignableTopic topic : request.topics()) {
            ReassignableTopicResponse topicResponse = new ReassignableTopicResponse().
                setName(topic.name());
            for (ReassignablePartition partition : topic.partitions()) {
                ApiError error = ApiError.NONE;
                try {
                    alterPartitionReassignment(topic.name(), partition, records);
                    successfulAlterations++;
                } catch (Throwable e) {
                    log.info("Unable to alter partition reassignment for " +
                        topic.name() + ":" + partition.partitionIndex() + " because " +
                        "of an " + e.getClass().getSimpleName() + " error: " + e.getMessage());
                    error = ApiError.fromThrowable(e);
                }
                totalAlterations++;
                topicResponse.partitions().add(new ReassignablePartitionResponse().
                    setPartitionIndex(partition.partitionIndex()).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
            }
            result.responses().add(topicResponse);
        }
        log.info("Successfully altered {} out of {} partition reassignment(s).",
            successfulAlterations, totalAlterations);
        return ControllerResult.atomicOf(records, result);
    }

    void alterPartitionReassignment(String topicName,
                                    ReassignablePartition target,
                                    List<ApiMessageAndVersion> records) {
        Uuid topicId = topicsByName.get(topicName);
        if (topicId == null) {
            throw new UnknownTopicOrPartitionException("Unable to find a topic " +
                "named " + topicName + ".");
        }
        TopicControlInfo topicInfo = topics.get(topicId);
        if (topicInfo == null) {
            throw new UnknownTopicOrPartitionException("Unable to find a topic " +
                "with ID " + topicId + ".");
        }
        TopicIdPartition tp = new TopicIdPartition(topicId, target.partitionIndex());
        PartitionRegistration part = topicInfo.parts.get(target.partitionIndex());
        if (part == null) {
            throw new UnknownTopicOrPartitionException("Unable to find partition " +
                topicName + ":" + target.partitionIndex() + ".");
        }
        Optional<ApiMessageAndVersion> record;
        if (target.replicas() == null) {
            record = cancelPartitionReassignment(topicName, tp, part);
        } else {
            record = changePartitionReassignment(tp, part, target);
        }
        record.ifPresent(records::add);
    }

    Optional<ApiMessageAndVersion> cancelPartitionReassignment(String topicName,
                                                               TopicIdPartition tp,
                                                               PartitionRegistration part) {
        if (!isReassignmentInProgress(part)) {
            throw new NoReassignmentInProgressException(NO_REASSIGNMENT_IN_PROGRESS.message());
        }
        PartitionReassignmentRevert revert = new PartitionReassignmentRevert(part);
        if (revert.unclean()) {
            if (!configurationControl.uncleanLeaderElectionEnabledForTopic(topicName)) {
                throw new InvalidReplicaAssignmentException("Unable to revert partition " +
                    "assignment for " + topicName + ":" + tp.partitionId() + " because " +
                    "it would require an unclean leader election.");
            }
        }
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            part,
            tp.topicId(),
            tp.partitionId(),
            new LeaderAcceptor(clusterControl, part),
            featureControl.metadataVersion(),
            getTopicEffectiveMinIsr(topicName)
        );
        builder.setZkMigrationEnabled(clusterControl.zkRegistrationAllowed());
        builder.setEligibleLeaderReplicasEnabled(isElrEnabled());
        if (configurationControl.uncleanLeaderElectionEnabledForTopic(topicName)) {
            builder.setElection(PartitionChangeBuilder.Election.UNCLEAN);
        }
        return builder
            .setTargetIsr(revert.isr()).
            setTargetReplicas(revert.replicas()).
            setTargetRemoving(Collections.emptyList()).
            setTargetAdding(Collections.emptyList()).
            setDefaultDirProvider(clusterDescriber).
            build();
    }

    /**
     * Apply a given partition reassignment. In general a partition reassignment goes
     * through several stages:
     *
     * 1. Issue a PartitionChangeRecord adding all the new replicas to the partition's
     * main replica list, and setting removingReplicas and addingReplicas.
     *
     * 2. Wait for the partition to have an ISR that contains all the new replicas. Or
     * if there are no new replicas, wait until we have an ISR that contains at least one
     * replica that we are not removing.
     *
     * 3. Issue a second PartitionChangeRecord removing all removingReplicas from the
     * partitions' main replica list, and clearing removingReplicas and addingReplicas.
     *
     * After stage 3, the reassignment is done.
     *
     * Under some conditions, steps #1 and #2 can be skipped entirely since the ISR is
     * already suitable to progress to stage #3. For example, a partition reassignment
     * that merely rearranges existing replicas in the list can bypass step #1 and #2 and
     * complete immediately.
     *
     * @param tp                The topic id and partition id.
     * @param part              The existing partition info.
     * @param target            The target partition info.
     *
     * @return                  The ChangePartitionRecord for the new partition assignment,
     *                          or empty if no change is needed.
     */
    Optional<ApiMessageAndVersion> changePartitionReassignment(TopicIdPartition tp,
                                                               PartitionRegistration part,
                                                               ReassignablePartition target) {
        // Check that the requested partition assignment is valid.
        PartitionAssignment currentAssignment = new PartitionAssignment(Replicas.toList(part.replicas), part::directory);
        PartitionAssignment targetAssignment = new PartitionAssignment(target.replicas(), clusterDescriber);

        validateManualPartitionAssignment(targetAssignment, OptionalInt.empty());

        List<Integer> currentReplicas = Replicas.toList(part.replicas);
        PartitionReassignmentReplicas reassignment =
            new PartitionReassignmentReplicas(currentAssignment, targetAssignment);
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            part,
            tp.topicId(),
            tp.partitionId(),
            new LeaderAcceptor(clusterControl, part),
            featureControl.metadataVersion(),
            getTopicEffectiveMinIsr(topics.get(tp.topicId()).name.toString())
        );
        builder.setZkMigrationEnabled(clusterControl.zkRegistrationAllowed());
        builder.setEligibleLeaderReplicasEnabled(isElrEnabled());
        if (!reassignment.replicas().equals(currentReplicas)) {
            builder.setTargetReplicas(reassignment.replicas());
        }
        if (!reassignment.removing().isEmpty()) {
            builder.setTargetRemoving(reassignment.removing());
        }
        if (!reassignment.adding().isEmpty()) {
            builder.setTargetAdding(reassignment.adding());
        }
        return builder.setDefaultDirProvider(clusterDescriber).build();
    }

    ListPartitionReassignmentsResponseData listPartitionReassignments(
        List<ListPartitionReassignmentsTopics> topicList,
        long epoch
    ) {
        ListPartitionReassignmentsResponseData response =
            new ListPartitionReassignmentsResponseData().setErrorMessage(null);
        if (topicList == null) {
            // List all reassigning topics.
            for (Entry<Uuid, int[]> entry : reassigningTopics.entrySet(epoch)) {
                listReassigningTopic(response, entry.getKey(), Replicas.toList(entry.getValue()));
            }
        } else {
            // List the given topics.
            for (ListPartitionReassignmentsTopics topic : topicList) {
                Uuid topicId = topicsByName.get(topic.name(), epoch);
                if (topicId != null) {
                    listReassigningTopic(response, topicId, topic.partitionIndexes());
                }
            }
        }
        return response;
    }

    ControllerResult<AssignReplicasToDirsResponseData> handleAssignReplicasToDirs(AssignReplicasToDirsRequestData request) {
        if (!featureControl.metadataVersion().isDirectoryAssignmentSupported()) {
            throw new UnsupportedVersionException("Directory assignment is not supported yet.");
        }
        int brokerId = request.brokerId();
        clusterControl.checkBrokerEpoch(brokerId, request.brokerEpoch());
        BrokerRegistration brokerRegistration = clusterControl.brokerRegistrations().get(brokerId);
        if (brokerRegistration == null) {
            throw new BrokerIdNotRegisteredException("Broker ID " + brokerId + " is not currently registered");
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        AssignReplicasToDirsResponseData response = new AssignReplicasToDirsResponseData();
        Set<TopicIdPartition> leaderAndIsrUpdates = new HashSet<>();
        for (AssignReplicasToDirsRequestData.DirectoryData reqDir : request.directories()) {
            Uuid dirId = reqDir.id();
            boolean directoryIsOffline = !brokerRegistration.hasOnlineDir(dirId);
            AssignReplicasToDirsResponseData.DirectoryData resDir = new AssignReplicasToDirsResponseData.DirectoryData().setId(dirId);
            for (AssignReplicasToDirsRequestData.TopicData reqTopic : reqDir.topics()) {
                Uuid topicId = reqTopic.topicId();
                Errors topicError = Errors.NONE;
                TopicControlInfo topicInfo = this.topics.get(topicId);
                if (topicInfo == null) {
                    log.warn("AssignReplicasToDirsRequest from broker {} references unknown topic ID {}", brokerId, topicId);
                    topicError = Errors.UNKNOWN_TOPIC_ID;
                }
                AssignReplicasToDirsResponseData.TopicData resTopic = new AssignReplicasToDirsResponseData.TopicData().setTopicId(topicId);
                for (AssignReplicasToDirsRequestData.PartitionData reqPartition : reqTopic.partitions()) {
                    int partitionIndex = reqPartition.partitionIndex();
                    Errors partitionError = topicError;
                    if (topicError == Errors.NONE) {
                        String topicName = topicInfo.name;
                        PartitionRegistration partitionRegistration = topicInfo.parts.get(partitionIndex);
                        if (partitionRegistration == null) {
                            log.warn("AssignReplicasToDirsRequest from broker {} references unknown partition {}-{}", brokerId, topicName, partitionIndex);
                            partitionError = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                        } else if (!Replicas.contains(partitionRegistration.replicas, brokerId)) {
                            log.warn("AssignReplicasToDirsRequest from broker {} references non assigned partition {}-{}", brokerId, topicName, partitionIndex);
                            partitionError = Errors.NOT_LEADER_OR_FOLLOWER;
                        } else {
                            Optional<ApiMessageAndVersion> partitionChangeRecord = new PartitionChangeBuilder(
                                    partitionRegistration,
                                    topicId,
                                    partitionIndex,
                                    new LeaderAcceptor(clusterControl, partitionRegistration),
                                    featureControl.metadataVersion(),
                                    getTopicEffectiveMinIsr(topicName)
                            )
                                    .setDirectory(brokerId, dirId)
                                    .setDefaultDirProvider(clusterDescriber)
                                    .build();
                            partitionChangeRecord.ifPresent(records::add);
                            if (directoryIsOffline) {
                                leaderAndIsrUpdates.add(new TopicIdPartition(topicId, partitionIndex));
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("Broker {} assigned partition {}:{} to {} dir {}",
                                    brokerId, topics.get(topicId).name(), partitionIndex,
                                    directoryIsOffline ? "OFFLINE" : "ONLINE", dirId);
                            }
                        }
                    }
                    resTopic.partitions().add(new AssignReplicasToDirsResponseData.PartitionData().
                            setPartitionIndex(partitionIndex).
                            setErrorCode(partitionError.code()));
                }
                resDir.topics().add(resTopic);
            }
            response.directories().add(resDir);
        }
        if (!leaderAndIsrUpdates.isEmpty()) {
            generateLeaderAndIsrUpdates("offline-dir-assignment", brokerId, NO_LEADER, NO_LEADER, records, leaderAndIsrUpdates.iterator());
        }
        return ControllerResult.of(records, response);
    }

    private void listReassigningTopic(ListPartitionReassignmentsResponseData response,
                                      Uuid topicId,
                                      List<Integer> partitionIds) {
        TopicControlInfo topicInfo = topics.get(topicId);
        if (topicInfo == null) return;
        OngoingTopicReassignment ongoingTopic = new OngoingTopicReassignment().
            setName(topicInfo.name);
        for (int partitionId : partitionIds) {
            Optional<OngoingPartitionReassignment> ongoing =
                getOngoingPartitionReassignment(topicInfo, partitionId);
            if (ongoing.isPresent()) {
                ongoingTopic.partitions().add(ongoing.get());
            }
        }
        if (!ongoingTopic.partitions().isEmpty()) {
            response.topics().add(ongoingTopic);
        }
    }

    private Optional<OngoingPartitionReassignment>
            getOngoingPartitionReassignment(TopicControlInfo topicInfo, int partitionId) {
        PartitionRegistration partition = topicInfo.parts.get(partitionId);
        if (partition == null || !isReassignmentInProgress(partition)) {
            return Optional.empty();
        }
        return Optional.of(new OngoingPartitionReassignment().
            setAddingReplicas(Replicas.toList(partition.addingReplicas)).
            setRemovingReplicas(Replicas.toList(partition.removingReplicas)).
            setPartitionIndex(partitionId).
            setReplicas(Replicas.toList(partition.replicas)));
    }

    // Visible to test.
    int getTopicEffectiveMinIsr(String topicName) {
        int currentMinIsr = defaultMinIsr;
        String minIsrConfig = configurationControl.getTopicConfig(topicName, MIN_IN_SYNC_REPLICAS_CONFIG);
        if (minIsrConfig != null) {
            currentMinIsr = Integer.parseInt(minIsrConfig);
        } else {
            log.debug("Can't find the min isr config for topic: " + topicName + ". Use default value " + defaultMinIsr);
        }
        
        Uuid topicId = topicsByName.get(topicName);
        int replicationFactor = topics.get(topicId).parts.get(0).replicas.length;
        return Math.min(currentMinIsr, replicationFactor);
    }

    /**
     * Updates the directory to partition mapping for a single partition.
     * Assignments to reserved directory IDs are ignored, since they cannot
     * be used for directories, there's no use in maintaining a set of
     * partitions assigned to them.
     */
    private void updatePartitionDirectories(
        Uuid topicId,
        int partitionId,
        Uuid[] previousDirectoryIds,
        Uuid[] newDirectoryIds
    ) {
        Objects.requireNonNull(topicId, "topicId cannot be null");
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, partitionId);
        if (previousDirectoryIds != null) {
            for (Uuid dir : previousDirectoryIds) {
                if (!DirectoryId.reserved(dir)) {
                    TimelineHashSet<TopicIdPartition> partitions = directoriesToPartitions.get(dir);
                    if (partitions != null) {
                        partitions.remove(topicIdPartition);
                        if (partitions.isEmpty()) {
                            directoriesToPartitions.remove(dir);
                        }
                    }
                }
            }
        }
        if (newDirectoryIds != null) {
            for (Uuid dir : newDirectoryIds) {
                if (!DirectoryId.reserved(dir)) {
                    Set<TopicIdPartition> partitions = directoriesToPartitions.computeIfAbsent(dir,
                        __ -> new TimelineHashSet<>(snapshotRegistry, 0));
                    partitions.add(topicIdPartition);
                }
            }
        }
    }

    private void updatePartitionInfo(
        Uuid topicId,
        Integer partitionId,
        PartitionRegistration prevPartInfo,
        PartitionRegistration newPartInfo
    ) {
        HashSet<Integer> validationSet = new HashSet<>();
        Arrays.stream(newPartInfo.isr).forEach(validationSet::add);
        Arrays.stream(newPartInfo.elr).forEach(validationSet::add);
        if (validationSet.size() != newPartInfo.isr.length + newPartInfo.elr.length) {
            log.error("{}-{} has overlapping ISR={} and ELR={}", topics.get(topicId).name, partitionId,
                Arrays.toString(newPartInfo.isr), Arrays.toString(newPartInfo.elr));
        }
        brokersToIsrs.update(topicId, partitionId, prevPartInfo == null ? null : prevPartInfo.isr,
            newPartInfo.isr, prevPartInfo == null ? NO_LEADER : prevPartInfo.leader, newPartInfo.leader);
        brokersToElrs.update(topicId, partitionId, prevPartInfo == null ? null : prevPartInfo.elr,
            newPartInfo.elr);
    }

    private static final class IneligibleReplica {
        private final int replicaId;
        private final String reason;

        private IneligibleReplica(int replicaId, String reason) {
            this.replicaId = replicaId;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return replicaId + " (" + reason + ")";
        }
    }

    private static final class LeaderAcceptor implements IntPredicate {
        private final ClusterControlManager clusterControl;
        private final PartitionRegistration partition;
        private final IntPredicate isAcceptableLeader;

        private LeaderAcceptor(ClusterControlManager clusterControl, PartitionRegistration partition) {
            this(clusterControl, partition, clusterControl::isActive);
        }

        private LeaderAcceptor(ClusterControlManager clusterControl, PartitionRegistration partition, IntPredicate isAcceptableLeader) {
            this.clusterControl = clusterControl;
            this.partition = partition;
            this.isAcceptableLeader = isAcceptableLeader;
        }

        @Override
        public boolean test(int brokerId) {
            if (!isAcceptableLeader.test(brokerId)) {
                return false;
            }
            Uuid replicaDirectory = partition.directory(brokerId);
            return clusterControl.hasOnlineDir(brokerId, replicaDirectory);
        }
    }
}
