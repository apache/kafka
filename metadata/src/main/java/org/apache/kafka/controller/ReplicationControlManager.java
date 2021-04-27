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
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.BrokersToIsrs.TopicIdPartition;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalInt;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.protocol.Errors.INVALID_REQUEST;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;


/**
 * The ReplicationControlManager is the part of the controller which deals with topics
 * and partitions. It is responsible for managing the in-sync replica set and leader
 * of each partition, as well as administrative tasks like creating or deleting topics.
 */
public class ReplicationControlManager {
    /**
     * A special value used to represent the leader for a partition with no leader. 
     */
    public static final int NO_LEADER = -1;

    /**
     * A special value used to represent a PartitionChangeRecord that does not change the
     * partition leader.
     */
    public static final int NO_LEADER_CHANGE = -2;

    static class TopicControlInfo {
        private final String name;
        private final Uuid id;
        private final TimelineHashMap<Integer, PartitionControlInfo> parts;

        TopicControlInfo(String name, SnapshotRegistry snapshotRegistry, Uuid id) {
            this.name = name;
            this.id = id;
            this.parts = new TimelineHashMap<>(snapshotRegistry, 0);
        }
    }

    static class PartitionControlInfo {
        public final int[] replicas;
        public final int[] isr;
        public final int[] removingReplicas;
        public final int[] addingReplicas;
        public final int leader;
        public final int leaderEpoch;
        public final int partitionEpoch;

        PartitionControlInfo(PartitionRecord record) {
            this(Replicas.toArray(record.replicas()),
                Replicas.toArray(record.isr()),
                Replicas.toArray(record.removingReplicas()),
                Replicas.toArray(record.addingReplicas()),
                record.leader(),
                record.leaderEpoch(),
                record.partitionEpoch());
        }

        PartitionControlInfo(int[] replicas, int[] isr, int[] removingReplicas,
                             int[] addingReplicas, int leader, int leaderEpoch,
                             int partitionEpoch) {
            this.replicas = replicas;
            this.isr = isr;
            this.removingReplicas = removingReplicas;
            this.addingReplicas = addingReplicas;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.partitionEpoch = partitionEpoch;
        }

        PartitionControlInfo merge(PartitionChangeRecord record) {
            int[] newIsr = (record.isr() == null) ? isr : Replicas.toArray(record.isr());
            int newLeader;
            int newLeaderEpoch;
            if (record.leader() == NO_LEADER_CHANGE) {
                newLeader = leader;
                newLeaderEpoch = leaderEpoch;
            } else {
                newLeader = record.leader();
                newLeaderEpoch = leaderEpoch + 1;
            }
            return new PartitionControlInfo(replicas,
                newIsr,
                removingReplicas,
                addingReplicas,
                newLeader,
                newLeaderEpoch,
                partitionEpoch + 1);
        }

        String diff(PartitionControlInfo prev) {
            StringBuilder builder = new StringBuilder();
            String prefix = "";
            if (!Arrays.equals(replicas, prev.replicas)) {
                builder.append(prefix).append("replicas: ").
                    append(Arrays.toString(prev.replicas)).
                    append(" -> ").append(Arrays.toString(replicas));
                prefix = ", ";
            }
            if (!Arrays.equals(isr, prev.isr)) {
                builder.append(prefix).append("isr: ").
                    append(Arrays.toString(prev.isr)).
                    append(" -> ").append(Arrays.toString(isr));
                prefix = ", ";
            }
            if (!Arrays.equals(removingReplicas, prev.removingReplicas)) {
                builder.append(prefix).append("removingReplicas: ").
                    append(Arrays.toString(prev.removingReplicas)).
                    append(" -> ").append(Arrays.toString(removingReplicas));
                prefix = ", ";
            }
            if (!Arrays.equals(addingReplicas, prev.addingReplicas)) {
                builder.append(prefix).append("addingReplicas: ").
                    append(Arrays.toString(prev.addingReplicas)).
                    append(" -> ").append(Arrays.toString(addingReplicas));
                prefix = ", ";
            }
            if (leader != prev.leader) {
                builder.append(prefix).append("leader: ").
                    append(prev.leader).append(" -> ").append(leader);
                prefix = ", ";
            }
            if (leaderEpoch != prev.leaderEpoch) {
                builder.append(prefix).append("leaderEpoch: ").
                    append(prev.leaderEpoch).append(" -> ").append(leaderEpoch);
                prefix = ", ";
            }
            if (partitionEpoch != prev.partitionEpoch) {
                builder.append(prefix).append("partitionEpoch: ").
                    append(prev.partitionEpoch).append(" -> ").append(partitionEpoch);
            }
            return builder.toString();
        }

        void maybeLogPartitionChange(Logger log, String description, PartitionControlInfo prev) {
            if (!electionWasClean(leader, prev.isr)) {
                log.info("UNCLEAN partition change for {}: {}", description, diff(prev));
            } else if (log.isDebugEnabled()) {
                log.debug("partition change for {}: {}", description, diff(prev));
            }
        }

        boolean hasLeader() {
            return leader != NO_LEADER;
        }

        int preferredReplica() {
            return replicas.length == 0 ? NO_LEADER : replicas[0];
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isr, removingReplicas, addingReplicas, leader,
                leaderEpoch, partitionEpoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionControlInfo)) return false;
            PartitionControlInfo other = (PartitionControlInfo) o;
            return Arrays.equals(replicas, other.replicas) &&
                Arrays.equals(isr, other.isr) &&
                Arrays.equals(removingReplicas, other.removingReplicas) &&
                Arrays.equals(addingReplicas, other.addingReplicas) &&
                leader == other.leader &&
                leaderEpoch == other.leaderEpoch &&
                partitionEpoch == other.partitionEpoch;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("PartitionControlInfo(");
            builder.append("replicas=").append(Arrays.toString(replicas));
            builder.append(", isr=").append(Arrays.toString(isr));
            builder.append(", removingReplicas=").append(Arrays.toString(removingReplicas));
            builder.append(", addingReplicas=").append(Arrays.toString(addingReplicas));
            builder.append(", leader=").append(leader);
            builder.append(", leaderEpoch=").append(leaderEpoch);
            builder.append(", partitionEpoch=").append(partitionEpoch);
            builder.append(")");
            return builder.toString();
        }
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
     * A reference to the controller's configuration control manager.
     */
    private final ConfigurationControlManager configurationControl;

    /**
     * A reference to the controller's cluster control manager.
     */
    private final ClusterControlManager clusterControl;

    /**
     * Maps topic names to topic UUIDs.
     */
    private final TimelineHashMap<String, Uuid> topicsByName;

    /**
     * Maps topic UUIDs to structures containing topic information, including partitions.
     */
    private final TimelineHashMap<Uuid, TopicControlInfo> topics;

    /**
     * A map of broker IDs to the partitions that the broker is in the ISR for.
     */
    private final BrokersToIsrs brokersToIsrs;

    ReplicationControlManager(SnapshotRegistry snapshotRegistry,
                              LogContext logContext,
                              short defaultReplicationFactor,
                              int defaultNumPartitions,
                              ConfigurationControlManager configurationControl,
                              ClusterControlManager clusterControl) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(ReplicationControlManager.class);
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.defaultNumPartitions = defaultNumPartitions;
        this.configurationControl = configurationControl;
        this.clusterControl = clusterControl;
        this.topicsByName = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.brokersToIsrs = new BrokersToIsrs(snapshotRegistry);
    }

    public void replay(TopicRecord record) {
        topicsByName.put(record.name(), record.topicId());
        topics.put(record.topicId(),
            new TopicControlInfo(record.name(), snapshotRegistry, record.topicId()));
        log.info("Created topic {} with topic ID {}.", record.name(), record.topicId());
    }

    public void replay(PartitionRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo newPartInfo = new PartitionControlInfo(record);
        PartitionControlInfo prevPartInfo = topicInfo.parts.get(record.partitionId());
        String description = topicInfo.name + "-" + record.partitionId() +
            " with topic ID " + record.topicId();
        if (prevPartInfo == null) {
            log.info("Created partition {} and {}.", description, newPartInfo);
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            brokersToIsrs.update(record.topicId(), record.partitionId(), null,
                newPartInfo.isr, NO_LEADER, newPartInfo.leader);
        } else if (!newPartInfo.equals(prevPartInfo)) {
            newPartInfo.maybeLogPartitionChange(log, description, prevPartInfo);
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            brokersToIsrs.update(record.topicId(), record.partitionId(), prevPartInfo.isr,
                newPartInfo.isr, prevPartInfo.leader, newPartInfo.leader);
        }
    }

    public void replay(PartitionChangeRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo prevPartitionInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartitionInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no partition with that id was found.");
        }
        PartitionControlInfo newPartitionInfo = prevPartitionInfo.merge(record);
        topicInfo.parts.put(record.partitionId(), newPartitionInfo);
        brokersToIsrs.update(record.topicId(), record.partitionId(),
            prevPartitionInfo.isr, newPartitionInfo.isr, prevPartitionInfo.leader,
            newPartitionInfo.leader);
        String topicPart = topicInfo.name + "-" + record.partitionId() + " with topic ID " +
            record.topicId();
        newPartitionInfo.maybeLogPartitionChange(log, topicPart, prevPartitionInfo);
    }

    public void replay(RemoveTopicRecord record) {
        // Remove this topic from the topics map and the topicsByName map.
        TopicControlInfo topic = topics.remove(record.topicId());
        if (topic == null) {
            throw new UnknownTopicIdException("Can't find topic with ID " + record.topicId() +
                " to remove.");
        }
        topicsByName.remove(topic.name);

        // Delete the configurations associated with this topic.
        configurationControl.deleteTopicConfigs(topic.name);

        // Remove the entries for this topic in brokersToIsrs.
        for (PartitionControlInfo partition : topic.parts.values()) {
            for (int i = 0; i < partition.isr.length; i++) {
                brokersToIsrs.removeTopicEntryForBroker(topic.id, partition.isr[i]);
            }
        }
        brokersToIsrs.removeTopicEntryForBroker(topic.id, NO_LEADER);

        log.info("Removed topic {} with ID {}.", topic.name, record.topicId());
    }

    ControllerResult<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check the topic names.
        validateNewTopicNames(topicErrors, request.topics());

        // Identify topics that already exist and mark them with the appropriate error
        request.topics().stream().filter(creatableTopic -> topicsByName.containsKey(creatableTopic.name()))
                .forEach(t -> topicErrors.put(t.name(), new ApiError(Errors.TOPIC_ALREADY_EXISTS)));

        // Verify that the configurations for the new topics are OK, and figure out what
        // ConfigRecords should be created.
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges =
            computeConfigChanges(topicErrors, request.topics());
        ControllerResult<Map<ConfigResource, ApiError>> configResult =
            configurationControl.incrementalAlterConfigs(configChanges);
        for (Entry<ConfigResource, ApiError> entry : configResult.response().entrySet()) {
            if (entry.getValue().isFailure()) {
                topicErrors.put(entry.getKey().name(), entry.getValue());
            }
        }
        records.addAll(configResult.records());

        // Try to create whatever topics are needed.
        Map<String, CreatableTopicResult> successes = new HashMap<>();
        for (CreatableTopic topic : request.topics()) {
            if (topicErrors.containsKey(topic.name())) continue;
            ApiError error = createTopic(topic, records, successes);
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
        log.info("createTopics result(s): {}", resultsBuilder.toString());
        return ControllerResult.atomicOf(records, data);
    }

    private ApiError createTopic(CreatableTopic topic,
                                 List<ApiMessageAndVersion> records,
                                 Map<String, CreatableTopicResult> successes) {
        Map<Integer, PartitionControlInfo> newParts = new HashMap<>();
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
                validateManualPartitionAssignment(assignment.brokerIds(), replicationFactor);
                replicationFactor = OptionalInt.of(assignment.brokerIds().size());
                int[] replicas = Replicas.toArray(assignment.brokerIds());
                newParts.put(assignment.partitionIndex(), new PartitionControlInfo(
                    replicas, replicas, null, null, replicas[0], 0, 0));
            }
        } else if (topic.replicationFactor() < -1 || topic.replicationFactor() == 0) {
            return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                "Replication factor was set to an invalid non-positive value.");
        } else if (!topic.assignments().isEmpty()) {
            return new ApiError(INVALID_REQUEST,
                "Replication factor was not set to -1 but a manual partition " +
                    "assignment was specified.");
        } else if (topic.numPartitions() < -1 || topic.numPartitions() == 0) {
            return new ApiError(Errors.INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.");
        } else {
            int numPartitions = topic.numPartitions() == -1 ?
                defaultNumPartitions : topic.numPartitions();
            short replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            try {
                List<List<Integer>> replicas = clusterControl.
                    placeReplicas(0, numPartitions, replicationFactor);
                for (int partitionId = 0; partitionId < replicas.size(); partitionId++) {
                    int[] r = Replicas.toArray(replicas.get(partitionId));
                    newParts.put(partitionId,
                        new PartitionControlInfo(r, r, null, null, r[0], 0, 0));
                }
            } catch (InvalidReplicationFactorException e) {
                return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                    "Unable to replicate the partition " + replicationFactor +
                        " times: " + e.getMessage());
            }
        }
        Uuid topicId = Uuid.randomUuid();
        successes.put(topic.name(), new CreatableTopicResult().
            setName(topic.name()).
            setTopicId(topicId).
            setErrorCode((short) 0).
            setErrorMessage(null).
            setNumPartitions(newParts.size()).
            setReplicationFactor((short) newParts.get(0).replicas.length));
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        for (Entry<Integer, PartitionControlInfo> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionControlInfo info = partEntry.getValue();
            records.add(new ApiMessageAndVersion(new PartitionRecord().
                setPartitionId(partitionIndex).
                setTopicId(topicId).
                setReplicas(Replicas.toList(info.replicas)).
                setIsr(Replicas.toList(info.isr)).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(info.leader).
                setLeaderEpoch(info.leaderEpoch).
                setPartitionEpoch(0), (short) 0));
        }
        return ApiError.NONE;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (InvalidTopicException e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_TOPIC_EXCEPTION, e.getMessage()));
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
            for (CreateTopicsRequestData.CreateableTopicConfig config : topic.configs()) {
                topicConfigs.put(config.name(), new SimpleImmutableEntry<>(SET, config.value()));
            }
            if (!topicConfigs.isEmpty()) {
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

    ControllerResult<Map<Uuid, ApiError>> deleteTopics(Collection<Uuid> ids) {
        Map<Uuid, ApiError> results = new HashMap<>(ids.size());
        List<ApiMessageAndVersion> records = new ArrayList<>(ids.size());
        for (Uuid id : ids) {
            try {
                deleteTopic(id, records);
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

    void deleteTopic(Uuid id, List<ApiMessageAndVersion> records) {
        TopicControlInfo topic = topics.get(id);
        if (topic == null) {
            throw new UnknownTopicIdException(UNKNOWN_TOPIC_ID.message());
        }
        records.add(new ApiMessageAndVersion(new RemoveTopicRecord().
            setTopicId(id), (short) 0));
    }

    // VisibleForTesting
    PartitionControlInfo getPartition(Uuid topicId, int partitionId) {
        TopicControlInfo topic = topics.get(topicId);
        if (topic == null) {
            return null;
        }
        return topic.parts.get(partitionId);
    }

    // VisibleForTesting
    BrokersToIsrs brokersToIsrs() {
        return brokersToIsrs;
    }

    ControllerResult<AlterIsrResponseData> alterIsr(AlterIsrRequestData request) {
        clusterControl.checkBrokerEpoch(request.brokerId(), request.brokerEpoch());
        AlterIsrResponseData response = new AlterIsrResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (AlterIsrRequestData.TopicData topicData : request.topics()) {
            AlterIsrResponseData.TopicData responseTopicData =
                new AlterIsrResponseData.TopicData().setName(topicData.name());
            response.topics().add(responseTopicData);
            Uuid topicId = topicsByName.get(topicData.name());
            if (topicId == null || !topics.containsKey(topicId)) {
                for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                continue;
            }
            TopicControlInfo topic = topics.get(topicId);
            for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                PartitionControlInfo partition = topic.parts.get(partitionData.partitionIndex());
                if (partition == null) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(UNKNOWN_TOPIC_OR_PARTITION.code()));
                    continue;
                }
                if (request.brokerId() != partition.leader) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(INVALID_REQUEST.code()));
                    continue;
                }
                if (partitionData.leaderEpoch() != partition.leaderEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.FENCED_LEADER_EPOCH.code()));
                    continue;
                }
                if (partitionData.currentIsrVersion() != partition.partitionEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_UPDATE_VERSION.code()));
                    continue;
                }
                int[] newIsr = Replicas.toArray(partitionData.newIsr());
                if (!Replicas.validateIsr(partition.replicas, newIsr)) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(INVALID_REQUEST.code()));
                    continue;
                }
                if (!Replicas.contains(newIsr, partition.leader)) {
                    // An alterIsr request can't remove the current leader.
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(INVALID_REQUEST.code()));
                    continue;
                }
                records.add(new ApiMessageAndVersion(new PartitionChangeRecord().
                    setPartitionId(partitionData.partitionIndex()).
                    setTopicId(topic.id).
                    setIsr(partitionData.newIsr()), (short) 0));
                responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                    setPartitionIndex(partitionData.partitionIndex()).
                    setErrorCode(Errors.NONE.code()).
                    setLeaderId(partition.leader).
                    setLeaderEpoch(partition.leaderEpoch).
                    setCurrentIsrVersion(partition.partitionEpoch + 1).
                    setIsr(partitionData.newIsr()));
            }
        }
        return ControllerResult.of(records, response);
    }

    /**
     * Generate the appropriate records to handle a broker being fenced.
     *
     * First, we remove this broker from any non-singleton ISR. Then we generate a
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
        generateLeaderAndIsrUpdates("handleBrokerFenced", brokerId, NO_LEADER, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
            setId(brokerId).setEpoch(brokerRegistration.epoch()), (short) 0));
    }

    /**
     * Generate the appropriate records to handle a broker being unregistered.
     *
     * First, we remove this broker from any non-singleton ISR. Then we generate an
     * UnregisterBrokerRecord.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnregistered(int brokerId, long brokerEpoch,
                                  List<ApiMessageAndVersion> records) {
        generateLeaderAndIsrUpdates("handleBrokerUnregistered", brokerId, NO_LEADER, records,
            brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
        records.add(new ApiMessageAndVersion(new UnregisterBrokerRecord().
            setBrokerId(brokerId).setBrokerEpoch(brokerEpoch), (short) 0));
    }

    /**
     * Generate the appropriate records to handle a broker becoming unfenced.
     *
     * First, we create an UnfenceBrokerRecord. Then, we check if if there are any
     * partitions that don't currently have a leader that should be led by the newly
     * unfenced broker.
     *
     * @param brokerId      The broker id.
     * @param brokerEpoch   The broker epoch.
     * @param records       The record list to append to.
     */
    void handleBrokerUnfenced(int brokerId, long brokerEpoch, List<ApiMessageAndVersion> records) {
        records.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
            setId(brokerId).setEpoch(brokerEpoch), (short) 0));
        generateLeaderAndIsrUpdates("handleBrokerUnfenced", NO_LEADER, brokerId, records,
            brokersToIsrs.partitionsWithNoLeader());
    }

    ControllerResult<ElectLeadersResponseData> electLeaders(ElectLeadersRequestData request) {
        boolean uncleanOk = electionTypeIsUnclean(request.electionType());
        List<ApiMessageAndVersion> records = new ArrayList<>();
        ElectLeadersResponseData response = new ElectLeadersResponseData();
        for (TopicPartitions topic : request.topicPartitions()) {
            ReplicaElectionResult topicResults =
                new ReplicaElectionResult().setTopic(topic.topic());
            response.replicaElectionResults().add(topicResults);
            for (int partitionId : topic.partitions()) {
                ApiError error = electLeader(topic.topic(), partitionId, uncleanOk, records);
                topicResults.partitionResult().add(new PartitionResult().
                    setPartitionId(partitionId).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
            }
        }
        return ControllerResult.of(records, response);
    }

    static boolean electionTypeIsUnclean(byte electionType) {
        ElectionType type;
        try {
            type = ElectionType.valueOf(electionType);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("Unknown election type " + (int) electionType);
        }
        return type == ElectionType.UNCLEAN;
    }

    ApiError electLeader(String topic, int partitionId, boolean uncleanOk,
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
        PartitionControlInfo partitionInfo = topicInfo.parts.get(partitionId);
        if (partitionInfo == null) {
            return new ApiError(UNKNOWN_TOPIC_OR_PARTITION,
                "No such partition as " + topic + "-" + partitionId);
        }
        int newLeader = bestLeader(partitionInfo.replicas, partitionInfo.isr, uncleanOk,
            r -> clusterControl.unfenced(r));
        if (newLeader == NO_LEADER) {
            // If we can't find any leader for the partition, return an error.
            return new ApiError(Errors.LEADER_NOT_AVAILABLE,
                "Unable to find any leader for the partition.");
        }
        if (newLeader == partitionInfo.leader) {
            // If the new leader we picked is the same as the current leader, there is
            // nothing to do.
            return ApiError.NONE;
        }
        if (partitionInfo.hasLeader() && newLeader != partitionInfo.preferredReplica()) {
            // It is not worth moving away from a valid leader to a new leader unless the
            // new leader is the preferred replica.
            return ApiError.NONE;
        }
        PartitionChangeRecord record = new PartitionChangeRecord().
            setPartitionId(partitionId).
            setTopicId(topicId).
            setLeader(newLeader);
        if (!electionWasClean(newLeader, partitionInfo.isr)) {
            // If the election was unclean, we have to forcibly set the ISR to just the
            // new leader. This can result in data loss!
            record.setIsr(Collections.singletonList(newLeader));
        }
        records.add(new ApiMessageAndVersion(record, (short) 0));
        return ApiError.NONE;
    }

    ControllerResult<BrokerHeartbeatReply> processBrokerHeartbeat(
                BrokerHeartbeatRequestData request, long lastCommittedOffset) {
        int brokerId = request.brokerId();
        long brokerEpoch = request.brokerEpoch();
        clusterControl.checkBrokerEpoch(brokerId, brokerEpoch);
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        BrokerControlStates states = heartbeatManager.calculateNextBrokerState(brokerId,
            request, lastCommittedOffset, () -> brokersToIsrs.hasLeaderships(brokerId));
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
                    generateLeaderAndIsrUpdates("enterControlledShutdown[" + brokerId + "]",
                        brokerId, NO_LEADER, records, brokersToIsrs.partitionsWithBrokerInIsr(brokerId));
                    break;
                case SHUTDOWN_NOW:
                    handleBrokerFenced(brokerId, records);
                    break;
            }
        }
        heartbeatManager.touch(brokerId,
            states.next().fenced(),
            request.currentMetadataOffset());
        boolean isCaughtUp = request.currentMetadataOffset() >= lastCommittedOffset;
        BrokerHeartbeatReply reply = new BrokerHeartbeatReply(isCaughtUp,
                states.next().fenced(),
                states.next().inControlledShutdown(),
                states.next().shouldShutDown());
        return ControllerResult.of(records, reply);
    }

    static boolean isGoodLeader(int[] isr, int leader) {
        return Replicas.contains(isr, leader);
    }

    static int bestLeader(int[] replicas, int[] isr, boolean uncleanOk,
                          Function<Integer, Boolean> isAcceptableLeader) {
        int bestUnclean = NO_LEADER;
        for (int i = 0; i < replicas.length; i++) {
            int replica = replicas[i];
            if (isAcceptableLeader.apply(replica)) {
                if (bestUnclean == NO_LEADER) bestUnclean = replica;
                if (Replicas.contains(isr, replica)) {
                    return replica;
                }
            }
        }
        return uncleanOk ? bestUnclean : NO_LEADER;
    }

    static boolean electionWasClean(int newLeader, int[] isr) {
        return newLeader == NO_LEADER || Replicas.contains(isr, newLeader);
    }

    public ControllerResult<Void> unregisterBroker(int brokerId) {
        BrokerRegistration registration = clusterControl.brokerRegistrations().get(brokerId);
        if (registration == null) {
            throw new BrokerIdNotRegisteredException("Broker ID " + brokerId +
                " is not currently registered");
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        handleBrokerUnregistered(brokerId, registration.epoch(), records);
        return ControllerResult.of(records, null);
    }

    ControllerResult<Void> maybeFenceStaleBrokers() {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        BrokerHeartbeatManager heartbeatManager = clusterControl.heartbeatManager();
        List<Integer> staleBrokers = heartbeatManager.findStaleBrokers();
        for (int brokerId : staleBrokers) {
            log.info("Fencing broker {} because its session has timed out.", brokerId);
            handleBrokerFenced(brokerId, records);
            heartbeatManager.fence(brokerId);
        }
        return ControllerResult.of(records, null);
    }

    ControllerResult<List<CreatePartitionsTopicResult>>
            createPartitions(List<CreatePartitionsTopic> topics) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        List<CreatePartitionsTopicResult> results = new ArrayList<>();
        for (CreatePartitionsTopic topic : topics) {
            ApiError apiError = ApiError.NONE;
            try {
                createPartitions(topic, records);
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
        return new ControllerResult<>(records, results, true);
    }

    void createPartitions(CreatePartitionsTopic topic,
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
        Iterator<PartitionControlInfo> iterator = topicInfo.parts.values().iterator();
        if (!iterator.hasNext()) {
            throw new UnknownServerException("Invalid state: topic " + topic.name() +
                " appears to have no partitions.");
        }
        PartitionControlInfo partitionInfo = iterator.next();
        if (partitionInfo.replicas.length > Short.MAX_VALUE) {
            throw new UnknownServerException("Invalid replication factor " +
                partitionInfo.replicas.length + ": expected a number equal to less than " +
                Short.MAX_VALUE);
        }
        short replicationFactor = (short) partitionInfo.replicas.length;
        int startPartitionId = topicInfo.parts.size();

        List<List<Integer>> placements;
        if (topic.assignments() != null) {
            placements = new ArrayList<>();
            for (CreatePartitionsAssignment assignment : topic.assignments()) {
                validateManualPartitionAssignment(assignment.brokerIds(),
                    OptionalInt.of(replicationFactor));
                placements.add(assignment.brokerIds());
            }
        } else {
            placements = clusterControl.placeReplicas(startPartitionId, additional,
                replicationFactor);
        }
        int partitionId = startPartitionId;
        for (List<Integer> placement : placements) {
            records.add(new ApiMessageAndVersion(new PartitionRecord().
                setPartitionId(partitionId).
                setTopicId(topicId).
                setReplicas(placement).
                setIsr(placement).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(placement.get(0)).
                setLeaderEpoch(0).
                setPartitionEpoch(0), (short) 0));
            partitionId++;
        }
    }

    void validateManualPartitionAssignment(List<Integer> assignment,
                                           OptionalInt replicationFactor) {
        if (assignment.isEmpty()) {
            throw new InvalidReplicaAssignmentException("The manual partition " +
                "assignment includes an empty replica list.");
        }
        List<Integer> sortedBrokerIds = new ArrayList<>(assignment);
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
     * Iterate over a sequence of partitions and generate ISR changes and/or leader
     * changes if necessary.
     *
     * @param context           A human-readable context string used in log4j logging.
     * @param brokerToRemove    NO_LEADER if no broker is being removed; the ID of the
     *                          broker to remove from the ISR and leadership, otherwise.
     * @param brokerToAdd       NO_LEADER if no broker is being added; the ID of the
     *                          broker which is now eligible to be a leader, otherwise.
     * @param records           A list of records which we will append to.
     * @param iterator          The iterator containing the partitions to examine.
     */
    void generateLeaderAndIsrUpdates(String context,
                                     int brokerToRemove,
                                     int brokerToAdd,
                                     List<ApiMessageAndVersion> records,
                                     Iterator<TopicIdPartition> iterator) {
        int oldSize = records.size();
        Function<Integer, Boolean> isAcceptableLeader =
            r -> (r != brokerToRemove) && (r == brokerToAdd || clusterControl.unfenced(r));
        while (iterator.hasNext()) {
            TopicIdPartition topicIdPart = iterator.next();
            TopicControlInfo topic = topics.get(topicIdPart.topicId());
            if (topic == null) {
                throw new RuntimeException("Topic ID " + topicIdPart.topicId() +
                        " existed in isrMembers, but not in the topics map.");
            }
            PartitionControlInfo partition = topic.parts.get(topicIdPart.partitionId());
            if (partition == null) {
                throw new RuntimeException("Partition " + topicIdPart +
                    " existed in isrMembers, but not in the partitions map.");
            }
            int[] newIsr = Replicas.copyWithout(partition.isr, brokerToRemove);
            int newLeader;
            if (isGoodLeader(newIsr, partition.leader)) {
                // If the current leader is good, don't change.
                newLeader = partition.leader;
            } else {
                // Choose a new leader.
                boolean uncleanOk = configurationControl.uncleanLeaderElectionEnabledForTopic(topic.name);
                newLeader = bestLeader(partition.replicas, newIsr, uncleanOk, isAcceptableLeader);
            }
            if (!electionWasClean(newLeader, newIsr)) {
                // After an unclean leader election, the ISR is reset to just the new leader.
                newIsr = new int[] {newLeader};
            } else if (newIsr.length == 0) {
                // We never want to shrink the ISR to size 0.
                newIsr = partition.isr;
            }
            PartitionChangeRecord record = new PartitionChangeRecord().
                setPartitionId(topicIdPart.partitionId()).
                setTopicId(topic.id);
            if (newLeader != partition.leader) record.setLeader(newLeader);
            if (!Arrays.equals(newIsr, partition.isr)) record.setIsr(Replicas.toList(newIsr));
            if (record.leader() != NO_LEADER_CHANGE || record.isr() != null) {
                records.add(new ApiMessageAndVersion(record, (short) 0));
            }
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
                log.debug("{}: changing partition(s): {}", context, bld.toString());
            } else if (log.isInfoEnabled()) {
                log.info("{}: changing {} partition(s)", context, records.size() - oldSize);
            }
        }
    }

    class ReplicationControlIterator implements Iterator<List<ApiMessageAndVersion>> {
        private final long epoch;
        private final Iterator<TopicControlInfo> iterator;

        ReplicationControlIterator(long epoch) {
            this.epoch = epoch;
            this.iterator = topics.values(epoch).iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<ApiMessageAndVersion> next() {
            if (!hasNext()) throw new NoSuchElementException();
            TopicControlInfo topic = iterator.next();
            List<ApiMessageAndVersion> records = new ArrayList<>();
            records.add(new ApiMessageAndVersion(new TopicRecord().
                setName(topic.name).
                setTopicId(topic.id), (short) 0));
            for (Entry<Integer, PartitionControlInfo> entry : topic.parts.entrySet(epoch)) {
                PartitionControlInfo partition = entry.getValue();
                records.add(new ApiMessageAndVersion(new PartitionRecord().
                    setPartitionId(entry.getKey()).
                    setTopicId(topic.id).
                    setReplicas(Replicas.toList(partition.replicas)).
                    setIsr(Replicas.toList(partition.isr)).
                    setRemovingReplicas(Replicas.toList(partition.removingReplicas)).
                    setAddingReplicas(Replicas.toList(partition.addingReplicas)).
                    setLeader(partition.leader).
                    setLeaderEpoch(partition.leaderEpoch).
                    setPartitionEpoch(partition.partitionEpoch), (short) 0));
            }
            return records;
        }
    }

    ReplicationControlIterator iterator(long epoch) {
        return new ReplicationControlIterator(epoch);
    }
}
