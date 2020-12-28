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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

public class ReplicationControlManager {
    static class TopicControlInfo {
        private final Uuid id;
        private final TimelineHashMap<Integer, PartitionControlInfo> parts;

        TopicControlInfo(SnapshotRegistry snapshotRegistry, Uuid id) {
            this.id = id;
            this.parts = new TimelineHashMap<>(snapshotRegistry, 0);
        }
    }

    static class PartitionControlInfo {
        private final int[] replicas;
        private final int[] isr;
        private final int[] removingReplicas;
        private final int[] addingReplicas;
        private final int leader;
        private final int leaderEpoch;

        PartitionControlInfo(PartitionRecord record) {
            this(toArray(record.replicas()),
                toArray(record.isr()),
                toArray(record.removingReplicas()),
                toArray(record.addingReplicas()),
                record.leader(),
                record.leaderEpoch());
        }

        PartitionControlInfo(int[] replicas, int[] isr, int[] removingReplicas,
                             int[] addingReplicas, int leader, int leaderEpoch) {
            this.replicas = replicas;
            this.isr = isr;
            this.removingReplicas = removingReplicas;
            this.addingReplicas = addingReplicas;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
        }

        String diff(PartitionControlInfo prev) {
            StringBuilder builder = new StringBuilder();
            String prefix = "";
            if (!Arrays.equals(replicas, prev.replicas)) {
                builder.append("replicas=").append(Arrays.toString(replicas));
                prefix = ", ";
            }
            if (!Arrays.equals(isr, prev.isr)) {
                builder.append(prefix).append("isr=").append(Arrays.toString(isr));
                prefix = ", ";
            }
            if (!Arrays.equals(removingReplicas, prev.removingReplicas)) {
                builder.append(prefix).append("removingReplicas=").
                    append(Arrays.toString(removingReplicas));
                prefix = ", ";
            }
            if (!Arrays.equals(addingReplicas, prev.addingReplicas)) {
                builder.append(prefix).append("addingReplicas=").
                    append(Arrays.toString(addingReplicas));
                prefix = ", ";
            }
            if (leader != prev.leader) {
                builder.append(prefix).append("leader=").append(leader);
                prefix = ", ";
            }
            if (leaderEpoch != prev.leaderEpoch) {
                builder.append(prefix).append("leaderEpoch=").append(leaderEpoch);
            }
            return builder.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isr, removingReplicas, addingReplicas, leader,
                leaderEpoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionControlInfo)) return false;
            PartitionControlInfo other = (PartitionControlInfo) o;
            return diff(other).isEmpty();
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
            builder.append(")");
            return builder.toString();
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;
    private final Random random;
    private final int defaultReplicationFactor;
    private final ConfigurationControlManager configurationControl;
    private final ClusterControlManager clusterControl;
    private final TimelineHashMap<String, Uuid> topicsByName;
    private final TimelineHashMap<Uuid, TopicControlInfo> topics;

    ReplicationControlManager(SnapshotRegistry snapshotRegistry,
                              LogContext logContext,
                              Random random,
                              int defaultReplicationFactor,
                              ConfigurationControlManager configurationControl,
                              ClusterControlManager clusterControl) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(ReplicationControlManager.class);
        this.random = random;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.configurationControl = configurationControl;
        this.clusterControl = clusterControl;
        this.topicsByName = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topics = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public void replay(TopicRecord record) {
        topicsByName.put(record.name(), record.topicId());
        topics.put(record.topicId(), new TopicControlInfo(snapshotRegistry, record.topicId()));
        log.info("Created topic {} with ID {}.", record.name(), record.topicId());
    }

    public void replay(PartitionRecord message) {
        TopicControlInfo topicInfo = topics.get(message.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + message.topicId() +
                "-" + message.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo newPartInfo = new PartitionControlInfo(message);
        PartitionControlInfo prevPartInfo = topicInfo.parts.get(message.partitionId());
        if (prevPartInfo == null) {
            log.info("Created partition {}-{} with {}.", message.topicId(),
                message.partitionId(), newPartInfo.toString());
            topicInfo.parts.put(message.partitionId(), newPartInfo);
        } else {
            String diff = newPartInfo.diff(prevPartInfo);
            if (!diff.isEmpty()) {
                log.info("Modified partition {}-{}: {}.", message.topicId(),
                    message.partitionId(), diff);
                topicInfo.parts.put(message.partitionId(), newPartInfo);
            }
        }
    }

    public ControllerResult<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check the topic names.
        validateNewTopicNames(topicErrors, request.topics());

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
        for (CreatableTopic topic : request.topics()) {
            if (topicErrors.containsKey(topic.name())) continue;
            ApiError error = createTopic(topic, records);
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
            CreatableTopicResult result = new CreatableTopicResult().
                setName(topic.name()).
                setErrorCode((short) 0).
                setErrorMessage(null);
            data.topics().add(result);
            resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                append("SUCCESS");
            resultsPrefix = ", ";
        }
        log.info("createTopics result(s): {}", resultsBuilder.toString());
        return new ControllerResult<>(records, data);
    }

    private ApiError createTopic(CreatableTopic topic, List<ApiMessageAndVersion> records) {
        Map<Integer, PartitionControlInfo> newParts = new HashMap<>();
        if (topic.numPartitions() <= 0) {
            return new ApiError(Errors.INVALID_REQUEST,
                "The number of partitions must be greater than 0.");
        }
        if (!topic.assignments().isEmpty()) {
            if (topic.replicationFactor() != -1) {
                return new ApiError(Errors.INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication " +
                    "factor was not set to -1.");
            } else if (topic.assignments().size() != topic.numPartitions()) {
                return new ApiError(Errors.INVALID_REQUEST, "" + topic.numPartitions() +
                    " partitions were specified, but only " + topic.assignments().size() +
                    " manual partition assignments were given.");
            }
            for (CreatableReplicaAssignment assignment : topic.assignments()) {
                if (newParts.containsKey(assignment.partitionIndex())) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                        "Found multiple manual partition assignments for partition " +
                            assignment.partitionIndex());
                }
                HashSet<Integer> brokerIds = new HashSet<>();
                for (int brokerId : assignment.brokerIds()) {
                    if (!brokerIds.add(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment specifies the same node " +
                                "id more than once.");
                    } else if (!clusterControl.isUsable(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment contains node " + brokerId +
                                ", but that node is not usable.");
                    }
                }
                int[] replicas = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    replicas[i] = assignment.brokerIds().get(0);
                }
                int[] isr = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    isr[i] = assignment.brokerIds().get(0);
                }
                newParts.put(assignment.partitionIndex(),
                    new PartitionControlInfo(replicas, isr, null, null, isr[0], 0));
            }
        } else if (topic.replicationFactor() < -1 || topic.replicationFactor() == 0) {
            return new ApiError(Errors.INVALID_REQUEST,
                "Replication factor was set to an invalid non-positive value.");
        } else if (!topic.assignments().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST,
                "Replication factor was not set to -1 but a manual partition " +
                    "assignment was specified.");
        } else {
            int replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            for (int partitionId = 0; partitionId < topic.numPartitions(); partitionId++) {
                List<Integer> replicas;
                try {
                    replicas = clusterControl.chooseRandomUsable(random, replicationFactor);
                } catch (Exception e) {
                    return new ApiError(Errors.INVALID_REQUEST,
                        "Unable to replicate the partition " + replicationFactor +
                            " times: " + e.getMessage());
                }
                newParts.put(partitionId, new PartitionControlInfo(toArray(replicas),
                    toArray(replicas), null, null, replicas.get(0), 0));
            }
        }
        Uuid topicId = Uuid.randomUuid();
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        for (Entry<Integer, PartitionControlInfo> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionControlInfo info = partEntry.getValue();
            records.add(new ApiMessageAndVersion(new PartitionRecord().
                setPartitionId(partitionIndex).
                setTopicId(topicId).
                setReplicas(toList(info.replicas)).
                setIsr(toList(info.isr)).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(info.leader).
                setLeaderEpoch(info.leaderEpoch), (short) 0));
        }
        return ApiError.NONE;
    }

    private static List<Integer> toList(int[] array) {
        if (array == null) return null;
        ArrayList<Integer> list = new ArrayList<>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    private static int[] toArray(List<Integer> list) {
        if (list == null) return null;
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (Exception e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_REQUEST, "Illegal topic name."));
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
}
