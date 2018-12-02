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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class CreateTopicsRequest extends AbstractRequest {
    private static final String REQUESTS_KEY_NAME = "create_topic_requests";

    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String VALIDATE_ONLY_KEY_NAME = "validate_only";
    private static final String NUM_PARTITIONS_KEY_NAME = "num_partitions";
    private static final String REPLICATION_FACTOR_KEY_NAME = "replication_factor";
    private static final String REPLICA_ASSIGNMENT_KEY_NAME = "replica_assignment";
    private static final String REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME = "replicas";

    private static final String CONFIG_NAME_KEY_NAME = "config_name";
    private static final String CONFIG_VALUE_KEY_NAME = "config_value";
    private static final String CONFIG_ENTRIES_KEY_NAME = "config_entries";

    private static final Schema CONFIG_ENTRY = new Schema(
            new Field(CONFIG_NAME_KEY_NAME, STRING, "Configuration name"),
            new Field(CONFIG_VALUE_KEY_NAME, NULLABLE_STRING, "Configuration value"));

    private static final Schema PARTITION_REPLICA_ASSIGNMENT_ENTRY = new Schema(
            PARTITION_ID,
            new Field(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME, new ArrayOf(INT32), "The set of all nodes that should " +
                    "host this partition. The first replica in the list is the preferred leader."));

    private static final Schema SINGLE_CREATE_TOPIC_REQUEST_V0 = new Schema(
            TOPIC_NAME,
            new Field(NUM_PARTITIONS_KEY_NAME, INT32, "Number of partitions to be created. -1 indicates unset."),
            new Field(REPLICATION_FACTOR_KEY_NAME, INT16, "Replication factor for the topic. -1 indicates unset."),
            new Field(REPLICA_ASSIGNMENT_KEY_NAME, new ArrayOf(PARTITION_REPLICA_ASSIGNMENT_ENTRY),
                    "Replica assignment among kafka brokers for this topic partitions. If this is set num_partitions " +
                            "and replication_factor must be unset."),
            new Field(CONFIG_ENTRIES_KEY_NAME, new ArrayOf(CONFIG_ENTRY), "Topic level configuration for topic to be set."));

    private static final Schema SINGLE_CREATE_TOPIC_REQUEST_V1 = SINGLE_CREATE_TOPIC_REQUEST_V0;

    private static final Schema CREATE_TOPICS_REQUEST_V0 = new Schema(
            new Field(REQUESTS_KEY_NAME, new ArrayOf(SINGLE_CREATE_TOPIC_REQUEST_V0),
                    "An array of single topic creation requests. Can not have multiple entries for the same topic."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for a topic to be completely created on the " +
                    "controller node. Values <= 0 will trigger topic creation and return immediately"));

    private static final Schema CREATE_TOPICS_REQUEST_V1 = new Schema(
            new Field(REQUESTS_KEY_NAME, new ArrayOf(SINGLE_CREATE_TOPIC_REQUEST_V1), "An array of single " +
                    "topic creation requests. Can not have multiple entries for the same topic."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for a topic to be completely created on the " +
                    "controller node. Values <= 0 will trigger topic creation and return immediately"),
            new Field(VALIDATE_ONLY_KEY_NAME, BOOLEAN, "If this is true, the request will be validated, but the " +
                    "topic won't be created."));

    /* v2 request is the same as v1. Throttle time has been added to the response */
    private static final Schema CREATE_TOPICS_REQUEST_V2 = CREATE_TOPICS_REQUEST_V1;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema CREATE_TOPICS_REQUEST_V3 = CREATE_TOPICS_REQUEST_V2;

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_TOPICS_REQUEST_V0, CREATE_TOPICS_REQUEST_V1, CREATE_TOPICS_REQUEST_V2,
            CREATE_TOPICS_REQUEST_V3};
    }

    public static final class TopicDetails {
        public final int numPartitions;
        public final short replicationFactor;
        public final Map<Integer, List<Integer>> replicasAssignments;
        public final Map<String, String> configs;

        private TopicDetails(int numPartitions,
                             short replicationFactor,
                             Map<Integer, List<Integer>> replicasAssignments,
                             Map<String, String> configs) {
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments;
            this.configs = configs;
        }

        public TopicDetails(int partitions,
                            short replicationFactor,
                            Map<String, String> configs) {
            this(partitions, replicationFactor, Collections.emptyMap(), configs);
        }

        public TopicDetails(int partitions,
                            short replicationFactor) {
            this(partitions, replicationFactor, Collections.emptyMap());
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments,
                            Map<String, String> configs) {
            this(NO_NUM_PARTITIONS, NO_REPLICATION_FACTOR, replicasAssignments, configs);
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments) {
            this(replicasAssignments, Collections.emptyMap());
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(numPartitions=").append(numPartitions).
                    append(", replicationFactor=").append(replicationFactor).
                    append(", replicasAssignments=").append(replicasAssignments).
                    append(", configs=").append(configs).
                    append(")");
            return bld.toString();
        }
    }

    public static class Builder extends AbstractRequest.Builder<CreateTopicsRequest> {
        private final Map<String, TopicDetails> topics;
        private final int timeout;
        private final boolean validateOnly; // introduced in V1

        public Builder(Map<String, TopicDetails> topics, int timeout) {
            this(topics, timeout, false);
        }

        public Builder(Map<String, TopicDetails> topics, int timeout, boolean validateOnly) {
            super(ApiKeys.CREATE_TOPICS);
            this.topics = topics;
            this.timeout = timeout;
            this.validateOnly = validateOnly;
        }

        @Override
        public CreateTopicsRequest build(short version) {
            if (validateOnly && version == 0)
                throw new UnsupportedVersionException("validateOnly is not supported in version 0 of " +
                        "CreateTopicsRequest");
            return new CreateTopicsRequest(topics, timeout, validateOnly, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=CreateTopicsRequest").
                append(", topics=").append(topics).
                append(", timeout=").append(timeout).
                append(", validateOnly=").append(validateOnly).
                append(")");
            return bld.toString();
        }
    }

    private final Map<String, TopicDetails> topics;
    private final Integer timeout;
    private final boolean validateOnly; // introduced in V1

    // Set to handle special case where 2 requests for the same topic exist on the wire.
    // This allows the broker to return an error code for these topics.
    private final Set<String> duplicateTopics;

    public static final int NO_NUM_PARTITIONS = -1;
    public static final short NO_REPLICATION_FACTOR = -1;

    private CreateTopicsRequest(Map<String, TopicDetails> topics, Integer timeout, boolean validateOnly, short version) {
        super(ApiKeys.CREATE_TOPICS, version);
        this.topics = topics;
        this.timeout = timeout;
        this.validateOnly = validateOnly;
        this.duplicateTopics = Collections.emptySet();
    }

    public CreateTopicsRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_TOPICS, version);

        Object[] requestStructs = struct.getArray(REQUESTS_KEY_NAME);
        Map<String, TopicDetails> topics = new HashMap<>();
        Set<String> duplicateTopics = new HashSet<>();

        for (Object requestStructObj : requestStructs) {
            Struct singleRequestStruct = (Struct) requestStructObj;
            String topic = singleRequestStruct.get(TOPIC_NAME);

            if (topics.containsKey(topic))
                duplicateTopics.add(topic);

            int numPartitions = singleRequestStruct.getInt(NUM_PARTITIONS_KEY_NAME);
            short replicationFactor = singleRequestStruct.getShort(REPLICATION_FACTOR_KEY_NAME);

            //replica assignment
            Object[] assignmentsArray = singleRequestStruct.getArray(REPLICA_ASSIGNMENT_KEY_NAME);
            Map<Integer, List<Integer>> partitionReplicaAssignments = new HashMap<>(assignmentsArray.length);
            for (Object assignmentStructObj : assignmentsArray) {
                Struct assignmentStruct = (Struct) assignmentStructObj;

                Integer partitionId = assignmentStruct.get(PARTITION_ID);

                Object[] replicasArray = assignmentStruct.getArray(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME);
                List<Integer> replicas = new ArrayList<>(replicasArray.length);
                for (Object replica : replicasArray) {
                    replicas.add((Integer) replica);
                }

                partitionReplicaAssignments.put(partitionId, replicas);
            }

            Object[] configArray = singleRequestStruct.getArray(CONFIG_ENTRIES_KEY_NAME);
            Map<String, String> configs = new HashMap<>(configArray.length);
            for (Object configStructObj : configArray) {
                Struct configStruct = (Struct) configStructObj;

                String key = configStruct.getString(CONFIG_NAME_KEY_NAME);
                String value = configStruct.getString(CONFIG_VALUE_KEY_NAME);

                configs.put(key, value);
            }

            TopicDetails args = new TopicDetails(numPartitions, replicationFactor, partitionReplicaAssignments, configs);

            topics.put(topic, args);
        }

        this.topics = topics;
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        if (struct.hasField(VALIDATE_ONLY_KEY_NAME))
            this.validateOnly = struct.getBoolean(VALIDATE_ONLY_KEY_NAME);
        else
            this.validateOnly = false;
        this.duplicateTopics = duplicateTopics;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        for (String topic : topics.keySet()) {
            topicErrors.put(topic, ApiError.fromThrowable(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new CreateTopicsResponse(topicErrors);
            case 2:
            case 3:
                return new CreateTopicsResponse(throttleTimeMs, topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.CREATE_TOPICS.latestVersion()));
        }
    }

    public Map<String, TopicDetails> topics() {
        return this.topics;
    }

    public int timeout() {
        return this.timeout;
    }

    public boolean validateOnly() {
        return validateOnly;
    }

    public Set<String> duplicateTopics() {
        return this.duplicateTopics;
    }

    public static CreateTopicsRequest parse(ByteBuffer buffer, short version) {
        return new CreateTopicsRequest(ApiKeys.CREATE_TOPICS.parseRequest(version, buffer), version);
    }

    /**
     * Visible for testing.
     */
    @Override
    public Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.CREATE_TOPICS.requestSchema(version));

        List<Struct> createTopicRequestStructs = new ArrayList<>(topics.size());
        for (Map.Entry<String, TopicDetails> entry : topics.entrySet()) {

            Struct singleRequestStruct = struct.instance(REQUESTS_KEY_NAME);
            String topic = entry.getKey();
            TopicDetails args = entry.getValue();

            singleRequestStruct.set(TOPIC_NAME, topic);
            singleRequestStruct.set(NUM_PARTITIONS_KEY_NAME, args.numPartitions);
            singleRequestStruct.set(REPLICATION_FACTOR_KEY_NAME, args.replicationFactor);

            // replica assignment
            List<Struct> replicaAssignmentsStructs = new ArrayList<>(args.replicasAssignments.size());
            for (Map.Entry<Integer, List<Integer>> partitionReplicaAssignment : args.replicasAssignments.entrySet()) {
                Struct replicaAssignmentStruct = singleRequestStruct.instance(REPLICA_ASSIGNMENT_KEY_NAME);
                replicaAssignmentStruct.set(PARTITION_ID, partitionReplicaAssignment.getKey());
                replicaAssignmentStruct.set(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME, partitionReplicaAssignment.getValue().toArray());
                replicaAssignmentsStructs.add(replicaAssignmentStruct);
            }
            singleRequestStruct.set(REPLICA_ASSIGNMENT_KEY_NAME, replicaAssignmentsStructs.toArray());

            // configs
            List<Struct> configsStructs = new ArrayList<>(args.configs.size());
            for (Map.Entry<String, String> configEntry : args.configs.entrySet()) {
                Struct configStruct = singleRequestStruct.instance(CONFIG_ENTRIES_KEY_NAME);
                configStruct.set(CONFIG_NAME_KEY_NAME, configEntry.getKey());
                configStruct.set(CONFIG_VALUE_KEY_NAME, configEntry.getValue());
                configsStructs.add(configStruct);
            }
            singleRequestStruct.set(CONFIG_ENTRIES_KEY_NAME, configsStructs.toArray());
            createTopicRequestStructs.add(singleRequestStruct);
        }
        struct.set(REQUESTS_KEY_NAME, createTopicRequestStructs.toArray());
        struct.set(TIMEOUT_KEY_NAME, timeout);
        if (version >= 1)
            struct.set(VALIDATE_ONLY_KEY_NAME, validateOnly);
        return struct;

    }
}
