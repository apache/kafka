/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
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

public class CreateTopicsRequest extends AbstractRequest {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.CREATE_TOPICS.id);

    private static final String REQUESTS_KEY_NAME = "create_topic_requests";

    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String NUM_PARTITIONS_KEY_NAME = "num_partitions";
    private static final String REPLICATION_FACTOR_KEY_NAME = "replication_factor";
    private static final String REPLICA_ASSIGNMENT_KEY_NAME = "replica_assignment";
    private static final String REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME = "partition_id";
    private static final String REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME = "replicas";

    private static final String CONFIG_KEY_KEY_NAME = "config_key";
    private static final String CONFIG_VALUE_KEY_NAME = "config_value";
    private static final String CONFIGS_KEY_NAME = "configs";

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
            this(partitions, replicationFactor, Collections.<Integer, List<Integer>>emptyMap(), configs);
        }

        public TopicDetails(int partitions,
                            short replicationFactor) {
            this(partitions, replicationFactor, Collections.<String, String>emptyMap());
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments,
                            Map<String, String> configs) {
            this(NO_NUM_PARTITIONS, NO_REPLICATION_FACTOR, replicasAssignments, configs);
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments) {
            this(replicasAssignments, Collections.<String, String>emptyMap());
        }
    }

    private final Map<String, TopicDetails> topics;
    private final Integer timeout;

    // Set to handle special case where 2 requests for the same topic exist on the wire.
    // This allows the broker to return an error code for these topics.
    private final Set<String> duplicateTopics;

    public static final int NO_NUM_PARTITIONS = -1;
    public static final short NO_REPLICATION_FACTOR = -1;

    public CreateTopicsRequest(Map<String, TopicDetails> topics, Integer timeout) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> createTopicRequestStructs = new ArrayList<>(topics.size());
        for (Map.Entry<String, TopicDetails> entry : topics.entrySet()) {

            Struct singleRequestStruct = struct.instance(REQUESTS_KEY_NAME);
            String topic = entry.getKey();
            TopicDetails args = entry.getValue();

            singleRequestStruct.set(TOPIC_KEY_NAME, topic);
            singleRequestStruct.set(NUM_PARTITIONS_KEY_NAME, args.numPartitions);
            singleRequestStruct.set(REPLICATION_FACTOR_KEY_NAME, args.replicationFactor);

            // replica assignment
            List<Struct> replicaAssignmentsStructs = new ArrayList<>(args.replicasAssignments.size());
            for (Map.Entry<Integer, List<Integer>> partitionReplicaAssignment : args.replicasAssignments.entrySet()) {
                Struct replicaAssignmentStruct = singleRequestStruct.instance(REPLICA_ASSIGNMENT_KEY_NAME);
                replicaAssignmentStruct.set(REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME, partitionReplicaAssignment.getKey());
                replicaAssignmentStruct.set(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME, partitionReplicaAssignment.getValue().toArray());
                replicaAssignmentsStructs.add(replicaAssignmentStruct);
            }
            singleRequestStruct.set(REPLICA_ASSIGNMENT_KEY_NAME, replicaAssignmentsStructs.toArray());

            // configs
            List<Struct> configsStructs = new ArrayList<>(args.configs.size());
            for (Map.Entry<String, String> configEntry : args.configs.entrySet()) {
                Struct configStruct = singleRequestStruct.instance(CONFIGS_KEY_NAME);
                configStruct.set(CONFIG_KEY_KEY_NAME, configEntry.getKey());
                configStruct.set(CONFIG_VALUE_KEY_NAME, configEntry.getValue());
                configsStructs.add(configStruct);
            }
            singleRequestStruct.set(CONFIGS_KEY_NAME, configsStructs.toArray());
            createTopicRequestStructs.add(singleRequestStruct);
        }
        struct.set(REQUESTS_KEY_NAME, createTopicRequestStructs.toArray());
        struct.set(TIMEOUT_KEY_NAME, timeout);

        this.topics = topics;
        this.timeout = timeout;
        this.duplicateTopics = Collections.emptySet();
    }

    public CreateTopicsRequest(Struct struct) {
        super(struct);

        Object[] requestStructs = struct.getArray(REQUESTS_KEY_NAME);
        Map<String, TopicDetails> topics = new HashMap<>();
        Set<String> duplicateTopics = new HashSet<>();

        for (Object requestStructObj : requestStructs) {
            Struct singleRequestStruct = (Struct) requestStructObj;
            String topic = singleRequestStruct.getString(TOPIC_KEY_NAME);

            if (topics.containsKey(topic))
                duplicateTopics.add(topic);

            int numPartitions = singleRequestStruct.getInt(NUM_PARTITIONS_KEY_NAME);
            short replicationFactor = singleRequestStruct.getShort(REPLICATION_FACTOR_KEY_NAME);

            //replica assignment
            Object[] assignmentsArray = singleRequestStruct.getArray(REPLICA_ASSIGNMENT_KEY_NAME);
            Map<Integer, List<Integer>> partitionReplicaAssignments = new HashMap<>(assignmentsArray.length);
            for (Object assignmentStructObj : assignmentsArray) {
                Struct assignmentStruct = (Struct) assignmentStructObj;

                Integer partitionId = assignmentStruct.getInt(REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME);

                Object[] replicasArray = assignmentStruct.getArray(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME);
                List<Integer> replicas = new ArrayList<>(replicasArray.length);
                for (Object replica : replicasArray) {
                    replicas.add((Integer) replica);
                }

                partitionReplicaAssignments.put(partitionId, replicas);
            }

            Object[] configArray = singleRequestStruct.getArray(CONFIGS_KEY_NAME);
            Map<String, String> configs = new HashMap<>(configArray.length);
            for (Object configStructObj : configArray) {
                Struct configStruct = (Struct) configStructObj;

                String key = configStruct.getString(CONFIG_KEY_KEY_NAME);
                String value = configStruct.getString(CONFIG_VALUE_KEY_NAME);

                configs.put(key, value);
            }

            TopicDetails args = new TopicDetails(numPartitions, replicationFactor, partitionReplicaAssignments, configs);

            topics.put(topic, args);
        }

        this.topics = topics;
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        this.duplicateTopics = duplicateTopics;
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : topics.keySet()) {
            topicErrors.put(topic, Errors.forException(e));
        }

        switch (versionId) {
            case 0:
                return new CreateTopicsResponse(topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.CREATE_TOPICS.id)));
        }
    }

    public Map<String, TopicDetails> topics() {
        return this.topics;
    }

    public Integer timeout() {
        return this.timeout;
    }

    public Set<String> duplicateTopics() {
        return this.duplicateTopics;
    }

    public static CreateTopicsRequest parse(ByteBuffer buffer, int versionId) {
        return new CreateTopicsRequest(ProtoUtils.parseRequest(ApiKeys.CREATE_TOPICS.id, versionId, buffer));
    }

    public static CreateTopicsRequest parse(ByteBuffer buffer) {
        return new CreateTopicsRequest(CURRENT_SCHEMA.read(buffer));
    }
}
