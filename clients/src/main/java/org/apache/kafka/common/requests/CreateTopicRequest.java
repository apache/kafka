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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTopicRequest extends AbstractRequest {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.CREATE_TOPIC.id);

    private static final String REQUESTS_KEY_NAME = "create_topic_requests";

    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String REPLICATION_FACTOR_KEY_NAME = "replication_factor";
    private static final String REPLICA_ASSIGNMENT_KEY_NAME = "replica_assignment";
    private static final String REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME = "partition_id";
    private static final String REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME = "replicas";

    private static final String CONFIG_KEY_KEY_NAME = "config_key";
    private static final String CONFIG_VALUE_KEY_NAME = "config_value";
    private static final String CONFIGS_KEY_NAME = "configs";

    public static final class TopicDetails {
        public final int partitions;
        public final int replicationFactor;
        public final Map<Integer, List<Integer>> replicasAssignments;
        public final Map<String, String> configs;

        private TopicDetails(int partitions,
                            int replicationFactor,
                            Map<Integer, List<Integer>> replicasAssignments,
                            Map<String, String> configs) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments;
            this.configs = configs;
        }

        public TopicDetails(int partitions,
                            int replicationFactor,
                            Map<String, String> configs) {
            this(partitions, replicationFactor, new HashMap<Integer, List<Integer>>(), configs);
        }

        public TopicDetails(int partitions,
                            int replicationFactor) {
            this(partitions, replicationFactor, new HashMap<String, String>());
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments,
                            Map<String, String> configs) {
            this(NO_PARTITIONS_SIGN, NO_REPLICATION_FACTOR_SIGN, replicasAssignments, configs);
        }

        public TopicDetails(Map<Integer, List<Integer>> replicasAssignments) {
            this(replicasAssignments, new HashMap<String, String>());
        }
    }

    private final Map<String, TopicDetails> topics;

    public static final int NO_PARTITIONS_SIGN = -1;
    public static final int NO_REPLICATION_FACTOR_SIGN = -1;

    public CreateTopicRequest(Map<String, TopicDetails> topics) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> createTopicRequestStructs = new ArrayList<>(topics.size());
        for (Map.Entry<String, TopicDetails> entry : topics.entrySet()) {

            Struct singleRequestStruct = struct.instance(REQUESTS_KEY_NAME);
            String topic = entry.getKey();
            TopicDetails args = entry.getValue();

            singleRequestStruct.set(TOPIC_KEY_NAME, topic);
            singleRequestStruct.set(PARTITIONS_KEY_NAME, args.partitions);
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

        this.topics = topics;
    }

    public CreateTopicRequest(Struct struct) {
        super(struct);

        Object[] requestStructs = struct.getArray(REQUESTS_KEY_NAME);
        topics = new HashMap<>();
        for (Object requestStructObj : requestStructs) {
            Struct singleRequestStruct = (Struct) requestStructObj;
            String topic = singleRequestStruct.getString(TOPIC_KEY_NAME);
            int partitions = singleRequestStruct.getInt(PARTITIONS_KEY_NAME);
            int replicationFactor = singleRequestStruct.getInt(REPLICATION_FACTOR_KEY_NAME);

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

            TopicDetails args = new TopicDetails(
                partitions, replicationFactor, partitionReplicaAssignments, configs);
            topics.put(topic, args);
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : topics.keySet()) {
            topicErrors.put(topic, Errors.forException(e));
        }

        switch (versionId) {
            case 0:
                return new CreateTopicResponse(topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.CREATE_TOPIC.id)));
        }
    }

    public Map<String, TopicDetails> topics() {
        return this.topics;
    }

    public static CreateTopicRequest parse(ByteBuffer buffer, int versionId) {
        return new CreateTopicRequest(ProtoUtils.parseRequest(ApiKeys.CREATE_TOPIC.id, versionId, buffer));
    }

    public static CreateTopicRequest parse(ByteBuffer buffer) {
        return new CreateTopicRequest(CURRENT_SCHEMA.read(buffer));
    }
}
