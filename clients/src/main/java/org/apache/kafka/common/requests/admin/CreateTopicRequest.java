/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests.admin;


import org.apache.kafka.common.ConfigEntry;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequestResponse;
import org.apache.kafka.common.PartitionReplicaAssignment;

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

    public static final class CreateTopicArguments {
        public final int partitions;
        public final int replicationFactor;
        public final List<PartitionReplicaAssignment> replicasAssignments;
        public final List<ConfigEntry> configs;

        public CreateTopicArguments(int partitions,
                                    int replicationFactor,
                                    List<PartitionReplicaAssignment> replicasAssignments,
                                    List<ConfigEntry> configs) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments;
            this.configs = configs;
        }
    }

    private final Map<String, CreateTopicArguments> createTopicArguments;

    public static final int NO_PARTITIONS_SIGN = -1;
    public static final int NO_REPLICATION_FACTOR_SIGN = -1;

    public CreateTopicRequest(Map<String, CreateTopicArguments> createTopicArguments) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> createTopicRequestStructs = new ArrayList<Struct>(createTopicArguments.size());
        for (Map.Entry<String, CreateTopicArguments> createTopicArgumentsEntry : createTopicArguments.entrySet()) {
            Struct singleRequestStruct = struct.instance(REQUESTS_KEY_NAME);
            String topic = createTopicArgumentsEntry.getKey();
            CreateTopicArguments args = createTopicArgumentsEntry.getValue();

            singleRequestStruct.set(TOPIC_KEY_NAME, topic);
            singleRequestStruct.set(PARTITIONS_KEY_NAME, args.partitions);
            singleRequestStruct.set(REPLICATION_FACTOR_KEY_NAME, args.replicationFactor);

            // replica assignment
            List<Struct> replicaAssignmentsStructs = new ArrayList<Struct>(args.replicasAssignments.size());
            for (PartitionReplicaAssignment partitionReplicaAssignment : args.replicasAssignments) {
                Struct replicaAssignmentStruct = singleRequestStruct.instance(REPLICA_ASSIGNMENT_KEY_NAME);
                replicaAssignmentStruct.set(REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME, partitionReplicaAssignment.partition);
                replicaAssignmentStruct.set(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME, partitionReplicaAssignment.replicas.toArray());
                replicaAssignmentsStructs.add(replicaAssignmentStruct);
            }
            singleRequestStruct.set(REPLICA_ASSIGNMENT_KEY_NAME, replicaAssignmentsStructs.toArray());

            // configs
            List<Struct> configsStructs = new ArrayList<Struct>(args.configs.size());
            for (ConfigEntry configEntry : args.configs) {
                Struct configStruct = singleRequestStruct.instance(CONFIGS_KEY_NAME);
                configStruct.set(CONFIG_KEY_KEY_NAME, configEntry.configKey());
                configStruct.set(CONFIG_VALUE_KEY_NAME, configEntry.configValue());
                configsStructs.add(configStruct);
            }
            singleRequestStruct.set(CONFIGS_KEY_NAME, configsStructs.toArray());
            createTopicRequestStructs.add(singleRequestStruct);
        }
        struct.set(REQUESTS_KEY_NAME, createTopicRequestStructs.toArray());

        this.createTopicArguments = createTopicArguments;
    }

    public CreateTopicRequest(Struct struct) {
        super(struct);

        Object[] requestStructs = struct.getArray(REQUESTS_KEY_NAME);
        createTopicArguments = new HashMap<String, CreateTopicArguments>();
        for (Object requestStructObj : requestStructs) {
            Struct singleRequestStruct = (Struct) requestStructObj;
            String topic = singleRequestStruct.getString(TOPIC_KEY_NAME);
            int partitions = singleRequestStruct.getInt(PARTITIONS_KEY_NAME);
            int replicationFactor = singleRequestStruct.getInt(REPLICATION_FACTOR_KEY_NAME);

            //replica assignment
            Object[] assignmentsArray = singleRequestStruct.getArray(REPLICA_ASSIGNMENT_KEY_NAME);
            List<PartitionReplicaAssignment> partitionReplicaAssignments = new ArrayList<PartitionReplicaAssignment>(assignmentsArray.length);
            for (Object assignmentStructObj : assignmentsArray) {
                Struct assignmentStruct = (Struct) assignmentStructObj;

                Object[] replicasArray = assignmentStruct.getArray(REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME);
                List<Integer> replicas = new ArrayList<Integer>(replicasArray.length);
                for (Object replica : replicasArray) {
                    replicas.add((Integer) replica);
                }
                PartitionReplicaAssignment assignment = new PartitionReplicaAssignment(
                        assignmentStruct.getInt(REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME),
                        replicas);

                partitionReplicaAssignments.add(assignment);
            }

            Object[] configArray = singleRequestStruct.getArray(CONFIGS_KEY_NAME);
            List<ConfigEntry> configs = new ArrayList<ConfigEntry>(configArray.length);
            for (Object configStructObj : configArray) {
                Struct configStruct = (Struct) configStructObj;
                configs.add(new ConfigEntry(configStruct.getString(CONFIG_KEY_KEY_NAME),
                        configStruct.getString(CONFIG_VALUE_KEY_NAME)));
            }

            CreateTopicArguments args = new CreateTopicArguments(
                    partitions, replicationFactor, partitionReplicaAssignments, configs);
            createTopicArguments.put(topic, args);
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<String, Errors>();
        for (String topic : createTopicArguments.keySet()) {
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

    public Map<String, CreateTopicArguments> createTopicArguments() {
        return this.createTopicArguments;
    }

    public static CreateTopicRequest parse(ByteBuffer buffer, int versionId) {
        return new CreateTopicRequest(ProtoUtils.parseRequest(ApiKeys.CREATE_TOPIC.id, versionId, buffer));
    }

    public static CreateTopicRequest parse(ByteBuffer buffer) {
        return new CreateTopicRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}