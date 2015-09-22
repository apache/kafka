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

public class AlterTopicRequest extends AbstractRequest {
    public static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.ALTER_TOPIC.id);
    private static final String REQUESTS_KEY_NAME = "alter_topic_requests";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String REPLICATION_FACTOR_KEY_NAME = "replication_factor";

    private static final String REPLICA_ASSIGNMENT_KEY_NAME = "replica_assignment";
    private static final String REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME = "partition_id";
    private static final String REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME = "replicas";

    public static final class AlterTopicArguments {
        public final int partitions;
        public final int replicationFactor;
        public final List<PartitionReplicaAssignment> replicasAssignments;

        public AlterTopicArguments(int partitions,
                                   int replicationFactor,
                                   List<PartitionReplicaAssignment> replicasAssignments) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.replicasAssignments = replicasAssignments;
        }
    }

    private final Map<String, AlterTopicArguments> alterTopicArguments;

    public static final int NO_PARTITIONS_SIGN = -1;
    public static final int NO_REPLICATION_FACTOR_SIGN = -1;

    public AlterTopicRequest(Map<String, AlterTopicArguments> alterTopicArguments) {
        super(new Struct(CURRENT_SCHEMA));

        List<Struct> alterTopicRequestStructs = new ArrayList<Struct>(alterTopicArguments.size());
        for (Map.Entry<String, AlterTopicArguments> alterTopicArgumentsEntry : alterTopicArguments.entrySet()) {
            Struct singleRequestStruct = struct.instance(REQUESTS_KEY_NAME);
            String topic = alterTopicArgumentsEntry.getKey();
            AlterTopicArguments args = alterTopicArgumentsEntry.getValue();

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

            alterTopicRequestStructs.add(singleRequestStruct);
        }
        struct.set(REQUESTS_KEY_NAME, alterTopicRequestStructs.toArray());

        this.alterTopicArguments = alterTopicArguments;
    }

    public AlterTopicRequest(Struct struct) {
        super(struct);

        Object[] requestStructs = struct.getArray(REQUESTS_KEY_NAME);
        this.alterTopicArguments = new HashMap<String, AlterTopicArguments>();
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

            AlterTopicArguments args = new AlterTopicArguments(
                    partitions, replicationFactor, partitionReplicaAssignments);
            alterTopicArguments.put(topic, args);
        }
    }

    @Override
    public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<String, Errors>();
        for (String topic : alterTopicArguments.keySet()) {
            topicErrors.put(topic, Errors.forException(e));
        }

        switch (versionId) {
            case 0:
                return new AlterTopicResponse(topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.ALTER_TOPIC.id)));
        }
    }


    public Map<String, AlterTopicArguments> alterTopicArguments() {
        return alterTopicArguments;
    }

    public static AlterTopicRequest parse(ByteBuffer buffer, int versionId) {
        return new AlterTopicRequest(ProtoUtils.parseRequest(ApiKeys.ALTER_TOPIC.id, versionId, buffer));
    }

    public static AlterTopicRequest parse(ByteBuffer buffer) {
        return new AlterTopicRequest((Struct) CURRENT_SCHEMA.read(buffer));
    }
}