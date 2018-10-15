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

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class CreatePartitionsRequest extends AbstractRequest {

    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String NEW_PARTITIONS_KEY_NAME = "new_partitions";
    private static final String COUNT_KEY_NAME = "count";
    private static final String ASSIGNMENT_KEY_NAME = "assignment";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String VALIDATE_ONLY_KEY_NAME = "validate_only";

    private static final Schema CREATE_PARTITIONS_REQUEST_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(
                    new Schema(
                            TOPIC_NAME,
                            new Field(NEW_PARTITIONS_KEY_NAME, new Schema(
                                    new Field(COUNT_KEY_NAME, INT32, "The new partition count."),
                                    new Field(ASSIGNMENT_KEY_NAME, ArrayOf.nullable(new ArrayOf(INT32)),
                                            "The assigned brokers.")
                            )))),
                    "List of topic and the corresponding new partitions."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for the partitions to be created."),
            new Field(VALIDATE_ONLY_KEY_NAME, BOOLEAN,
                    "If true then validate the request, but don't actually increase the number of partitions."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema CREATE_PARTITIONS_REQUEST_V1 = CREATE_PARTITIONS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_PARTITIONS_REQUEST_V0, CREATE_PARTITIONS_REQUEST_V1};
    }

    // It is an error for duplicate topics to be present in the request,
    // so track duplicates here to allow KafkaApis to report per-topic errors.
    private final Set<String> duplicates;
    private final Map<String, PartitionDetails> newPartitions;
    private final int timeout;
    private final boolean validateOnly;

    public static class PartitionDetails {

        private final int totalCount;

        private final List<List<Integer>> newAssignments;

        public PartitionDetails(int totalCount) {
            this(totalCount, null);
        }

        public PartitionDetails(int totalCount, List<List<Integer>> newAssignments) {
            this.totalCount = totalCount;
            this.newAssignments = newAssignments;
        }

        public int totalCount() {
            return totalCount;
        }

        public List<List<Integer>> newAssignments() {
            return newAssignments;
        }

        @Override
        public String toString() {
            return "(totalCount=" + totalCount() + ", newAssignments=" + newAssignments() + ")";
        }

    }

    public static class Builder extends AbstractRequest.Builder<CreatePartitionsRequest> {

        private final Map<String, PartitionDetails> newPartitions;
        private final int timeout;
        private final boolean validateOnly;

        public Builder(Map<String, PartitionDetails> newPartitions, int timeout, boolean validateOnly) {
            super(ApiKeys.CREATE_PARTITIONS);
            this.newPartitions = newPartitions;
            this.timeout = timeout;
            this.validateOnly = validateOnly;
        }

        @Override
        public CreatePartitionsRequest build(short version) {
            return new CreatePartitionsRequest(newPartitions, timeout, validateOnly, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=CreatePartitionsRequest").
                    append(", newPartitions=").append(newPartitions).
                    append(", timeout=").append(timeout).
                    append(", validateOnly=").append(validateOnly).
                    append(")");
            return bld.toString();
        }
    }

    CreatePartitionsRequest(Map<String, PartitionDetails> newPartitions, int timeout, boolean validateOnly, short apiVersion) {
        super(ApiKeys.CREATE_PARTITIONS, apiVersion);
        this.newPartitions = newPartitions;
        this.duplicates = Collections.emptySet();
        this.timeout = timeout;
        this.validateOnly = validateOnly;
    }

    public CreatePartitionsRequest(Struct struct, short apiVersion) {
        super(ApiKeys.CREATE_PARTITIONS, apiVersion);
        Object[] topicCountArray = struct.getArray(TOPIC_PARTITIONS_KEY_NAME);
        Map<String, PartitionDetails> counts = new HashMap<>(topicCountArray.length);
        Set<String> dupes = new HashSet<>();
        for (Object topicPartitionCountObj : topicCountArray) {
            Struct topicPartitionCountStruct = (Struct) topicPartitionCountObj;
            String topic = topicPartitionCountStruct.get(TOPIC_NAME);
            Struct partitionCountStruct = topicPartitionCountStruct.getStruct(NEW_PARTITIONS_KEY_NAME);
            int count = partitionCountStruct.getInt(COUNT_KEY_NAME);
            Object[] assignmentsArray = partitionCountStruct.getArray(ASSIGNMENT_KEY_NAME);
            PartitionDetails newPartition;
            if (assignmentsArray != null) {
                List<List<Integer>> assignments = new ArrayList<>(assignmentsArray.length);
                for (Object replicas : assignmentsArray) {
                    Object[] replicasArray = (Object[]) replicas;
                    List<Integer> replicasList = new ArrayList<>(replicasArray.length);
                    assignments.add(replicasList);
                    for (Object broker : replicasArray) {
                        replicasList.add((Integer) broker);
                    }
                }
                newPartition = new PartitionDetails(count, assignments);
            } else {
                newPartition = new PartitionDetails(count);
            }
            PartitionDetails dupe = counts.put(topic, newPartition);
            if (dupe != null) {
                dupes.add(topic);
            }
        }
        this.newPartitions = counts;
        this.duplicates = dupes;
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
        this.validateOnly = struct.getBoolean(VALIDATE_ONLY_KEY_NAME);
    }

    public Set<String> duplicates() {
        return duplicates;
    }

    public Map<String, PartitionDetails> newPartitions() {
        return newPartitions;
    }

    public int timeout() {
        return timeout;
    }

    public boolean validateOnly() {
        return validateOnly;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.CREATE_PARTITIONS.requestSchema(version()));
        List<Struct> topicPartitionsList = new ArrayList<>();
        for (Map.Entry<String, PartitionDetails> topicPartitionCount : this.newPartitions.entrySet()) {
            Struct topicPartitionCountStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionCountStruct.set(TOPIC_NAME, topicPartitionCount.getKey());
            PartitionDetails partitionDetails = topicPartitionCount.getValue();
            Struct partitionCountStruct = topicPartitionCountStruct.instance(NEW_PARTITIONS_KEY_NAME);
            partitionCountStruct.set(COUNT_KEY_NAME, partitionDetails.totalCount());
            Object[][] assignments = null;
            if (partitionDetails.newAssignments() != null) {
                assignments = new Object[partitionDetails.newAssignments().size()][];
                int i = 0;
                for (List<Integer> partitionAssignment : partitionDetails.newAssignments()) {
                    assignments[i] = partitionAssignment.toArray(new Object[0]);
                    i++;
                }
            }
            partitionCountStruct.set(ASSIGNMENT_KEY_NAME, assignments);
            topicPartitionCountStruct.set(NEW_PARTITIONS_KEY_NAME, partitionCountStruct);
            topicPartitionsList.add(topicPartitionCountStruct);
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicPartitionsList.toArray(new Object[0]));
        struct.set(TIMEOUT_KEY_NAME, this.timeout);
        struct.set(VALIDATE_ONLY_KEY_NAME, this.validateOnly);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        for (String topic : newPartitions.keySet()) {
            topicErrors.put(topic, ApiError.fromThrowable(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new CreatePartitionsResponse(throttleTimeMs, topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CREATE_PARTITIONS.latestVersion()));
        }
    }

    public static CreatePartitionsRequest parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsRequest(ApiKeys.CREATE_PARTITIONS.parseRequest(version, buffer), version);
    }
}
