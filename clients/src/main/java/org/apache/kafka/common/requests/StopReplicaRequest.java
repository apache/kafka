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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class StopReplicaRequest extends AbstractRequest {
    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String DELETE_PARTITIONS_KEY_NAME = "delete_partitions";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema STOP_REPLICA_REQUEST_PARTITION_V0 = new Schema(
            TOPIC_NAME,
            PARTITION_ID);
    private static final Schema STOP_REPLICA_REQUEST_V0 = new Schema(
            new Field(CONTROLLER_ID_KEY_NAME, INT32, "The controller id."),
            new Field(CONTROLLER_EPOCH_KEY_NAME, INT32, "The controller epoch."),
            new Field(DELETE_PARTITIONS_KEY_NAME, BOOLEAN, "Boolean which indicates if replica's partitions must be deleted."),
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(STOP_REPLICA_REQUEST_PARTITION_V0)));

    public static Schema[] schemaVersions() {
        return new Schema[] {STOP_REPLICA_REQUEST_V0};
    }

    public static class Builder extends AbstractRequest.Builder<StopReplicaRequest> {
        private final int controllerId;
        private final int controllerEpoch;
        private final boolean deletePartitions;
        private final Set<TopicPartition> partitions;

        public Builder(int controllerId, int controllerEpoch, boolean deletePartitions,
                       Set<TopicPartition> partitions) {
            super(ApiKeys.STOP_REPLICA);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.deletePartitions = deletePartitions;
            this.partitions = partitions;
        }

        @Override
        public StopReplicaRequest build(short version) {
            return new StopReplicaRequest(controllerId, controllerEpoch,
                    deletePartitions, partitions, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=StopReplicaRequest").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", deletePartitions=").append(deletePartitions).
                append(", partitions=").append(Utils.join(partitions, ",")).
                append(")");
            return bld.toString();
        }
    }

    private final int controllerId;
    private final int controllerEpoch;
    private final boolean deletePartitions;
    private final Set<TopicPartition> partitions;

    private StopReplicaRequest(int controllerId, int controllerEpoch, boolean deletePartitions,
                               Set<TopicPartition> partitions, short version) {
        super(version);
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest(Struct struct, short version) {
        super(version);

        partitions = new HashSet<>();
        for (Object partitionDataObj : struct.getArray(PARTITIONS_KEY_NAME)) {
            Struct partitionData = (Struct) partitionDataObj;
            String topic = partitionData.get(TOPIC_NAME);
            int partition = partitionData.get(PARTITION_ID);
            partitions.add(new TopicPartition(topic, partition));
        }

        controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
        controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
        deletePartitions = struct.getBoolean(DELETE_PARTITIONS_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, Errors> responses = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions) {
            responses.put(partition, Errors.forException(e));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new StopReplicaResponse(Errors.NONE, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.STOP_REPLICA.latestVersion()));
        }
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public boolean deletePartitions() {
        return deletePartitions;
    }

    public Set<TopicPartition> partitions() {
        return partitions;
    }

    public static StopReplicaRequest parse(ByteBuffer buffer, short version) {
        return new StopReplicaRequest(ApiKeys.STOP_REPLICA.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.STOP_REPLICA.requestSchema(version()));

        struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
        struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);
        struct.set(DELETE_PARTITIONS_KEY_NAME, deletePartitions);

        List<Struct> partitionDatas = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions) {
            Struct partitionData = struct.instance(PARTITIONS_KEY_NAME);
            partitionData.set(TOPIC_NAME, partition.topic());
            partitionData.set(PARTITION_ID, partition.partition());
            partitionDatas.add(partitionData);
        }

        struct.set(PARTITIONS_KEY_NAME, partitionDatas.toArray());
        return struct;
    }
}
