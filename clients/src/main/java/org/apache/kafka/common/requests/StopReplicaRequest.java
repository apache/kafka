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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StopReplicaRequest extends AbstractRequest {
    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String DELETE_PARTITIONS_KEY_NAME = "delete_partitions";
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_KEY_NAME = "partition";

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
        public StopReplicaRequest build() {
            return new StopReplicaRequest(controllerId, controllerEpoch,
                    deletePartitions, partitions, version());
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
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.STOP_REPLICA.id, version)), version);

        struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
        struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);
        struct.set(DELETE_PARTITIONS_KEY_NAME, deletePartitions);

        List<Struct> partitionDatas = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions) {
            Struct partitionData = struct.instance(PARTITIONS_KEY_NAME);
            partitionData.set(TOPIC_KEY_NAME, partition.topic());
            partitionData.set(PARTITION_KEY_NAME, partition.partition());
            partitionDatas.add(partitionData);
        }

        struct.set(PARTITIONS_KEY_NAME, partitionDatas.toArray());

        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest(Struct struct, short versionId) {
        super(struct, versionId);

        partitions = new HashSet<>();
        for (Object partitionDataObj : struct.getArray(PARTITIONS_KEY_NAME)) {
            Struct partitionData = (Struct) partitionDataObj;
            String topic = partitionData.getString(TOPIC_KEY_NAME);
            int partition = partitionData.getInt(PARTITION_KEY_NAME);
            partitions.add(new TopicPartition(topic, partition));
        }

        controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
        controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
        deletePartitions = struct.getBoolean(DELETE_PARTITIONS_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Map<TopicPartition, Short> responses = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions) {
            responses.put(partition, Errors.forException(e).code());
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new StopReplicaResponse(Errors.NONE.code(), responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.STOP_REPLICA.id)));
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

    public static StopReplicaRequest parse(ByteBuffer buffer, int versionId) {
        return new StopReplicaRequest(ProtoUtils.parseRequest(ApiKeys.STOP_REPLICA.id, versionId, buffer),
                (short) versionId);
    }

    public static StopReplicaRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.STOP_REPLICA.id));
    }
}
