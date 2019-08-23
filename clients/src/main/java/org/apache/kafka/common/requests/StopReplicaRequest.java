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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class StopReplicaRequest extends AbstractControlRequest {
    private static final Field.Bool DELETE_PARTITIONS = new Field.Bool("delete_partitions", "Boolean which indicates if replica's partitions must be deleted.");
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions", "The partitions");
    private static final Field.Array PARTITION_IDS = new Field.Array("partition_ids", INT32, "The partition ids of a topic");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            TOPIC_NAME,
            PARTITION_ID);
    private static final Field PARTITIONS_V1 = PARTITIONS.withFields(
            TOPIC_NAME,
            PARTITION_IDS);

    private static final Schema STOP_REPLICA_REQUEST_V0 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            DELETE_PARTITIONS,
            PARTITIONS_V0);

    // STOP_REPLICA_REQUEST_V1 added a broker_epoch Field. This field specifies the generation of the broker across
    // bounces. It also normalizes partitions under each topic.
    private static final Schema STOP_REPLICA_REQUEST_V1 = new Schema(
            CONTROLLER_ID,
            CONTROLLER_EPOCH,
            BROKER_EPOCH,
            DELETE_PARTITIONS,
            PARTITIONS_V1);


    public static Schema[] schemaVersions() {
        return new Schema[] {STOP_REPLICA_REQUEST_V0, STOP_REPLICA_REQUEST_V1};
    }

    public static class Builder extends AbstractControlRequest.Builder<StopReplicaRequest> {
        private final boolean deletePartitions;
        private final Collection<TopicPartition> partitions;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch, boolean deletePartitions,
                       Collection<TopicPartition> partitions) {
            super(ApiKeys.STOP_REPLICA, version, controllerId, controllerEpoch, brokerEpoch);
            this.deletePartitions = deletePartitions;
            this.partitions = partitions;
        }

        @Override
        public StopReplicaRequest build(short version) {
            return new StopReplicaRequest(controllerId, controllerEpoch, brokerEpoch,
                    deletePartitions, partitions, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=StopReplicaRequest").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", deletePartitions=").append(deletePartitions).
                append(", brokerEpoch=").append(brokerEpoch).
                append(", partitions=").append(Utils.join(partitions, ",")).
                append(")");
            return bld.toString();
        }
    }

    private final boolean deletePartitions;
    private final Collection<TopicPartition> partitions;

    private StopReplicaRequest(int controllerId, int controllerEpoch, long brokerEpoch, boolean deletePartitions,
                               Collection<TopicPartition> partitions, short version) {
        super(ApiKeys.STOP_REPLICA, version, controllerId, controllerEpoch, brokerEpoch);
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest(Struct struct, short version) {
        super(ApiKeys.STOP_REPLICA, struct, version);

        partitions = new HashSet<>();
        if (version > 0) { // V1
            for (Object topicObj : struct.get(PARTITIONS)) {
                Struct topicData = (Struct) topicObj;
                String topic = topicData.get(TOPIC_NAME);
                for (Object partitionObj : topicData.get(PARTITION_IDS)) {
                    int partition = (Integer) partitionObj;
                    partitions.add(new TopicPartition(topic, partition));
                }
            }
        } else { // V0
            for (Object partitionDataObj : struct.get(PARTITIONS)) {
                Struct partitionData = (Struct) partitionDataObj;
                String topic = partitionData.get(TOPIC_NAME);
                int partition = partitionData.get(PARTITION_ID);
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        deletePartitions = struct.get(DELETE_PARTITIONS);
    }

    @Override
    public StopReplicaResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        Map<TopicPartition, Errors> responses = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions) {
            responses.put(partition, error);
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new StopReplicaResponse(error, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.STOP_REPLICA.latestVersion()));
        }
    }

    public boolean deletePartitions() {
        return deletePartitions;
    }

    public Collection<TopicPartition> partitions() {
        return partitions;
    }

    public static StopReplicaRequest parse(ByteBuffer buffer, short version) {
        return new StopReplicaRequest(ApiKeys.STOP_REPLICA.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.STOP_REPLICA.requestSchema(version()));

        struct.set(CONTROLLER_ID, controllerId);
        struct.set(CONTROLLER_EPOCH, controllerEpoch);
        struct.setIfExists(BROKER_EPOCH, brokerEpoch);
        struct.set(DELETE_PARTITIONS, deletePartitions);

        if (version() > 0) { // V1
            Map<String, List<Integer>> topicPartitionsMap = CollectionUtils.groupPartitionsByTopic(partitions);
            List<Struct> topicsData = new ArrayList<>(topicPartitionsMap.size());
            for (Map.Entry<String, List<Integer>> entry : topicPartitionsMap.entrySet()) {
                Struct topicData = struct.instance(PARTITIONS);
                topicData.set(TOPIC_NAME, entry.getKey());
                topicData.set(PARTITION_IDS, entry.getValue().toArray());
                topicsData.add(topicData);
            }
            struct.set(PARTITIONS, topicsData.toArray());

        } else { // V0
            List<Struct> partitionDatas = new ArrayList<>(partitions.size());
            for (TopicPartition partition : partitions) {
                Struct partitionData = struct.instance(PARTITIONS);
                partitionData.set(TOPIC_NAME, partition.topic());
                partitionData.set(PARTITION_ID, partition.partition());
                partitionDatas.add(partitionData);
            }
            struct.set(PARTITIONS, partitionDatas.toArray());
        }
        return struct;
    }
}
