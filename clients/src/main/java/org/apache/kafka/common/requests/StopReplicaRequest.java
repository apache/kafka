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
import org.apache.kafka.common.message.StopReplicaRequestData;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionV0;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopic;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StopReplicaRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<StopReplicaRequest> {

        private final Collection<TopicPartition> partitions;
        private final boolean deletePartitions;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch, boolean deletePartitions,
                       Collection<TopicPartition> partitions) {
            super(ApiKeys.STOP_REPLICA, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitions = partitions;
            this.deletePartitions = deletePartitions;
        }

        public StopReplicaRequest build(short version) {
            StopReplicaRequestData data = new StopReplicaRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setDeletePartitions(deletePartitions);

            if (version == 0) {
                List<StopReplicaPartitionV0> requestPartitions = partitions.stream().map(tp ->
                    new StopReplicaPartitionV0()
                        .setTopicName(tp.topic())
                        .setPartitionIndex(tp.partition())
                ).collect(Collectors.toList());
                data.setPartitionsV0(requestPartitions);
            } else {
                Map<String, List<Integer>> topicPartitionsMap = CollectionUtils.groupPartitionsByTopic(partitions);
                List<StopReplicaTopic> topics = topicPartitionsMap.entrySet().stream().map(entry ->
                    new StopReplicaTopic()
                        .setName(entry.getKey())
                        .setPartitionIndexes(entry.getValue())
                ).collect(Collectors.toList());
                data.setTopics(topics);
            }

            return new StopReplicaRequest(data, version);
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

    private final StopReplicaRequestData data;
    private final Collection<TopicPartition> topicPartitions;

    private StopReplicaRequest(StopReplicaRequestData data, short version) {
        super(ApiKeys.STOP_REPLICA, version);
        this.data = data;
        Stream<TopicPartition> partitionStream;
        if (version == 0) {
            partitionStream = data.partitionsV0().stream()
                    .map(tp -> new TopicPartition(tp.topicName(), tp.partitionIndex()));
        } else {
            partitionStream = data.topics().stream().flatMap(topic ->
                topic.partitionIndexes().stream().map(partitionIndex -> new TopicPartition(topic.name(), partitionIndex))
            );
        }
        topicPartitions = Collections.unmodifiableList(partitionStream.collect(Collectors.toList()));
    }

    public StopReplicaRequest(Struct struct, short version) {
        this(new StopReplicaRequestData(struct, version), version);
    }

    @Override
    public StopReplicaResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        StopReplicaResponseData data = new StopReplicaResponseData();
        data.setErrorCode(error.code());
        List<StopReplicaPartitionError> partitions = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            partitions.add(new StopReplicaPartitionError()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition()));
        }
        data.setPartitionErrors(partitions);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new StopReplicaResponse(data);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.STOP_REPLICA.latestVersion()));
        }
    }

    public boolean deletePartitions() {
        return data.deletePartitions();
    }

    public Collection<TopicPartition> partitions() {
        return topicPartitions;
    }

    @Override
    public int controllerId() {
        return data.controllerId();
    }

    @Override
    public int controllerEpoch() {
        return data.controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        return data.brokerEpoch();
    }

    public static StopReplicaRequest parse(ByteBuffer buffer, short version) {
        return new StopReplicaRequest(ApiKeys.STOP_REPLICA.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    protected long size() {
        return data.size(version());
    }

}
