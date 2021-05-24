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
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionV0;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicV1;
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaTopicState;
import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.MappedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StopReplicaRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<StopReplicaRequest> {
        private final boolean deletePartitions;
        private final List<StopReplicaTopicState> topicStates;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       boolean deletePartitions, List<StopReplicaTopicState> topicStates) {
            super(ApiKeys.STOP_REPLICA, version, controllerId, controllerEpoch, brokerEpoch);
            this.deletePartitions = deletePartitions;
            this.topicStates = topicStates;
        }

        public StopReplicaRequest build(short version) {
            StopReplicaRequestData data = new StopReplicaRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch);

            if (version >= 3) {
                data.setTopicStates(topicStates);
            } else if (version >= 1) {
                data.setDeletePartitions(deletePartitions);
                List<StopReplicaTopicV1> topics = topicStates.stream().map(topic ->
                    new StopReplicaTopicV1()
                        .setName(topic.topicName())
                        .setPartitionIndexes(topic.partitionStates().stream()
                            .map(StopReplicaPartitionState::partitionIndex)
                            .collect(Collectors.toList())))
                    .collect(Collectors.toList());
                data.setTopics(topics);
            } else {
                data.setDeletePartitions(deletePartitions);
                List<StopReplicaPartitionV0> partitions = topicStates.stream().flatMap(topic ->
                    topic.partitionStates().stream().map(partition ->
                        new StopReplicaPartitionV0()
                            .setTopicName(topic.topicName())
                            .setPartitionIndex(partition.partitionIndex())))
                    .collect(Collectors.toList());
                data.setUngroupedPartitions(partitions);
            }

            return new StopReplicaRequest(data, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=StopReplicaRequest").
                append(", controllerId=").append(controllerId).
                append(", controllerEpoch=").append(controllerEpoch).
                append(", brokerEpoch=").append(brokerEpoch).
                append(", deletePartitions=").append(deletePartitions).
                append(", topicStates=").append(Utils.join(topicStates, ",")).
                append(")");
            return bld.toString();
        }
    }

    private final StopReplicaRequestData data;

    private StopReplicaRequest(StopReplicaRequestData data, short version) {
        super(ApiKeys.STOP_REPLICA, version);
        this.data = data;
    }

    @Override
    public StopReplicaResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        StopReplicaResponseData data = new StopReplicaResponseData();
        data.setErrorCode(error.code());

        List<StopReplicaPartitionError> partitions = new ArrayList<>();
        for (StopReplicaTopicState topic : topicStates()) {
            for (StopReplicaPartitionState partition : topic.partitionStates()) {
                partitions.add(new StopReplicaPartitionError()
                    .setTopicName(topic.topicName())
                    .setPartitionIndex(partition.partitionIndex())
                    .setErrorCode(error.code()));
            }
        }
        data.setPartitionErrors(partitions);

        return new StopReplicaResponse(data);
    }

    /**
     * Note that this method has allocation overhead per iterated element, so callers should copy the result into
     * another collection if they need to iterate more than once.
     *
     * Implementation note: we should strive to avoid allocation overhead per element, see
     * `UpdateMetadataRequest.partitionStates()` for the preferred approach. That's not possible in this case and
     * StopReplicaRequest should be relatively rare in comparison to other request types.
     */
    public Iterable<StopReplicaTopicState> topicStates() {
        if (version() < 1) {
            Map<String, StopReplicaTopicState> topicStates = new HashMap<>();
            for (StopReplicaPartitionV0 partition : data.ungroupedPartitions()) {
                StopReplicaTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    topic -> new StopReplicaTopicState().setTopicName(topic));
                topicState.partitionStates().add(new StopReplicaPartitionState()
                    .setPartitionIndex(partition.partitionIndex())
                    .setDeletePartition(data.deletePartitions()));
            }
            return topicStates.values();
        } else if (version() < 3) {
            return () -> new MappedIterator<>(data.topics().iterator(), topic ->
                new StopReplicaTopicState()
                    .setTopicName(topic.name())
                    .setPartitionStates(topic.partitionIndexes().stream()
                        .map(partition -> new StopReplicaPartitionState()
                            .setPartitionIndex(partition)
                            .setDeletePartition(data.deletePartitions()))
                        .collect(Collectors.toList())));
        } else {
            return data.topicStates();
        }
    }

    public Map<TopicPartition, StopReplicaPartitionState> partitionStates() {
        Map<TopicPartition, StopReplicaPartitionState> partitionStates = new HashMap<>();

        if (version() < 1) {
            for (StopReplicaPartitionV0 partition : data.ungroupedPartitions()) {
                partitionStates.put(
                    new TopicPartition(partition.topicName(), partition.partitionIndex()),
                    new StopReplicaPartitionState()
                        .setPartitionIndex(partition.partitionIndex())
                        .setDeletePartition(data.deletePartitions()));
            }
        } else if (version() < 3) {
            for (StopReplicaTopicV1 topic : data.topics()) {
                for (Integer partitionIndex : topic.partitionIndexes()) {
                    partitionStates.put(
                        new TopicPartition(topic.name(), partitionIndex),
                        new StopReplicaPartitionState()
                            .setPartitionIndex(partitionIndex)
                            .setDeletePartition(data.deletePartitions()));
                }
            }
        } else {
            for (StopReplicaTopicState topicState : data.topicStates()) {
                for (StopReplicaPartitionState partitionState: topicState.partitionStates()) {
                    partitionStates.put(
                        new TopicPartition(topicState.topicName(), partitionState.partitionIndex()),
                        partitionState);
                }
            }
        }

        return partitionStates;
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
        return new StopReplicaRequest(new StopReplicaRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public StopReplicaRequestData data() {
        return data;
    }
}
