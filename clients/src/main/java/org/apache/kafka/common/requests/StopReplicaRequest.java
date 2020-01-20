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
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.MappedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StopReplicaRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<StopReplicaRequest> {
        private final boolean deletePartitions;
        private final Collection<TopicPartition> partitions;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch, boolean deletePartitions,
                       Collection<TopicPartition> partitions) {
            super(ApiKeys.STOP_REPLICA, version, controllerId, controllerEpoch, brokerEpoch);
            this.deletePartitions = deletePartitions;
            this.partitions = partitions;
        }

        public StopReplicaRequest build(short version) {
            StopReplicaRequestData data = new StopReplicaRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setDeletePartitions(deletePartitions);

            if (version >= 1) {
                Map<String, List<Integer>> topicPartitionsMap = CollectionUtils.groupPartitionsByTopic(partitions);
                List<StopReplicaTopic> topics = topicPartitionsMap.entrySet().stream().map(entry ->
                    new StopReplicaTopic()
                        .setName(entry.getKey())
                        .setPartitionIndexes(entry.getValue())
                ).collect(Collectors.toList());
                data.setTopics(topics);
            } else {
                List<StopReplicaPartitionV0> requestPartitions = partitions.stream().map(tp ->
                    new StopReplicaPartitionV0()
                        .setTopicName(tp.topic())
                        .setPartitionIndex(tp.partition())
                ).collect(Collectors.toList());
                data.setUngroupedPartitions(requestPartitions);
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

    private StopReplicaRequest(StopReplicaRequestData data, short version) {
        super(ApiKeys.STOP_REPLICA, version);
        this.data = data;
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
        for (TopicPartition tp : partitions()) {
            partitions.add(new StopReplicaPartitionError()
                .setTopicName(tp.topic())
                .setPartitionIndex(tp.partition())
                .setErrorCode(error.code()));
        }
        data.setPartitionErrors(partitions);
        return new StopReplicaResponse(data);
    }

    public boolean deletePartitions() {
        return data.deletePartitions();
    }

    /**
     * Note that this method has allocation overhead per iterated element, so callers should copy the result into
     * another collection if they need to iterate more than once.
     *
     * Implementation note: we should strive to avoid allocation overhead per element, see
     * `UpdateMetadataRequest.partitionStates()` for the preferred approach. That's not possible in this case and
     * StopReplicaRequest should be relatively rare in comparison to other request types.
     */
    public Iterable<TopicPartition> partitions() {
        if (version() >= 1) {
            return () -> new FlattenedIterator<>(data.topics().iterator(), topic ->
                new MappedIterator<>(topic.partitionIndexes().iterator(), partition ->
                    new TopicPartition(topic.name(), partition)));
        }
        return () -> new MappedIterator<>(data.ungroupedPartitions().iterator(),
            partition -> new TopicPartition(partition.topicName(), partition.partitionIndex()));
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

    // Visible for testing
    StopReplicaRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
