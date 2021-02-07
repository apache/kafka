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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrTopicError;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LeaderAndIsrRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {

        private final List<LeaderAndIsrPartitionState> partitionStates;
        private final Map<String, Uuid> topicIds;
        private final Collection<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<LeaderAndIsrPartitionState> partitionStates, Map<String, Uuid> topicIds,
                       Collection<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
            this.topicIds = topicIds;
            this.liveLeaders = liveLeaders;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            List<LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            LeaderAndIsrRequestData data = new LeaderAndIsrRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders);

            if (version >= 2) {
                Map<String, LeaderAndIsrTopicState> topicStatesMap = groupByTopic(partitionStates, topicIds);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            return new LeaderAndIsrRequest(data, version);
        }

        private static Map<String, LeaderAndIsrTopicState> groupByTopic(List<LeaderAndIsrPartitionState> partitionStates, Map<String, Uuid> topicIds) {
            Map<String, LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version >= 2
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(), t -> new LeaderAndIsrTopicState()
                                .setTopicName(partition.topicName())
                                .setTopicId(topicIds.getOrDefault(partition.topicName(), Uuid.ZERO_UUID)));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", topicIds=").append(topicIds)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();

        }
    }

    private final LeaderAndIsrRequestData data;

    LeaderAndIsrRequest(LeaderAndIsrRequestData data, short version) {
        super(ApiKeys.LEADER_AND_ISR, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        if (version() >= 2) {
            for (LeaderAndIsrTopicState topicState : data.topicStates()) {
                for (LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
                    // Set the topic name so that we can always present the ungrouped view to callers
                    partitionState.setTopicName(topicState.topicName());
                }
            }
        }
    }

    @Override
    public LeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaderAndIsrResponseData responseData = new LeaderAndIsrResponseData();
        Errors error = Errors.forException(e);
        responseData.setErrorCode(error.code());

        if (version() < 5) {
            List<LeaderAndIsrPartitionError> partitions = new ArrayList<>();
            for (LeaderAndIsrPartitionState partition : partitionStates()) {
                partitions.add(new LeaderAndIsrPartitionError()
                        .setTopicName(partition.topicName())
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(error.code()));
            }
            responseData.setPartitionErrors(partitions);
            return new LeaderAndIsrResponse(responseData, version());
        }

        List<LeaderAndIsrTopicError> topics = new ArrayList<>(data.topicStates().size());
        Map<String, Uuid> topicIds = topicIds();
        for (LeaderAndIsrTopicState topicState : data.topicStates()) {
            LeaderAndIsrTopicError topicError = new LeaderAndIsrTopicError();
            topicError.setTopicId(topicIds.get(topicState.topicName()));
            List<LeaderAndIsrPartitionError> partitions = new ArrayList<>(topicState.partitionStates().size());
            for (LeaderAndIsrPartitionState partition : topicState.partitionStates()) {
                partitions.add(new LeaderAndIsrPartitionError()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(error.code()));
            }
            topicError.setPartitionErrors(partitions);
            topics.add(topicError);
        }
        responseData.setTopics(topics);
        return new LeaderAndIsrResponse(responseData, version());
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

    public Iterable<LeaderAndIsrPartitionState> partitionStates() {
        if (version() >= 2)
            return () -> new FlattenedIterator<>(data.topicStates().iterator(),
                topicState -> topicState.partitionStates().iterator());
        return data.ungroupedPartitionStates();
    }

    public Map<String, Uuid> topicIds() {
        return data.topicStates().stream()
                .collect(Collectors.toMap(LeaderAndIsrTopicState::topicName, LeaderAndIsrTopicState::topicId));
    }

    public List<LeaderAndIsrLiveLeader> liveLeaders() {
        return Collections.unmodifiableList(data.liveLeaders());
    }

    @Override
    public LeaderAndIsrRequestData data() {
        return data;
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(new LeaderAndIsrRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
