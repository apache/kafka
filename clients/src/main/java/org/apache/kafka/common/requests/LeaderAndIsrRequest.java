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
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderAndIsrRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {

        private final List<LeaderAndIsrPartitionState> partitionStates;
        private final Set<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<LeaderAndIsrPartitionState> partitionStates, Set<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
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
                Map<String, List<LeaderAndIsrPartitionState>> groupedMap = groupByTopic(partitionStates);
                // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
                // the generated code if version >= 2
                List<LeaderAndIsrTopicState> topicStates = groupedMap.entrySet().stream().map(entry ->
                    new LeaderAndIsrTopicState()
                        .setTopicName(entry.getKey())
                        .setPartitionStates(entry.getValue())
                ).collect(Collectors.toList());
                data.setTopicStates(topicStates);
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            return new LeaderAndIsrRequest(data, version);
        }

        private static Map<String, List<LeaderAndIsrPartitionState>> groupByTopic(List<LeaderAndIsrPartitionState> partitionStates) {
            Map<String, List<LeaderAndIsrPartitionState>> dataByTopic = new HashMap<>();
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                List<LeaderAndIsrPartitionState> topicData = dataByTopic.computeIfAbsent(partition.topicName(),
                    t -> new ArrayList<>());
                topicData.add(partition);
            }
            return dataByTopic;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();

        }
    }

    private final LeaderAndIsrRequestData data;
    private volatile List<LeaderAndIsrPartitionState> partitionStates;

    private LeaderAndIsrRequest(LeaderAndIsrRequestData data, short version) {
        super(ApiKeys.LEADER_AND_ISR, version);
        this.data = data;
    }

    public LeaderAndIsrRequest(Struct struct, short version) {
        this(new LeaderAndIsrRequestData(struct, version), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public LeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaderAndIsrResponseData responseData = new LeaderAndIsrResponseData();
        Errors error = Errors.forException(e);
        responseData.setErrorCode(error.code());

        List<LeaderAndIsrPartitionError> partitions = new ArrayList<>(partitionStates().size());
        for (LeaderAndIsrPartitionState partition : partitionStates()) {
            partitions.add(new LeaderAndIsrPartitionError()
                .setTopicName(partition.topicName())
                .setPartitionIndex(partition.partitionIndex())
                .setErrorCode(error.code()));
        }
        responseData.setPartitionErrors(partitions);

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
            case 3:
                return new LeaderAndIsrResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LEADER_AND_ISR.latestVersion()));
        }
    }

    public int controllerId() {
        return data.controllerId();
    }

    public int controllerEpoch() {
        return data.controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        return data.brokerEpoch();
    }

    public List<LeaderAndIsrPartitionState> partitionStates() {
        if (partitionStates == null) {
            synchronized (data) {
                if (partitionStates == null) {
                    if (version() >= 2) {
                        List<LeaderAndIsrPartitionState> partitionStates = new ArrayList<>();
                        for (LeaderAndIsrTopicState topicState : data.topicStates()) {
                            for (LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
                                // Set the topic name so that we can always present the ungrouped view to callers
                                partitionState.setTopicName(topicState.topicName());
                                partitionStates.add(partitionState);
                            }
                        }
                        this.partitionStates = partitionStates;
                    } else {
                        partitionStates = data.ungroupedPartitionStates();
                    }
                }
            }
        }
        return partitionStates;
    }

    protected int size() {
        return data.size(version());
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(ApiKeys.LEADER_AND_ISR.parseRequest(version, buffer), version);
    }
}
