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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrRequestTopicState;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrRequestPartition;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderAndIsrRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {
        private final LeaderAndIsrRequestData data;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       Map<TopicPartition, LeaderAndIsrRequestPartition> partitionStates, Set<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);

            List<LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LeaderAndIsrLiveLeader()
                    .setBrokerId(n.id())
                    .setHostName(n.host())
                    .setPort(n.port())
            ).collect(Collectors.toList());

            LeaderAndIsrRequestData data = new LeaderAndIsrRequestData();
            data
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders);

            if (version >= 2) {
                Map<String, Map<Integer, LeaderAndIsrRequestPartition>> groupedMap = CollectionUtils.groupPartitionDataByTopic(partitionStates);
                List<LeaderAndIsrRequestTopicState> topicStates = groupedMap.entrySet().stream().map(entry ->
                    new LeaderAndIsrRequestTopicState()
                        .setName(entry.getKey())
                        .setPartitionStatesV0(new ArrayList<>(entry.getValue().values()))
                ).collect(Collectors.toList());
                data.setTopicStates(topicStates);
            } else {
                data.setPartitionStatesV0(new ArrayList<>(partitionStates.values()));
            }

            this.data = data;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            return new LeaderAndIsrRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LeaderAndIsrRequestData data;
    private final Map<TopicPartition, PartitionState> partitionStates;

    private LeaderAndIsrRequest(LeaderAndIsrRequestData data, short version) {
        super(ApiKeys.LEADER_AND_ISR, version, data.controllerId(), data.controllerEpoch(), data.brokerEpoch());
        this.data = data;
        partitionStates = partitionStates(data, version);
    }

    public LeaderAndIsrRequest(Struct struct, short version) {
        this(new LeaderAndIsrRequestData(struct, version), version);
    }

    private static Map<TopicPartition, PartitionState> partitionStates(LeaderAndIsrRequestData data, short version) {
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        if (version >= 2) {
            for (LeaderAndIsrRequestData.LeaderAndIsrRequestTopicState topicState : data.topicStates()) {
                for (LeaderAndIsrRequestData.LeaderAndIsrRequestPartition partitionState : topicState.partitionStatesV0()) {
                    partitionStates.put(new TopicPartition(topicState.name(), partitionState.partitionIndex()),
                            new PartitionState(partitionState));
                }
            }
        } else {
            for (LeaderAndIsrRequestData.LeaderAndIsrRequestPartition partitionState : data.partitionStatesV0()) {
                partitionStates.put(new TopicPartition(partitionState.topicName(), partitionState.partitionIndex()),
                        new PartitionState(partitionState));
            }
        }
        return partitionStates;
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

        List<LeaderAndIsrResponseData.LeaderAndIsrResponsePartition> partitions = new ArrayList<>(partitionStates.size());
        for (TopicPartition partition : partitionStates().keySet()) {
            partitions.add(new LeaderAndIsrResponseData.LeaderAndIsrResponsePartition()
                .setTopicName(partition.topic())
                .setPartitionIndex(partition.partition())
                .setErrorCode(error.code()));
        }
        responseData.setPartitions(partitions);

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
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public Map<TopicPartition, PartitionState> partitionStates() {
        return partitionStates;
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(ApiKeys.LEADER_AND_ISR.parseRequest(version, buffer), version);
    }

    public static final class PartitionState {
        public final BasePartitionState basePartitionState;
        public final List<Integer> addingReplicas;
        public final List<Integer> removingReplicas;
        public final boolean isNew;

        public PartitionState(int controllerEpoch,
                              int leader,
                              int leaderEpoch,
                              List<Integer> isr,
                              int zkVersion,
                              List<Integer> replicas,
                              boolean isNew) {
            this(controllerEpoch,
                 leader,
                 leaderEpoch,
                 isr,
                 zkVersion,
                 replicas,
                 Collections.emptyList(),
                 Collections.emptyList(),
                 isNew);
        }

        public PartitionState(int controllerEpoch,
                              int leader,
                              int leaderEpoch,
                              List<Integer> isr,
                              int zkVersion,
                              List<Integer> replicas,
                              List<Integer> addingReplicas,
                              List<Integer> removingReplicas,
                              boolean isNew) {
            this.basePartitionState = new BasePartitionState(controllerEpoch, leader, leaderEpoch, isr, zkVersion, replicas);
            this.addingReplicas = addingReplicas;
            this.removingReplicas = removingReplicas;
            this.isNew = isNew;
        }

        public PartitionState(LeaderAndIsrRequestData.LeaderAndIsrRequestPartition data) {
            this.basePartitionState = new BasePartitionState(data.controllerEpoch(), data.leaderKey(), data.leaderEpoch(),
                    data.isrReplicas(), data.zkVersion(), data.replicas());
            this.addingReplicas = data.addingReplicas();
            this.removingReplicas = data.removingReplicas();
            this.isNew = data.isNew();
        }

        @Override
        public String toString() {
            return "PartitionState(controllerEpoch=" + basePartitionState.controllerEpoch +
                ", leader=" + basePartitionState.leader +
                ", leaderEpoch=" + basePartitionState.leaderEpoch +
                ", isr=" + Utils.join(basePartitionState.isr, ",") +
                ", zkVersion=" + basePartitionState.zkVersion +
                ", replicas=" + Utils.join(basePartitionState.replicas, ",") +
                ", addingReplicas=" + Utils.join(addingReplicas, ",") +
                ", removingReplicas=" + Utils.join(removingReplicas, ",") +
                ", isNew=" + isNew + ")";
        }
    }
}
