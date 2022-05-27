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
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.DescribeQuorumResponseData.TopicData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Possible error codes.
 *
 * Top level errors:
 * - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * - {@link Errors#BROKER_NOT_AVAILABLE}
 *
 * Partition level errors:
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 */
public class DescribeQuorumResponse extends AbstractResponse {
    private final DescribeQuorumResponseData data;

    public DescribeQuorumResponse(DescribeQuorumResponseData data) {
        super(ApiKeys.DESCRIBE_QUORUM);
        this.data = data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();

        errors.put(Errors.forCode(data.errorCode()), 1);

        for (DescribeQuorumResponseData.TopicData topicResponse : data.topics()) {
            for (DescribeQuorumResponseData.PartitionData partitionResponse : topicResponse.partitions()) {
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode()));
            }
        }
        return errors;
    }

    @Override
    public DescribeQuorumResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public static DescribeQuorumResponseData singletonResponse(TopicPartition topicPartition,
                                                               int leaderId,
                                                               int leaderEpoch,
                                                               long highWatermark,
                                                               List<ReplicaState> voterStates,
                                                               List<ReplicaState> observerStates) {
        return new DescribeQuorumResponseData()
            .setTopics(Collections.singletonList(new DescribeQuorumResponseData.TopicData()
                .setTopicName(topicPartition.topic())
                .setPartitions(Collections.singletonList(new DescribeQuorumResponseData.PartitionData()
                    .setErrorCode(Errors.NONE.code())
                    .setLeaderId(leaderId)
                    .setLeaderEpoch(leaderEpoch)
                    .setHighWatermark(highWatermark)
                    .setCurrentVoters(voterStates)
                    .setObservers(observerStates)))));
    }

    public static DescribeQuorumResponse parse(ByteBuffer buffer, short version) {
        return new DescribeQuorumResponse(new DescribeQuorumResponseData(new ByteBufferAccessor(buffer), version));
    }

    public String getTopicNameByIndex(Integer index) {
        return data.topics().get(index).topicName();
    }

    public Integer getPartitionLeaderId(String topicName, Integer partition) {
        Integer leaderId = -1;
        TopicData topic = data.topics().stream()
            .filter(t -> t.topicName().equals(topicName))
            .findFirst()
            .orElse(null);
        if (topic != null) {
            leaderId = Integer.valueOf(topic.partitions().get(partition).leaderId());
        }
        return leaderId;
    }

    /**
     * Get the replica info for the given topic name and partition.
     * @param topicName Name of the topic to fetch
     * @param partition Index of the parition to fetch
     * @param getVoterInfo Return the voter information if true, return observers otherwise
     * @return List of {@link ReplicaState}
     */
    private List<ReplicaState> getReplicaInfo(String topicName, Integer partition, boolean getVoterInfo) {
        TopicData topic = data.topics().stream()
                .filter(t -> t.topicName().equals(topicName))
                .findFirst()
                .orElse(null);
        if (topic != null) {
            List<ReplicaState> replicaStates = getVoterInfo ? topic.partitions().get(partition).currentVoters()
                    : topic.partitions().get(partition).observers();
            return replicaStates;
        }
        return null;
    }

    public List<ReplicaState> getVoterInfo(String topicName, Integer partition) {
        return getReplicaInfo(topicName, partition, true);
    }

    public List<ReplicaState> getObserverInfo(String topicName, Integer partition) {
        return getReplicaInfo(topicName, partition, false);
    }

}
