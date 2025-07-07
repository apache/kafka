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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class EndQuorumEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<EndQuorumEpochRequest> {
        private final EndQuorumEpochRequestData data;

        public Builder(EndQuorumEpochRequestData data) {
            super(ApiKeys.END_QUORUM_EPOCH);
            this.data = data;
        }

        @Override
        public EndQuorumEpochRequest build(short version) {
            return new EndQuorumEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final EndQuorumEpochRequestData data;

    private EndQuorumEpochRequest(EndQuorumEpochRequestData data, short version) {
        super(ApiKeys.END_QUORUM_EPOCH, version);
        this.data = data;
    }

    @Override
    public EndQuorumEpochRequestData data() {
        return data;
    }

    @Override
    public EndQuorumEpochResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EndQuorumEpochResponse(new EndQuorumEpochResponseData()
            .setErrorCode(Errors.forException(e).code()));
    }

    public static EndQuorumEpochRequest parse(Readable readable, short version) {
        return new EndQuorumEpochRequest(new EndQuorumEpochRequestData(readable, version), version);
    }

    public static EndQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                             int leaderEpoch,
                                                             int leaderId,
                                                             List<Integer> preferredSuccessors) {
        return singletonRequest(topicPartition, null, leaderEpoch, leaderId, preferredSuccessors);
    }

    public static EndQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                             String clusterId,
                                                             int leaderEpoch,
                                                             int leaderId,
                                                             List<Integer> preferredSuccessors) {
        return new EndQuorumEpochRequestData()
                   .setClusterId(clusterId)
                   .setTopics(Collections.singletonList(
                       new EndQuorumEpochRequestData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new EndQuorumEpochRequestData.PartitionData()
                                   .setPartitionIndex(topicPartition.partition())
                                   .setLeaderEpoch(leaderEpoch)
                                   .setLeaderId(leaderId)
                                   .setPreferredSuccessors(preferredSuccessors))))
                   );
    }

    public static List<EndQuorumEpochRequestData.ReplicaInfo> preferredCandidates(EndQuorumEpochRequestData.PartitionData partition) {
        if (partition.preferredCandidates().isEmpty()) {
            return partition
                .preferredSuccessors()
                .stream()
                .map(id -> new EndQuorumEpochRequestData.ReplicaInfo()
                    .setCandidateId(id)
                    .setCandidateDirectoryId(Uuid.ZERO_UUID)
                )
                .collect(Collectors.toList());
        } else {
            return partition.preferredCandidates();
        }
    }
}
