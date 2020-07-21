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
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class VoteRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<VoteRequest> {
        private final VoteRequestData data;

        public Builder(VoteRequestData data) {
            super(ApiKeys.VOTE);
            this.data = data;
        }

        @Override
        public VoteRequest build(short version) {
            return new VoteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final VoteRequestData data;

    private VoteRequest(VoteRequestData data, short version) {
        super(ApiKeys.VOTE, version);
        this.data = data;
    }

    public VoteRequest(Struct struct, short version) {
        super(ApiKeys.VOTE, version);
        this.data = new VoteRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new VoteResponse(getTopLevelErrorResponse(Errors.forException(e)));
    }

    public static VoteRequestData singletonRequest(TopicPartition topicPartition,
                                                   int candidateEpoch,
                                                   int candidateId,
                                                   int lastEpoch,
                                                   long lastEpochEndOffset) {
        return singletonRequest(topicPartition,
            null,
            candidateEpoch,
            candidateId,
            lastEpoch,
            lastEpochEndOffset);
    }

    public static VoteRequestData singletonRequest(TopicPartition topicPartition,
                                                   String clusterId,
                                                   int candidateEpoch,
                                                   int candidateId,
                                                   int lastEpoch,
                                                   long lastEpochEndOffset) {
        return new VoteRequestData()
                   .setClusterId(clusterId)
                   .setTopics(Collections.singletonList(
                       new VoteRequestData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new VoteRequestData.PartitionData()
                                   .setPartitionIndex(topicPartition.partition())
                                   .setCandidateEpoch(candidateEpoch)
                                   .setCandidateId(candidateId)
                                   .setLastOffsetEpoch(lastEpoch)
                                   .setLastOffset(lastEpochEndOffset))
                           )));
    }

    public static VoteResponseData getPartitionLevelErrorResponse(VoteRequestData data, Errors error) {
        short errorCode = error.code();
        List<VoteResponseData.TopicData> topicResponses = new ArrayList<>();
        for (VoteRequestData.TopicData topic : data.topics()) {
            topicResponses.add(
                new VoteResponseData.TopicData()
                    .setTopicName(topic.topicName())
                    .setPartitions(topic.partitions().stream().map(
                        requestPartition -> new VoteResponseData.PartitionData()
                                                .setPartitionIndex(requestPartition.partitionIndex())
                                                .setErrorCode(errorCode)
                    ).collect(Collectors.toList())));
        }

        return new VoteResponseData().setTopics(topicResponses);
    }

    public static VoteResponseData getTopLevelErrorResponse(Errors error) {
        return new VoteResponseData().setErrorCode(error.code());
    }
}
