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
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

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

    private final VoteRequestData data;

    private VoteRequest(VoteRequestData data, short version) {
        super(ApiKeys.VOTE, version);
        this.data = data;
    }

    @Override
    public VoteRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new VoteResponse(new VoteResponseData()
            .setErrorCode(Errors.forException(e).code()));
    }

    public static VoteRequest parse(ByteBuffer buffer, short version) {
        return new VoteRequest(new VoteRequestData(new ByteBufferAccessor(buffer), version), version);
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

}
