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
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

public class BeginQuorumEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<BeginQuorumEpochRequest> {
        private final BeginQuorumEpochRequestData data;

        public Builder(BeginQuorumEpochRequestData data) {
            super(ApiKeys.BEGIN_QUORUM_EPOCH);
            this.data = data;
        }

        @Override
        public BeginQuorumEpochRequest build(short version) {
            return new BeginQuorumEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BeginQuorumEpochRequestData data;

    private BeginQuorumEpochRequest(BeginQuorumEpochRequestData data, short version) {
        super(ApiKeys.BEGIN_QUORUM_EPOCH, version);
        this.data = data;
    }

    @Override
    public BeginQuorumEpochRequestData data() {
        return data;
    }

    @Override
    public BeginQuorumEpochResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new BeginQuorumEpochResponse(new BeginQuorumEpochResponseData()
            .setErrorCode(Errors.forException(e).code()));
    }

    public static BeginQuorumEpochRequest parse(ByteBuffer buffer, short version) {
        return new BeginQuorumEpochRequest(new BeginQuorumEpochRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static BeginQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                               int leaderEpoch,
                                                               int leaderId) {
        return singletonRequest(topicPartition, null, leaderEpoch, leaderId);
    }

    public static BeginQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                               String clusterId,
                                                               int leaderEpoch,
                                                               int leaderId) {
        return new BeginQuorumEpochRequestData()
                   .setClusterId(clusterId)
                   .setTopics(Collections.singletonList(
                       new BeginQuorumEpochRequestData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new BeginQuorumEpochRequestData.PartitionData()
                                   .setPartitionIndex(topicPartition.partition())
                                   .setLeaderEpoch(leaderEpoch)
                                   .setLeaderId(leaderId))))
                   );
    }

}
