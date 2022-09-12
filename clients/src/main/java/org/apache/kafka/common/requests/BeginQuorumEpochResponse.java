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
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes.
 *
 * Top level errors:
 * - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * - {@link Errors#BROKER_NOT_AVAILABLE}
 *
 * Partition level errors:
 * - {@link Errors#FENCED_LEADER_EPOCH}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#INCONSISTENT_VOTER_SET}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 */
public class BeginQuorumEpochResponse extends AbstractResponse {
    private final BeginQuorumEpochResponseData data;

    public BeginQuorumEpochResponse(BeginQuorumEpochResponseData data) {
        super(ApiKeys.BEGIN_QUORUM_EPOCH);
        this.data = data;
    }

    public static BeginQuorumEpochResponseData singletonResponse(
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId
    ) {
        return new BeginQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(Collections.singletonList(
                       new BeginQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new BeginQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();

        errors.put(Errors.forCode(data.errorCode()), 1);

        for (BeginQuorumEpochResponseData.TopicData topicResponse : data.topics()) {
            for (BeginQuorumEpochResponseData.PartitionData partitionResponse : topicResponse.partitions()) {
                errors.compute(Errors.forCode(partitionResponse.errorCode()),
                    (error, count) -> count == null ? 1 : count + 1);
            }
        }
        return errors;
    }

    @Override
    public BeginQuorumEpochResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // Not supported by the response schema
    }

    public static BeginQuorumEpochResponse parse(ByteBuffer buffer, short version) {
        return new BeginQuorumEpochResponse(new BeginQuorumEpochResponseData(new ByteBufferAccessor(buffer), version));
    }

}
