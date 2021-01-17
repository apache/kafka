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
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
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
public class EndQuorumEpochResponse extends AbstractResponse {
    private final EndQuorumEpochResponseData data;

    public EndQuorumEpochResponse(EndQuorumEpochResponseData data) {
        super(ApiKeys.END_QUORUM_EPOCH);
        this.data = data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();

        errors.put(Errors.forCode(data.errorCode()), 1);

        for (EndQuorumEpochResponseData.TopicData topicResponse : data.topics()) {
            for (EndQuorumEpochResponseData.PartitionData partitionResponse : topicResponse.partitions()) {
                updateErrorCounts(errors, Errors.forCode(partitionResponse.errorCode()));
            }
        }
        return errors;
    }

    @Override
    public EndQuorumEpochResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public static EndQuorumEpochResponseData singletonResponse(
        Errors topLevelError,
        TopicPartition topicPartition,
        Errors partitionLevelError,
        int leaderEpoch,
        int leaderId
    ) {
        return new EndQuorumEpochResponseData()
                   .setErrorCode(topLevelError.code())
                   .setTopics(Collections.singletonList(
                       new EndQuorumEpochResponseData.TopicData()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new EndQuorumEpochResponseData.PartitionData()
                                   .setErrorCode(partitionLevelError.code())
                                   .setLeaderId(leaderId)
                                   .setLeaderEpoch(leaderEpoch)
                           )))
                   );
    }

    public static EndQuorumEpochResponse parse(ByteBuffer buffer, short version) {
        return new EndQuorumEpochResponse(new EndQuorumEpochResponseData(new ByteBufferAccessor(buffer), version));
    }
}
