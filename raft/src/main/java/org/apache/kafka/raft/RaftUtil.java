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
package org.apache.kafka.raft;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collections;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

public class RaftUtil {

    public static ApiMessage errorResponse(ApiKeys apiKey, Errors error) {
        switch (apiKey) {
            case VOTE:
                return new VoteResponseData().setErrorCode(error.code());
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponseData().setErrorCode(error.code());
            case FETCH:
                return new FetchResponseData().setErrorCode(error.code());
            default:
                throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        }
    }

    public static FetchRequestData singletonFetchRequest(
        TopicPartition topicPartition,
        Consumer<FetchRequestData.FetchPartition> partitionConsumer
    ) {
        FetchRequestData.FetchPartition fetchPartition =
            new FetchRequestData.FetchPartition()
                .setPartition(topicPartition.partition());
        partitionConsumer.accept(fetchPartition);

        FetchRequestData.FetchTopic fetchTopic =
            new FetchRequestData.FetchTopic()
                .setTopic(topicPartition.topic())
                .setPartitions(singletonList(fetchPartition));

        return new FetchRequestData()
            .setTopics(singletonList(fetchTopic));
    }

    public static FetchResponseData singletonFetchResponse(
        TopicPartition topicPartition,
        Errors topLevelError,
        Consumer<FetchResponseData.PartitionData> partitionConsumer
    ) {
        FetchResponseData.PartitionData fetchablePartition =
            new FetchResponseData.PartitionData();

        fetchablePartition.setPartitionIndex(topicPartition.partition());

        partitionConsumer.accept(fetchablePartition);

        FetchResponseData.FetchableTopicResponse fetchableTopic =
            new FetchResponseData.FetchableTopicResponse()
                .setTopic(topicPartition.topic())
                .setPartitions(Collections.singletonList(fetchablePartition));

        return new FetchResponseData()
            .setErrorCode(topLevelError.code())
            .setResponses(Collections.singletonList(fetchableTopic));
    }

    static boolean hasValidTopicPartition(FetchRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
            data.topics().get(0).topic().equals(topicPartition.topic()) &&
            data.topics().get(0).partitions().size() == 1 &&
            data.topics().get(0).partitions().get(0).partition() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(FetchResponseData data, TopicPartition topicPartition) {
        return data.responses().size() == 1 &&
            data.responses().get(0).topic().equals(topicPartition.topic()) &&
            data.responses().get(0).partitions().size() == 1 &&
            data.responses().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}
