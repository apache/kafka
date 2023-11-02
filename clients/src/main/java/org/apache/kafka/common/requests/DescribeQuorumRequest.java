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
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeQuorumRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeQuorumRequest> {
        private final DescribeQuorumRequestData data;

        public Builder(DescribeQuorumRequestData data) {
            super(ApiKeys.DESCRIBE_QUORUM);
            this.data = data;
        }

        @Override
        public DescribeQuorumRequest build(short version) {
            return new DescribeQuorumRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeQuorumRequestData data;

    private DescribeQuorumRequest(DescribeQuorumRequestData data, short version) {
        super(ApiKeys.DESCRIBE_QUORUM, version);
        this.data = data;
    }

    public static DescribeQuorumRequest parse(ByteBuffer buffer, short version) {
        return new DescribeQuorumRequest(new DescribeQuorumRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static DescribeQuorumRequestData singletonRequest(TopicPartition topicPartition) {
        return new DescribeQuorumRequestData()
            .setTopics(Collections.singletonList(
                new DescribeQuorumRequestData.TopicData()
                    .setTopicName(topicPartition.topic())
                    .setPartitions(Collections.singletonList(
                        new DescribeQuorumRequestData.PartitionData()
                            .setPartitionIndex(topicPartition.partition()))
            )));
    }

    @Override
    public DescribeQuorumRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeQuorumResponse(getTopLevelErrorResponse(Errors.forException(e)));
    }

    public static DescribeQuorumResponseData getPartitionLevelErrorResponse(DescribeQuorumRequestData data, Errors error) {
        short errorCode = error.code();

        List<DescribeQuorumResponseData.TopicData> topicResponses = new ArrayList<>();
        for (DescribeQuorumRequestData.TopicData topic : data.topics()) {
            topicResponses.add(
                new DescribeQuorumResponseData.TopicData()
                    .setTopicName(topic.topicName())
                    .setPartitions(topic.partitions().stream().map(
                        requestPartition -> new DescribeQuorumResponseData.PartitionData()
                                                .setPartitionIndex(requestPartition.partitionIndex())
                                                .setErrorCode(errorCode)
                    ).collect(Collectors.toList())));
        }

        return new DescribeQuorumResponseData().setTopics(topicResponses);
    }

    public static DescribeQuorumResponseData getTopLevelErrorResponse(Errors topLevelError) {
        return new DescribeQuorumResponseData().setErrorCode(topLevelError.code());
    }
}
