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

import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeShareGroupOffsetsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeShareGroupOffsetsRequest> {

        private final DescribeShareGroupOffsetsRequestData data;

        public Builder(DescribeShareGroupOffsetsRequestData data) {
            this(data, false);
        }

        public Builder(DescribeShareGroupOffsetsRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public DescribeShareGroupOffsetsRequest build(short version) {
            return new DescribeShareGroupOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeShareGroupOffsetsRequestData data;

    public DescribeShareGroupOffsetsRequest(DescribeShareGroupOffsetsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, version);
        this.data = data;
    }

    @Override
    public DescribeShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> results = new ArrayList<>();
        data.topics().forEach(
                topicResult -> results.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                        .setTopicName(topicResult.topicName())
                        .setPartitions(topicResult.partitions().stream()
                                .map(partitionData -> new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                                        .setPartitionIndex(partitionData)
                                        .setErrorCode(Errors.forException(e).code()))
                                .collect(Collectors.toList()))));
        return new DescribeShareGroupOffsetsResponse(new DescribeShareGroupOffsetsResponseData()
                .setResponses(results));
    }

    @Override
    public DescribeShareGroupOffsetsRequestData data() {
        return data;
    }

    public static DescribeShareGroupOffsetsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeShareGroupOffsetsRequest(
                new DescribeShareGroupOffsetsRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    public static List<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic> getErrorDescribeShareGroupOffsets(
        List<DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestTopic> topics,
        Errors error
    ) {
        return topics.stream()
            .map(
                requestTopic -> new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
                    .setTopicName(requestTopic.topicName())
                    .setPartitions(
                        requestTopic.partitions().stream().map(
                            partition -> new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
                                .setPartitionIndex(partition)
                                .setErrorCode(error.code())
                                .setErrorMessage(error.message())
                                .setStartOffset(0)
                        ).collect(Collectors.toList())
                    )
            ).collect(Collectors.toList());
    }
}
