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

import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeProducersResponseData.PartitionResponse;
import org.apache.kafka.common.message.DescribeProducersResponseData.TopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DescribeProducersRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeProducersRequest> {
        public final DescribeProducersRequestData data;

        public Builder(DescribeProducersRequestData data) {
            super(ApiKeys.DESCRIBE_PRODUCERS);
            this.data = data;
        }

        public DescribeProducersRequestData.TopicRequest addTopic(String topic) {
            DescribeProducersRequestData.TopicRequest topicRequest =
                new DescribeProducersRequestData.TopicRequest().setName(topic);
            data.topics().add(topicRequest);
            return topicRequest;
        }

        @Override
        public DescribeProducersRequest build(short version) {
            return new DescribeProducersRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeProducersRequestData data;

    private DescribeProducersRequest(DescribeProducersRequestData data, short version) {
        super(ApiKeys.DESCRIBE_PRODUCERS, version);
        this.data = data;
    }

    @Override
    public DescribeProducersRequestData data() {
        return data;
    }

    @Override
    public DescribeProducersResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeProducersResponseData response = new DescribeProducersResponseData();
        for (TopicRequest topicRequest : data.topics()) {
            TopicResponse topicResponse = new TopicResponse()
                .setName(topicRequest.name());
            for (int partitionId : topicRequest.partitionIndexes()) {
                topicResponse.partitions().add(
                    new PartitionResponse()
                        .setPartitionIndex(partitionId)
                        .setErrorCode(error.code())
                );
            }
        }
        return new DescribeProducersResponse(response);
    }

    public static DescribeProducersRequest parse(ByteBuffer buffer, short version) {
        return new DescribeProducersRequest(new DescribeProducersRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
