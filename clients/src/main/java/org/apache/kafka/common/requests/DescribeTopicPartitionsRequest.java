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

import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class DescribeTopicPartitionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeTopicPartitionsRequest> {
        private final DescribeTopicPartitionsRequestData data;

        public Builder(DescribeTopicPartitionsRequestData data) {
            super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS);
            this.data = data;
        }

        public Builder(List<String> topics) {
            super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, ApiKeys.DESCRIBE_TOPIC_PARTITIONS.oldestVersion(),
                ApiKeys.DESCRIBE_TOPIC_PARTITIONS.latestVersion());
            DescribeTopicPartitionsRequestData data = new DescribeTopicPartitionsRequestData();
            topics.forEach(topicName -> data.topics().add(
                new DescribeTopicPartitionsRequestData.TopicRequest().setName(topicName))
            );
            this.data = data;
        }

        @Override
        public DescribeTopicPartitionsRequest build(short version) {
            return new DescribeTopicPartitionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

    }

    private final DescribeTopicPartitionsRequestData data;

    public DescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestData data) {
        super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, (short) 0);
        this.data = data;
    }

    public DescribeTopicPartitionsRequest(DescribeTopicPartitionsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_TOPIC_PARTITIONS, version);
        this.data = data;
    }

    @Override
    public DescribeTopicPartitionsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeTopicPartitionsResponseData responseData = new DescribeTopicPartitionsResponseData();
        for (DescribeTopicPartitionsRequestData.TopicRequest topic : data.topics()) {
            responseData.topics().add(new DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic()
                .setName(topic.name())
                .setErrorCode(error.code())
                .setIsInternal(false)
                .setPartitions(Collections.emptyList())
            );
        }
        responseData.setThrottleTimeMs(throttleTimeMs);
        return new DescribeTopicPartitionsResponse(responseData);
    }

    public static DescribeTopicPartitionsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeTopicPartitionsRequest(
            new DescribeTopicPartitionsRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }
}
