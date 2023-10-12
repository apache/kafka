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

import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.DescribeTopicsRequestData;
import org.apache.kafka.common.message.DescribeTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class DescribeTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeTopicsRequest> {
        private final DescribeTopicsRequestData data;

        public Builder(DescribeTopicsRequestData data) {
            super(ApiKeys.DESCRIBE_TOPICS);
            this.data = data;
        }

        public Builder(List<String> topics) {
            super(ApiKeys.DESCRIBE_TOPICS, ApiKeys.DESCRIBE_TOPICS.oldestVersion(), ApiKeys.DESCRIBE_TOPICS.latestVersion());
            DescribeTopicsRequestData data = new DescribeTopicsRequestData();
            topics.forEach(topicName -> data.topics().add(new DescribeTopicsRequestData.TopicRequest().setName(topicName)));
            this.data = data;
        }

        @Override
        public DescribeTopicsRequest build(short version) {
            return new DescribeTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

    }

    private final DescribeTopicsRequestData data;

    public DescribeTopicsRequest(DescribeTopicsRequestData data) {
        super(ApiKeys.DESCRIBE_TOPICS, (short) 0);
        this.data = data;
    }

    public DescribeTopicsRequest(DescribeTopicsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_TOPICS, version);
        this.data = data;
    }

    @Override
    public DescribeTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        DescribeTopicsResponseData responseData = new DescribeTopicsResponseData();
        for (DescribeTopicsRequestData.TopicRequest topic : data.topics()) {
            responseData.topics().add(new DescribeTopicsResponseData.DescribeTopicsResponseTopic()
                .setName(topic.name())
                .setErrorCode(error.code())
                .setIsInternal(false)
                .setPartitions(Collections.emptyList())
            );
        }
        responseData.setThrottleTimeMs(throttleTimeMs);
        return new DescribeTopicsResponse(responseData);
    }

    public static DescribeTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeTopicsRequest(
            new DescribeTopicsRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }
}
