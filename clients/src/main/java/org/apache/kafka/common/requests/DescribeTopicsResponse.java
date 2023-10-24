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

import org.apache.kafka.common.message.DescribeTopicsResponseData;
import org.apache.kafka.common.message.DescribeTopicsResponseData.DescribeTopicsResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeTopicsResponse extends AbstractResponse {
    private final DescribeTopicsResponseData data;

    public DescribeTopicsResponse(DescribeTopicsResponseData data) {
        super(ApiKeys.DESCRIBE_TOPICS);
        this.data = data;
    }

    @Override
    public DescribeTopicsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(topicResponse -> {
            topicResponse.partitions().forEach(p -> updateErrorCounts(errorCounts, Errors.forCode(p.errorCode())));
            updateErrorCounts(errorCounts, Errors.forCode(topicResponse.errorCode()));
        });
        return errorCounts;
    }

    public static DescribeTopicsResponse prepareResponse(int throttleTimeMs,
                                                         List<DescribeTopicsResponseTopic> topics) {
        DescribeTopicsResponseData responseData = new DescribeTopicsResponseData();
        responseData.setThrottleTimeMs(throttleTimeMs);
        topics.forEach(topicResponse -> responseData.topics().add(topicResponse));
        return new DescribeTopicsResponse(responseData);
    }

    public static DescribeTopicsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeTopicsResponse(
            new DescribeTopicsResponseData(new ByteBufferAccessor(buffer), version));
    }
}
