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

import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ConsumerGroupDescribeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ConsumerGroupDescribeRequest> {

        private final ConsumerGroupDescribeRequestData data;

        public Builder(ConsumerGroupDescribeRequestData data) {
            this(data, false);
        }

        public Builder(ConsumerGroupDescribeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.CONSUMER_GROUP_DESCRIBE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ConsumerGroupDescribeRequest build(short version) {
            return new ConsumerGroupDescribeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ConsumerGroupDescribeRequestData data;

    public ConsumerGroupDescribeRequest(ConsumerGroupDescribeRequestData data, short version) {
        super(ApiKeys.CONSUMER_GROUP_DESCRIBE, version);
        this.data = data;
    }

    @Override
    public ConsumerGroupDescribeResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ConsumerGroupDescribeResponseData data = new ConsumerGroupDescribeResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        // Set error for each group
        this.data.groupIds().forEach(
            groupId -> data.groups().add(
                new ConsumerGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.forException(e).code())
            )
        );
        return new ConsumerGroupDescribeResponse(data);
    }

    @Override
    public ConsumerGroupDescribeRequestData data() {
        return data;
    }

    public static ConsumerGroupDescribeRequest parse(ByteBuffer buffer, short version) {
        return new ConsumerGroupDescribeRequest(
            new ConsumerGroupDescribeRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }
}
