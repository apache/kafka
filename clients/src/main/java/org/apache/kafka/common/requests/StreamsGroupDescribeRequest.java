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

import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class StreamsGroupDescribeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<StreamsGroupDescribeRequest> {

        private final StreamsGroupDescribeRequestData data;

        public Builder(StreamsGroupDescribeRequestData data) {
            this(data, false);
        }

        public Builder(StreamsGroupDescribeRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STREAMS_GROUP_DESCRIBE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StreamsGroupDescribeRequest build(short version) {
            return new StreamsGroupDescribeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsGroupDescribeRequestData data;

    public StreamsGroupDescribeRequest(StreamsGroupDescribeRequestData data, short version) {
        super(ApiKeys.STREAMS_GROUP_DESCRIBE, version);
        this.data = data;
    }

    @Override
    public StreamsGroupDescribeResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        StreamsGroupDescribeResponseData data = new StreamsGroupDescribeResponseData()
            .setThrottleTimeMs(throttleTimeMs);
        // Set error for each group
        this.data.groupIds().forEach(
            groupId -> data.groups().add(
                new StreamsGroupDescribeResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setErrorCode(Errors.forException(e).code())
            )
        );
        return new StreamsGroupDescribeResponse(data);
    }

    @Override
    public StreamsGroupDescribeRequestData data() {
        return data;
    }

    public static StreamsGroupDescribeRequest parse(ByteBuffer buffer, short version) {
        return new StreamsGroupDescribeRequest(
            new StreamsGroupDescribeRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }

    public static List<StreamsGroupDescribeResponseData.DescribedGroup> getErrorDescribedGroupList(
        List<String> groupIds,
        Errors error
    ) {
        return groupIds.stream()
            .map(groupId -> new StreamsGroupDescribeResponseData.DescribedGroup()
                .setGroupId(groupId)
                .setErrorCode(error.code())
            ).collect(Collectors.toList());
    }
}
