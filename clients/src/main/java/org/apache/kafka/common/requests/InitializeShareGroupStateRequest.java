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

import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InitializeShareGroupStateRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<InitializeShareGroupStateRequest> {

        private final InitializeShareGroupStateRequestData data;

        public Builder(InitializeShareGroupStateRequestData data) {
            this(data, true);
        }

        public Builder(InitializeShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public InitializeShareGroupStateRequest build(short version) {
            return new InitializeShareGroupStateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final InitializeShareGroupStateRequestData data;

    public InitializeShareGroupStateRequest(InitializeShareGroupStateRequestData data, short version) {
        super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE, version);
        this.data = data;
    }

    @Override
    public InitializeShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<InitializeShareGroupStateResponseData.InitializeStateResult> results = new ArrayList<>();
        data.topics().forEach(
            topicResult -> results.add(new InitializeShareGroupStateResponseData.InitializeStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                    .map(partitionData -> new InitializeShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionData.partition())
                        .setErrorCode(Errors.forException(e).code()))
                    .collect(Collectors.toList()))));
        return new InitializeShareGroupStateResponse(new InitializeShareGroupStateResponseData()
            .setResults(results));
    }

    @Override
    public InitializeShareGroupStateRequestData data() {
        return data;
    }

    public static InitializeShareGroupStateRequest parse(ByteBuffer buffer, short version) {
        return new InitializeShareGroupStateRequest(
            new InitializeShareGroupStateRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }
}
