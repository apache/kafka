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

import org.apache.kafka.common.message.DeleteShareGroupStateRequestData;
import org.apache.kafka.common.message.DeleteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteShareGroupStateRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DeleteShareGroupStateRequest> {

        private final DeleteShareGroupStateRequestData data;

        public Builder(DeleteShareGroupStateRequestData data) {
            this(data, true);
        }

        public Builder(DeleteShareGroupStateRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.DELETE_SHARE_GROUP_STATE, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public DeleteShareGroupStateRequest build(short version) {
            return new DeleteShareGroupStateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteShareGroupStateRequestData data;

    public DeleteShareGroupStateRequest(DeleteShareGroupStateRequestData data, short version) {
        super(ApiKeys.DELETE_SHARE_GROUP_STATE, version);
        this.data = data;
    }

    @Override
    public DeleteShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<DeleteShareGroupStateResponseData.DeleteStateResult> results = new ArrayList<>();
        data.topics().forEach(
                topicResult -> results.add(new DeleteShareGroupStateResponseData.DeleteStateResult()
                        .setTopicId(topicResult.topicId())
                        .setPartitions(topicResult.partitions().stream()
                                .map(partitionData -> new DeleteShareGroupStateResponseData.PartitionResult()
                                        .setPartition(partitionData.partition())
                                        .setErrorCode(Errors.forException(e).code()))
                                .collect(Collectors.toList()))));
        return new DeleteShareGroupStateResponse(new DeleteShareGroupStateResponseData()
                .setResults(results));
    }

    @Override
    public DeleteShareGroupStateRequestData data() {
        return data;
    }

    public static DeleteShareGroupStateRequest parse(ByteBuffer buffer, short version) {
        return new DeleteShareGroupStateRequest(
                new DeleteShareGroupStateRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }
}
