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

import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ReadShareGroupStateRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ReadShareGroupStateRequest> {

        private final ReadShareGroupStateRequestData data;

        public Builder(ReadShareGroupStateRequestData data) {
            super(ApiKeys.READ_SHARE_GROUP_STATE);
            this.data = data;
        }

        @Override
        public ReadShareGroupStateRequest build(short version) {
            return new ReadShareGroupStateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ReadShareGroupStateRequestData data;

    public ReadShareGroupStateRequest(ReadShareGroupStateRequestData data, short version) {
        super(ApiKeys.READ_SHARE_GROUP_STATE, version);
        this.data = data;
    }

    @Override
    public ReadShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<ReadShareGroupStateResponseData.ReadStateResult> results = new ArrayList<>();
        data.topics().forEach(
            topicResult -> results.add(new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                    .map(partitionData -> new ReadShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionData.partition())
                        .setErrorCode(Errors.forException(e).code())
                        .setErrorMessage(Errors.forException(e).message()))
                    .collect(Collectors.toList()))));
        return new ReadShareGroupStateResponse(new ReadShareGroupStateResponseData()
            .setResults(results));
    }

    @Override
    public ReadShareGroupStateRequestData data() {
        return data;
    }

    public static ReadShareGroupStateRequest parse(Readable readable, short version) {
        return new ReadShareGroupStateRequest(
            new ReadShareGroupStateRequestData(readable, version),
            version
        );
    }
}
