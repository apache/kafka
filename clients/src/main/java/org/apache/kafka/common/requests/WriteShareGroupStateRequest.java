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

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class WriteShareGroupStateRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<WriteShareGroupStateRequest> {

        private final WriteShareGroupStateRequestData data;

        public Builder(WriteShareGroupStateRequestData data) {
            super(ApiKeys.WRITE_SHARE_GROUP_STATE);
            this.data = data;
        }

        @Override
        public WriteShareGroupStateRequest build(short version) {
            return new WriteShareGroupStateRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final WriteShareGroupStateRequestData data;

    public WriteShareGroupStateRequest(WriteShareGroupStateRequestData data, short version) {
        super(ApiKeys.WRITE_SHARE_GROUP_STATE, version);
        this.data = data;
    }

    @Override
    public WriteShareGroupStateResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<WriteShareGroupStateResponseData.WriteStateResult> results = new ArrayList<>();
        data.topics().forEach(
            topicResult -> results.add(new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                    .map(partitionData -> new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionData.partition())
                        .setErrorCode(Errors.forException(e).code())
                        .setErrorMessage(Errors.forException(e).message()))
                    .collect(Collectors.toList()))));
        return new WriteShareGroupStateResponse(new WriteShareGroupStateResponseData()
            .setResults(results));
    }

    @Override
    public WriteShareGroupStateRequestData data() {
        return data;
    }

    public static WriteShareGroupStateRequest parse(Readable readable, short version) {
        return new WriteShareGroupStateRequest(
            new WriteShareGroupStateRequestData(readable, version),
            version
        );
    }
}
