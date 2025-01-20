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

import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ReadShareGroupStateSummaryRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ReadShareGroupStateSummaryRequest> {

        private final ReadShareGroupStateSummaryRequestData data;

        public Builder(ReadShareGroupStateSummaryRequestData data) {
            this(data, false);
        }

        public Builder(ReadShareGroupStateSummaryRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ReadShareGroupStateSummaryRequest build(short version) {
            return new ReadShareGroupStateSummaryRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ReadShareGroupStateSummaryRequestData data;

    public ReadShareGroupStateSummaryRequest(ReadShareGroupStateSummaryRequestData data, short version) {
        super(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY, version);
        this.data = data;
    }

    @Override
    public ReadShareGroupStateSummaryResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> results = new ArrayList<>();
        data.topics().forEach(
            topicResult -> results.add(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                .setTopicId(topicResult.topicId())
                .setPartitions(topicResult.partitions().stream()
                    .map(partitionData -> new ReadShareGroupStateSummaryResponseData.PartitionResult()
                        .setPartition(partitionData.partition())
                        .setErrorCode(Errors.forException(e).code())
                        .setErrorMessage(Errors.forException(e).message()))
                    .collect(Collectors.toList()))));
        return new ReadShareGroupStateSummaryResponse(new ReadShareGroupStateSummaryResponseData()
            .setResults(results));
    }

    @Override
    public ReadShareGroupStateSummaryRequestData data() {
        return data;
    }

    public static ReadShareGroupStateSummaryRequest parse(ByteBuffer buffer, short version) {
        return new ReadShareGroupStateSummaryRequest(
            new ReadShareGroupStateSummaryRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }
}
