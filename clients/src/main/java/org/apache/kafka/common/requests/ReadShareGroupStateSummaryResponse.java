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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateSummaryResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadShareGroupStateSummaryResponse extends AbstractResponse {
    private final ReadShareGroupStateSummaryResponseData data;

    public ReadShareGroupStateSummaryResponse(ReadShareGroupStateSummaryResponseData data) {
        super(ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY);
        this.data = data;
    }

    @Override
    public ReadShareGroupStateSummaryResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        data.results().forEach(
            result -> result.partitions().forEach(
                partitionResult -> updateErrorCounts(counts, Errors.forCode(partitionResult.errorCode()))
            )
        );
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // No op
    }

    public static ReadShareGroupStateSummaryResponse parse(ByteBuffer buffer, short version) {
        return new ReadShareGroupStateSummaryResponse(
            new ReadShareGroupStateSummaryResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public static ReadShareGroupStateSummaryResponseData toErrorResponseData(
        Uuid topicId,
        int partitionId,
        Errors error,
        String errorMessage
    ) {
        return new ReadShareGroupStateSummaryResponseData().setResults(
            List.of(new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                .setTopicId(topicId)
                .setPartitions(List.of(new ReadShareGroupStateSummaryResponseData.PartitionResult()
                    .setPartition(partitionId)
                    .setErrorCode(error.code())
                    .setErrorMessage(errorMessage)))));
    }

    public static ReadShareGroupStateSummaryResponseData.PartitionResult toErrorResponsePartitionResult(
        int partitionId,
        Errors error,
        String errorMessage
    ) {
        return new ReadShareGroupStateSummaryResponseData.PartitionResult()
            .setPartition(partitionId)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    public static ReadShareGroupStateSummaryResponseData toResponseData(
        Uuid topicId,
        int partition,
        long startOffset,
        int stateEpoch
    ) {
        return new ReadShareGroupStateSummaryResponseData()
            .setResults(List.of(
                new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
                    .setTopicId(topicId)
                    .setPartitions(List.of(
                        new ReadShareGroupStateSummaryResponseData.PartitionResult()
                            .setPartition(partition)
                            .setStartOffset(startOffset)
                            .setStateEpoch(stateEpoch)
                    ))
            ));
    }

    public static ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult toResponseReadStateSummaryResult(
        Uuid topicId,
        List<ReadShareGroupStateSummaryResponseData.PartitionResult> partitionResults
    ) {
        return new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
            .setTopicId(topicId)
            .setPartitions(partitionResults);
    }

    public static ReadShareGroupStateSummaryResponseData toGlobalErrorResponse(ReadShareGroupStateSummaryRequestData request, Errors error) {
        List<ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult> readStateSummaryResults = new ArrayList<>();
        request.topics().forEach(topicData -> {
            List<ReadShareGroupStateSummaryResponseData.PartitionResult> partitionResults = new ArrayList<>();
            topicData.partitions().forEach(partitionData -> partitionResults.add(
                toErrorResponsePartitionResult(partitionData.partition(), error, error.message()))
            );
            readStateSummaryResults.add(toResponseReadStateSummaryResult(topicData.topicId(), partitionResults));
        });
        return new ReadShareGroupStateSummaryResponseData().setResults(readStateSummaryResults);
    }
}
