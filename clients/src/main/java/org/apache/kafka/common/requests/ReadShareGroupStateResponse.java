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
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadShareGroupStateResponse extends AbstractResponse {
    private final ReadShareGroupStateResponseData data;

    public ReadShareGroupStateResponse(ReadShareGroupStateResponseData data) {
        super(ApiKeys.READ_SHARE_GROUP_STATE);
        this.data = data;
    }

    @Override
    public ReadShareGroupStateResponseData data() {
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

    public static ReadShareGroupStateResponse parse(ByteBuffer buffer, short version) {
        return new ReadShareGroupStateResponse(
                new ReadShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public static ReadShareGroupStateResponseData toResponseData(
            Uuid topicId,
            int partition,
            long startOffset,
            int stateEpoch,
            List<ReadShareGroupStateResponseData.StateBatch> stateBatches
    ) {
        return new ReadShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                        new ReadShareGroupStateResponseData.ReadStateResult()
                                .setTopicId(topicId)
                                .setPartitions(Collections.singletonList(
                                        new ReadShareGroupStateResponseData.PartitionResult()
                                                .setPartition(partition)
                                                .setStartOffset(startOffset)
                                                .setStateEpoch(stateEpoch)
                                                .setStateBatches(stateBatches)
                                ))
                ));
    }

    public static ReadShareGroupStateResponseData toErrorResponseData(Uuid topicId, int partitionId, Errors error, String errorMessage) {
        return new ReadShareGroupStateResponseData().setResults(
                Collections.singletonList(new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(partitionId)
                                .setErrorCode(error.code())
                                .setErrorMessage(errorMessage)))));
    }

    public static ReadShareGroupStateResponseData.PartitionResult toErrorResponsePartitionResult(int partitionId, Errors error, String errorMessage) {
        return new ReadShareGroupStateResponseData.PartitionResult()
                .setPartition(partitionId)
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage);
    }

    public static ReadShareGroupStateResponseData.ReadStateResult toResponseReadStateResult(Uuid topicId, List<ReadShareGroupStateResponseData.PartitionResult> partitionResults) {
        return new ReadShareGroupStateResponseData.ReadStateResult()
                .setTopicId(topicId)
                .setPartitions(partitionResults);
    }

    public static ReadShareGroupStateResponseData toGlobalErrorResponse(ReadShareGroupStateRequestData request, Errors error) {
        List<ReadShareGroupStateResponseData.ReadStateResult> readStateResults = new ArrayList<>();
        request.topics().forEach(topicData -> {
            List<ReadShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>();
            topicData.partitions().forEach(partitionData -> partitionResults.add(
                toErrorResponsePartitionResult(partitionData.partition(), error, error.message()))
            );
            readStateResults.add(toResponseReadStateResult(topicData.topicId(), partitionResults));
        });
        return new ReadShareGroupStateResponseData().setResults(readStateResults);
    }
}
