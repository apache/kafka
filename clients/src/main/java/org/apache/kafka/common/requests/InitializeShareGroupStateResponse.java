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
import org.apache.kafka.common.message.InitializeShareGroupStateRequestData;
import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InitializeShareGroupStateResponse extends AbstractResponse {
    private final InitializeShareGroupStateResponseData data;

    public InitializeShareGroupStateResponse(InitializeShareGroupStateResponseData data) {
        super(ApiKeys.INITIALIZE_SHARE_GROUP_STATE);
        this.data = data;
    }

    @Override
    public InitializeShareGroupStateResponseData data() {
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

    public static InitializeShareGroupStateResponse parse(ByteBuffer buffer, short version) {
        return new InitializeShareGroupStateResponse(
            new InitializeShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public static InitializeShareGroupStateResponseData toGlobalErrorResponse(InitializeShareGroupStateRequestData request, Errors error) {
        List<InitializeShareGroupStateResponseData.InitializeStateResult> initStateResults = new ArrayList<>();
        request.topics().forEach(topicData -> {
            List<InitializeShareGroupStateResponseData.PartitionResult> partitionResults = new ArrayList<>();
            topicData.partitions().forEach(partitionData -> partitionResults.add(
                toErrorResponsePartitionResult(partitionData.partition(), error, error.message()))
            );
            initStateResults.add(toResponseInitializeStateResult(topicData.topicId(), partitionResults));
        });
        return new InitializeShareGroupStateResponseData().setResults(initStateResults);
    }

    public static InitializeShareGroupStateResponseData.PartitionResult toErrorResponsePartitionResult(
        int partitionId,
        Errors error,
        String errorMessage
    ) {
        return new InitializeShareGroupStateResponseData.PartitionResult()
            .setPartition(partitionId)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    public static InitializeShareGroupStateResponseData.InitializeStateResult toResponseInitializeStateResult(
        Uuid topicId,
        List<InitializeShareGroupStateResponseData.PartitionResult> partitionResults
    ) {
        return new InitializeShareGroupStateResponseData.InitializeStateResult()
            .setTopicId(topicId)
            .setPartitions(partitionResults);
    }

    public static InitializeShareGroupStateResponseData toErrorResponseData(Uuid topicId, int partitionId, Errors error, String errorMessage) {
        return new InitializeShareGroupStateResponseData().setResults(List.of(
            new InitializeShareGroupStateResponseData.InitializeStateResult()
                .setTopicId(topicId)
                .setPartitions(List.of(new InitializeShareGroupStateResponseData.PartitionResult()
                    .setPartition(partitionId)
                    .setErrorCode(error.code())
                    .setErrorMessage(errorMessage)))
        ));
    }

    public static InitializeShareGroupStateResponseData.PartitionResult toResponsePartitionResult(int partitionId) {
        return new InitializeShareGroupStateResponseData.PartitionResult().setPartition(partitionId);
    }

    public static InitializeShareGroupStateResponseData toResponseData(Uuid topicId, int partitionId) {
        return new InitializeShareGroupStateResponseData().setResults(List.of(
            new InitializeShareGroupStateResponseData.InitializeStateResult()
                .setTopicId(topicId)
                .setPartitions(List.of(
                    new InitializeShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionId)
                ))
        ));
    }
}
