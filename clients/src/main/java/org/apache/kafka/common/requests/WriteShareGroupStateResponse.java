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
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteShareGroupStateResponse extends AbstractResponse {
    private final WriteShareGroupStateResponseData data;

    public WriteShareGroupStateResponse(WriteShareGroupStateResponseData data) {
        super(ApiKeys.WRITE_SHARE_GROUP_STATE);
        this.data = data;
    }

    @Override
    public WriteShareGroupStateResponseData data() {
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

    public static WriteShareGroupStateResponse parse(ByteBuffer buffer, short version) {
        return new WriteShareGroupStateResponse(
                new WriteShareGroupStateResponseData(new ByteBufferAccessor(buffer), version)
        );
    }

    public static WriteShareGroupStateResponseData toResponseData(Uuid topicId, int partitionId) {
        return new WriteShareGroupStateResponseData()
                .setResults(Collections.singletonList(
                        new WriteShareGroupStateResponseData.WriteStateResult()
                                .setTopicId(topicId)
                                .setPartitions(Collections.singletonList(
                                        new WriteShareGroupStateResponseData.PartitionResult()
                                                .setPartition(partitionId)))));
    }

    public static WriteShareGroupStateResponseData toErrorResponseData(Uuid topicId, int partitionId, Errors error, String errorMessage) {
        WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData();
        responseData.setResults(Collections.singletonList(new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId)
                .setPartitions(Collections.singletonList(new WriteShareGroupStateResponseData.PartitionResult()
                        .setPartition(partitionId)
                        .setErrorCode(error.code())
                        .setErrorMessage(errorMessage)))));
        return responseData;
    }

    public static WriteShareGroupStateResponseData.PartitionResult toErrorResponsePartitionResult(int partitionId, Errors error, String errorMessage) {
        return new WriteShareGroupStateResponseData.PartitionResult()
                .setPartition(partitionId)
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage);
    }

    public static WriteShareGroupStateResponseData.WriteStateResult toResponseWriteStateResult(Uuid topicId, List<WriteShareGroupStateResponseData.PartitionResult> partitionResults) {
        return new WriteShareGroupStateResponseData.WriteStateResult()
                .setTopicId(topicId)
                .setPartitions(partitionResults);
    }

    public static WriteShareGroupStateResponseData.PartitionResult toResponsePartitionResult(int partitionId) {
        return new WriteShareGroupStateResponseData.PartitionResult()
                .setPartition(partitionId);
    }
}
