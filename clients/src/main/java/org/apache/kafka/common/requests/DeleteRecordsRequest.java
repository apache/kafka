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

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DeleteRecordsRequest extends AbstractRequest {

    public static final long HIGH_WATERMARK = -1L;

    private final DeleteRecordsRequestData data;

    public static class Builder extends AbstractRequest.Builder<DeleteRecordsRequest> {
        private final DeleteRecordsRequestData data;

        public Builder(DeleteRecordsRequestData data) {
            super(ApiKeys.DELETE_RECORDS);
            this.data = data;
        }

        @Override
        public DeleteRecordsRequest build(short version) {
            return new DeleteRecordsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private DeleteRecordsRequest(DeleteRecordsRequestData data, short version) {
        super(ApiKeys.DELETE_RECORDS, version);
        this.data = data;
    }

    @Override
    public DeleteRecordsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteRecordsResponseData result = new DeleteRecordsResponseData().setThrottleTimeMs(throttleTimeMs);
        short errorCode = Errors.forException(e).code();
        for (DeleteRecordsTopic topic : data.topics()) {
            DeleteRecordsTopicResult topicResult = new DeleteRecordsTopicResult().setName(topic.name());
            result.topics().add(topicResult);
            for (DeleteRecordsRequestData.DeleteRecordsPartition partition : topic.partitions()) {
                topicResult.partitions().add(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(errorCode)
                        .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK));
            }
        }
        return new DeleteRecordsResponse(result);
    }

    public static DeleteRecordsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsRequest(new DeleteRecordsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
