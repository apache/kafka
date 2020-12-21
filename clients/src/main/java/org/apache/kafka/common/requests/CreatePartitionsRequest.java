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

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

public class CreatePartitionsRequest extends AbstractRequest {

    private final CreatePartitionsRequestData data;

    public static class Builder extends AbstractRequest.Builder<CreatePartitionsRequest> {

        private final CreatePartitionsRequestData data;

        public Builder(CreatePartitionsRequestData data) {
            super(ApiKeys.CREATE_PARTITIONS);
            this.data = data;
        }

        @Override
        public CreatePartitionsRequest build(short version) {
            return new CreatePartitionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    CreatePartitionsRequest(CreatePartitionsRequestData data, short apiVersion) {
        super(ApiKeys.CREATE_PARTITIONS, apiVersion);
        this.data = data;
    }

    @Override
    public CreatePartitionsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        CreatePartitionsResponseData response = new CreatePartitionsResponseData();
        response.setThrottleTimeMs(throttleTimeMs);

        ApiError apiError = ApiError.fromThrowable(e);
        for (CreatePartitionsTopic topic : data.topics()) {
            response.results().add(new CreatePartitionsTopicResult()
                    .setName(topic.name())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message())
            );
        }
        return new CreatePartitionsResponse(response);
    }

    public static CreatePartitionsRequest parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsRequest(new CreatePartitionsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
