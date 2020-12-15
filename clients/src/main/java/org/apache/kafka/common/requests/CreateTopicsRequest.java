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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

public class CreateTopicsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<CreateTopicsRequest> {
        private final CreateTopicsRequestData data;

        public Builder(CreateTopicsRequestData data) {
            super(ApiKeys.CREATE_TOPICS);
            this.data = data;
        }

        @Override
        public CreateTopicsRequest build(short version) {
            if (data.validateOnly() && version == 0)
                throw new UnsupportedVersionException("validateOnly is not supported in version 0 of " +
                        "CreateTopicsRequest");

            final List<String> topicsWithDefaults = data.topics()
                .stream()
                .filter(topic -> topic.assignments().isEmpty())
                .filter(topic ->
                    topic.numPartitions() == CreateTopicsRequest.NO_NUM_PARTITIONS
                        || topic.replicationFactor() == CreateTopicsRequest.NO_REPLICATION_FACTOR)
                .map(CreatableTopic::name)
                .collect(Collectors.toList());

            if (!topicsWithDefaults.isEmpty() && version < 4) {
                throw new UnsupportedVersionException("Creating topics with default "
                    + "partitions/replication factor are only supported in CreateTopicRequest "
                    + "version 4+. The following topics need values for partitions and replicas: "
                    + topicsWithDefaults);
            }

            return new CreateTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final CreateTopicsRequestData data;

    public static final int NO_NUM_PARTITIONS = -1;
    public static final short NO_REPLICATION_FACTOR = -1;

    public CreateTopicsRequest(CreateTopicsRequestData data, short version) {
        super(ApiKeys.CREATE_TOPICS, version);
        this.data = data;
    }

    @Override
    public CreateTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        CreateTopicsResponseData response = new CreateTopicsResponseData();
        if (version() >= 2) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        ApiError apiError = ApiError.fromThrowable(e);
        for (CreatableTopic topic : data.topics()) {
            response.topics().add(new CreatableTopicResult().
                setName(topic.name()).
                setErrorCode(apiError.error().code()).
                setErrorMessage(apiError.message()));
        }
        return new CreateTopicsResponse(response);
    }

    public static CreateTopicsRequest parse(ByteBuffer buffer, short version) {
        return new CreateTopicsRequest(new CreateTopicsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
