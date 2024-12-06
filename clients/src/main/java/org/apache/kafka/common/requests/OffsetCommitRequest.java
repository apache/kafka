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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedProtocolFieldException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class OffsetCommitRequest extends AbstractRequest {
    // default values for the current version
    public static final int DEFAULT_GENERATION_ID = -1;
    public static final String DEFAULT_MEMBER_ID = "";
    public static final long DEFAULT_RETENTION_TIME = -1L;

    // default values for old versions, will be removed after these versions are no longer supported
    public static final long DEFAULT_TIMESTAMP = -1L;            // for V0, V1

    private final OffsetCommitRequestData data;

    public static class Builder extends AbstractRequest.Builder<OffsetCommitRequest> {

        private final OffsetCommitRequestData data;

        public Builder(OffsetCommitRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.OFFSET_COMMIT, enableUnstableLastVersion);
            this.data = data;
        }

        public Builder(OffsetCommitRequestData data) {
            this(data, false);
        }

        @Override
        public OffsetCommitRequest build(short version) {
            if (data.groupInstanceId() != null && version < 7) {
                throw new UnsupportedProtocolFieldException("GroupInstanceId", apiKey().name(), version, 7);
            }
            return new OffsetCommitRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public OffsetCommitRequest(OffsetCommitRequestData data, short version) {
        super(ApiKeys.OFFSET_COMMIT, version);
        this.data = data;
    }

    @Override
    public OffsetCommitRequestData data() {
        return data;
    }

    public Map<TopicPartition, Long> offsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (OffsetCommitRequestTopic topic : data.topics()) {
            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                offsets.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                        partition.committedOffset());
            }
        }
        return offsets;
    }

    public static OffsetCommitResponseData getErrorResponse(
        OffsetCommitRequestData request,
        Errors error
    ) {
        OffsetCommitResponseData response = new OffsetCommitResponseData();
        request.topics().forEach(topic -> {
            OffsetCommitResponseTopic responseTopic = new OffsetCommitResponseTopic()
                .setName(topic.name());
            response.topics().add(responseTopic);

            topic.partitions().forEach(partition ->
                responseTopic.partitions().add(new OffsetCommitResponsePartition()
                    .setPartitionIndex(partition.partitionIndex())
                    .setErrorCode(error.code()))
            );
        });
        return response;
    }

    @Override
    public OffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new OffsetCommitResponse(getErrorResponse(data, Errors.forException(e))
            .setThrottleTimeMs(throttleTimeMs));
    }

    @Override
    public OffsetCommitResponse getErrorResponse(Throwable e) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e);
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new OffsetCommitRequest(new OffsetCommitRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
