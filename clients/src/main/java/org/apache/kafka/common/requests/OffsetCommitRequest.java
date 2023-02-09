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

import org.apache.kafka.common.TopicResolver;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        public Builder(OffsetCommitRequestData data) {
            super(ApiKeys.OFFSET_COMMIT);
            this.data = data;
        }

        @Override
        public OffsetCommitRequest build(short version) {
            if (data.groupInstanceId() != null && version < 7) {
                throw new UnsupportedVersionException("The broker offset commit protocol version " +
                        version + " does not support usage of config group.instance.id.");
            }

            // Copy since we can mutate it.
            OffsetCommitRequestData requestData = data.duplicate();

            if (version >= 9) {
                requestData.topics().forEach(topic -> {
                    // Set the topic name to null if a topic ID for the topic is present. If no topic ID is
                    // provided (i.e. its value is ZERO_UUID), the client should provide a topic name as a
                    // fallback. This allows the OffsetCommit API to support both topic IDs and topic names
                    // inside the same request or response.
                    if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                        topic.setName(null);
                    } else if (topic.name() == null || "".equals(topic.name())) {
                        // Fail-fast the entire request. This means that a single invalid topic in a multi-topic
                        // request will make it fail. We may want to relax the constraint to allow the request
                        // with valid topics (i.e. for which a valid ID or name was provided) exist in the request.
                        throw new UnknownTopicOrPartitionException(
                                "A topic name must be provided when no topic ID is specified.");
                    }
                });
            } else {
                requestData.topics().forEach(topic -> {
                    // Topic must be set to default for version < 9.
                    if (!Uuid.ZERO_UUID.equals(topic.topicId())) {
                        topic.setTopicId(Uuid.ZERO_UUID);
                    }
                    // Topic name must not be null. Validity will be checked at serialization time.
                });
            }
            return new OffsetCommitRequest(requestData, version);
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

    public Map<TopicPartition, Long> offsets(TopicResolver topicResolver) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (OffsetCommitRequestTopic topic : data.topics()) {
            String topicName = topic.name();

            if (version() >= 9 && topicName == null) {
                topicName = topicResolver.getTopicName(topic.topicId()).orElseThrow(
                        () -> new UnknownTopicIdException("Topic with ID " + topic.topicId() + " not found."));
            }

            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                offsets.put(new TopicPartition(topicName, partition.partitionIndex()),
                        partition.committedOffset());
            }
        }
        return offsets;
    }

    public static List<OffsetCommitResponseTopic> getErrorResponseTopics(
            List<OffsetCommitRequestTopic> requestTopics,
            Errors e) {
        List<OffsetCommitResponseTopic> responseTopicData = new ArrayList<>();
        for (OffsetCommitRequestTopic entry : requestTopics) {
            List<OffsetCommitResponsePartition> responsePartitions =
                    new ArrayList<>();
            for (OffsetCommitRequestData.OffsetCommitRequestPartition requestPartition : entry.partitions()) {
                responsePartitions.add(new OffsetCommitResponsePartition()
                                           .setPartitionIndex(requestPartition.partitionIndex())
                                           .setErrorCode(e.code()));
            }
            responseTopicData.add(new OffsetCommitResponseTopic()
                    .setName(entry.name())
                    .setTopicId(entry.topicId())
                    .setPartitions(responsePartitions)
            );
        }
        return responseTopicData;
    }

    @Override
    public OffsetCommitResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<OffsetCommitResponseTopic>
                responseTopicData = getErrorResponseTopics(data.topics(), Errors.forException(e));
        return new OffsetCommitResponse(new OffsetCommitResponseData()
                .setTopics(responseTopicData)
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
