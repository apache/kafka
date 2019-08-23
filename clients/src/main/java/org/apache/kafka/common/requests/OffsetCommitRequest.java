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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

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

    // default values for old versions,
    // will be removed after these versions are deprecated
    @Deprecated
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
            return new OffsetCommitRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final short version;

    public OffsetCommitRequest(OffsetCommitRequestData data, short version) {
        super(ApiKeys.OFFSET_COMMIT, version);
        this.data = data;
        this.version = version;
    }


    public OffsetCommitRequest(Struct struct, short version) {
        super(ApiKeys.OFFSET_COMMIT, version);
        this.data = new OffsetCommitRequestData(struct, version);
        this.version = version;
    }

    public OffsetCommitRequestData data() {
        return data;
    }

    public Map<TopicPartition, Long> offsets() {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : data.topics()) {
            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                offsets.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                        partition.committedOffset());
            }
        }
        return offsets;
    }

    public static List<OffsetCommitResponseData.OffsetCommitResponseTopic> getErrorResponseTopics(
            List<OffsetCommitRequestData.OffsetCommitRequestTopic> requestTopics,
            Errors e) {
        List<OffsetCommitResponseData.OffsetCommitResponseTopic>
                responseTopicData = new ArrayList<>();
        for (OffsetCommitRequestData.OffsetCommitRequestTopic entry : requestTopics) {
            List<OffsetCommitResponseData.OffsetCommitResponsePartition> responsePartitions =
                    new ArrayList<>();
            for (OffsetCommitRequestData.OffsetCommitRequestPartition requestPartition : entry.partitions()) {
                responsePartitions.add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
                        .setPartitionIndex(requestPartition.partitionIndex())
                        .setErrorCode(e.code()));
            }
            responseTopicData.add(new OffsetCommitResponseData.OffsetCommitResponseTopic()
                    .setName(entry.name())
                    .setPartitions(responsePartitions)
            );
        }
        return responseTopicData;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<OffsetCommitResponseData.OffsetCommitResponseTopic>
                responseTopicData = getErrorResponseTopics(data.topics(), Errors.forException(e));

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new OffsetCommitResponse(
                        new OffsetCommitResponseData()
                                .setTopics(responseTopicData)
                );
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return new OffsetCommitResponse(
                        new OffsetCommitResponseData()
                                .setTopics(responseTopicData)
                                .setThrottleTimeMs(throttleTimeMs)

                );
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.OFFSET_COMMIT.latestVersion()));
        }
    }

    public static OffsetCommitRequest parse(ByteBuffer buffer, short version) {
        return new OffsetCommitRequest(ApiKeys.OFFSET_COMMIT.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }
}
