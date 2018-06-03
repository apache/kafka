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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class DeleteRecordsRequest extends AbstractRequest {

    public static final long HIGH_WATERMARK = -1L;

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String TIMEOUT_KEY_NAME = "timeout";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String OFFSET_KEY_NAME = "offset";


    private static final Schema DELETE_RECORDS_REQUEST_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(OFFSET_KEY_NAME, INT64, "The offset before which the messages will be deleted. -1 means high-watermark for the partition."));

    private static final Schema DELETE_RECORDS_REQUEST_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(DELETE_RECORDS_REQUEST_PARTITION_V0)));

    private static final Schema DELETE_RECORDS_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(DELETE_RECORDS_REQUEST_TOPIC_V0)),
            new Field(TIMEOUT_KEY_NAME, INT32, "The maximum time to await a response in ms."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_RECORDS_REQUEST_V1 = DELETE_RECORDS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_RECORDS_REQUEST_V0, DELETE_RECORDS_REQUEST_V1};
    }

    private final int timeout;
    private final Map<TopicPartition, Long> partitionOffsets;

    public static class Builder extends AbstractRequest.Builder<DeleteRecordsRequest> {
        private final int timeout;
        private final Map<TopicPartition, Long> partitionOffsets;

        public Builder(int timeout, Map<TopicPartition, Long> partitionOffsets) {
            super(ApiKeys.DELETE_RECORDS);
            this.timeout = timeout;
            this.partitionOffsets = partitionOffsets;
        }

        @Override
        public DeleteRecordsRequest build(short version) {
            return new DeleteRecordsRequest(timeout, partitionOffsets, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=DeleteRecordsRequest")
                   .append(", timeout=").append(timeout)
                   .append(", partitionOffsets=(").append(partitionOffsets)
                   .append("))");
            return builder.toString();
        }
    }


    public DeleteRecordsRequest(Struct struct, short version) {
        super(version);
        partitionOffsets = new HashMap<>();
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.get(TOPIC_NAME);
            for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.get(PARTITION_ID);
                long offset = partitionStruct.getLong(OFFSET_KEY_NAME);
                partitionOffsets.put(new TopicPartition(topic, partition), offset);
            }
        }
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    public DeleteRecordsRequest(int timeout, Map<TopicPartition, Long> partitionOffsets, short version) {
        super(version);
        this.timeout = timeout;
        this.partitionOffsets = partitionOffsets;
    }
    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DELETE_RECORDS.requestSchema(version()));
        Map<String, Map<Integer, Long>> offsetsByTopic = CollectionUtils.groupDataByTopic(partitionOffsets);
        struct.set(TIMEOUT_KEY_NAME, timeout);
        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Long>> offsetsByTopicEntry : offsetsByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, offsetsByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, Long> offsetsByPartitionEntry : offsetsByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(PARTITION_ID, offsetsByPartitionEntry.getKey());
                partitionStruct.set(OFFSET_KEY_NAME, offsetsByPartitionEntry.getValue());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> responseMap = new HashMap<>();

        for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
            responseMap.put(entry.getKey(), new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.forException(e)));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new DeleteRecordsResponse(throttleTimeMs, responseMap);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.DELETE_RECORDS.latestVersion()));
        }
    }

    public int timeout() {
        return timeout;
    }

    public Map<TopicPartition, Long> partitionOffsets() {
        return partitionOffsets;
    }

    public static DeleteRecordsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsRequest(ApiKeys.DELETE_RECORDS.parseRequest(version, buffer), version);
    }
}
