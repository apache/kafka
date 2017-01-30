/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

public class OffsetFetchResponse extends AbstractResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.OFFSET_FETCH.id);
    private static final short CURRENT_VERSION = ProtoUtils.latestVersion(ApiKeys.OFFSET_FETCH.id);
    private static final String RESPONSES_KEY_NAME = "responses";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    // topic level fields
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level fields
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String COMMIT_OFFSET_KEY_NAME = "offset";
    private static final String METADATA_KEY_NAME = "metadata";

    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";
    public static final PartitionData UNKNOWN_PARTITION = new PartitionData(INVALID_OFFSET, NO_METADATA,
            Errors.UNKNOWN_TOPIC_OR_PARTITION);

    /**
     * Possible error codes:
     *
     * - Partition errors:
     *   - UNKNOWN_TOPIC_OR_PARTITION (3)
     *
     * - Group or coordinator errors:
     *   - GROUP_LOAD_IN_PROGRESS (14)
     *   - NOT_COORDINATOR_FOR_GROUP (16)
     *   - GROUP_AUTHORIZATION_FAILED (30)
     */

    private static final List<Errors> PARTITION_ERRORS = Arrays.asList(
            Errors.UNKNOWN_TOPIC_OR_PARTITION,
            Errors.TOPIC_AUTHORIZATION_FAILED);

    private final Map<TopicPartition, PartitionData> responseData;
    private final Errors error;

    public static final class PartitionData {
        public final long offset;
        public final String metadata;
        public final Errors error;

        public PartitionData(long offset, String metadata, Errors error) {
            this.offset = offset;
            this.metadata = metadata;
            this.error = error;
        }

        public boolean hasError() {
            return this.error != Errors.NONE;
        }
    }

    /**
     * Constructor for the latest version.
     * @param error Potential coordinator or group level error code
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        this(error, responseData, CURRENT_VERSION);
    }

    /**
     * Unified constructor for all versions.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     * @param version The request API version
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData, int version) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.OFFSET_FETCH.id, version)));

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> entries : topicsData.entrySet()) {
            Struct topicData = this.struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : entries.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(COMMIT_OFFSET_KEY_NAME, fetchPartitionData.offset);
                partitionData.set(METADATA_KEY_NAME, fetchPartitionData.metadata);
                partitionData.set(ERROR_CODE_KEY_NAME, fetchPartitionData.error.code());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }

        this.struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        this.responseData = responseData;
        this.error = error;
        if (version > 1)
            this.struct.set(ERROR_CODE_KEY_NAME, this.error.code());
    }

    public OffsetFetchResponse(Struct struct) {
        super(struct);
        Errors topLevelError = Errors.NONE;
        this.responseData = new HashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                long offset = partitionResponse.getLong(COMMIT_OFFSET_KEY_NAME);
                String metadata = partitionResponse.getString(METADATA_KEY_NAME);
                Errors error = Errors.forCode(partitionResponse.getShort(ERROR_CODE_KEY_NAME));
                if (error != Errors.NONE && !PARTITION_ERRORS.contains(error))
                    topLevelError = error;
                PartitionData partitionData = new PartitionData(offset, metadata, error);
                this.responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }

        // for version 2 and later use the top-level error code (in ERROR_CODE_KEY_NAME) from the response.
        // for older versions there is no top-level error in the response and all errors are partition errors,
        // so if there is a group or coordinator error at the partition level use that as the top-level error.
        // this way clients can depend on the top-level error regardless of the offset fetch version.
        this.error = struct.hasField(ERROR_CODE_KEY_NAME) ? Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME)) : topLevelError;
    }

    public void maybeThrowFirstPartitionError() {
        Collection<PartitionData> partitionsData = this.responseData.values();
        for (PartitionData data : partitionsData) {
            if (data.hasError())
                throw data.error.exception();
        }
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    public Errors error() {
        return this.error;
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer, int version) {
        Schema schema = ProtoUtils.responseSchema(ApiKeys.OFFSET_FETCH.id, version);
        return new OffsetFetchResponse(schema.read(buffer));
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer) {
        return new OffsetFetchResponse(CURRENT_SCHEMA.read(buffer));
    }
}
