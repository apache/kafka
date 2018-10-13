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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_LEADER_EPOCH;
import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_METADATA;
import static org.apache.kafka.common.protocol.CommonFields.COMMITTED_OFFSET;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - UNKNOWN_TOPIC_OR_PARTITION (3)
 *
 * - Group or coordinator errors:
 *   - COORDINATOR_LOAD_IN_PROGRESS (14)
 *   - COORDINATOR_NOT_AVAILABLE (15)
 *   - NOT_COORDINATOR (16)
 *   - GROUP_AUTHORIZATION_FAILED (30)
 */
public class OffsetFetchResponse extends AbstractResponse {
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("responses",
            "Responses by topic for fetched offsets");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partition_responses",
            "Responses by partition for fetched offsets");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            COMMITTED_OFFSET,
            COMMITTED_METADATA,
            ERROR_CODE);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema OFFSET_FETCH_RESPONSE_V0 = new Schema(
            TOPICS_V0);

    // V1 begins support for fetching offsets from the internal __consumer_offsets topic
    private static final Schema OFFSET_FETCH_RESPONSE_V1 = OFFSET_FETCH_RESPONSE_V0;

    // V2 adds top-level error code
    private static final Schema OFFSET_FETCH_RESPONSE_V2 = new Schema(
            TOPICS_V0,
            ERROR_CODE);

    // V3 request includes throttle time
    private static final Schema OFFSET_FETCH_RESPONSE_V3 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V0,
            ERROR_CODE);

    // V4 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema OFFSET_FETCH_RESPONSE_V4 = OFFSET_FETCH_RESPONSE_V3;

    // V5 adds the leader epoch to the committed offset
    private static final Field PARTITIONS_V5 = PARTITIONS.withFields(
            PARTITION_ID,
            COMMITTED_OFFSET,
            COMMITTED_LEADER_EPOCH,
            COMMITTED_METADATA,
            ERROR_CODE);

    private static final Field TOPICS_V5 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V5);

    private static final Schema OFFSET_FETCH_RESPONSE_V5 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V5,
            ERROR_CODE);

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_FETCH_RESPONSE_V0, OFFSET_FETCH_RESPONSE_V1, OFFSET_FETCH_RESPONSE_V2,
            OFFSET_FETCH_RESPONSE_V3, OFFSET_FETCH_RESPONSE_V4, OFFSET_FETCH_RESPONSE_V5};
    }

    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";
    public static final PartitionData UNKNOWN_PARTITION = new PartitionData(INVALID_OFFSET,
            Optional.empty(), NO_METADATA, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    public static final PartitionData UNAUTHORIZED_PARTITION = new PartitionData(INVALID_OFFSET,
            Optional.empty(), NO_METADATA, Errors.TOPIC_AUTHORIZATION_FAILED);

    private static final List<Errors> PARTITION_ERRORS = Collections.singletonList(Errors.UNKNOWN_TOPIC_OR_PARTITION);

    private final Map<TopicPartition, PartitionData> responseData;
    private final Errors error;
    private final int throttleTimeMs;

    public static final class PartitionData {
        public final long offset;
        public final String metadata;
        public final Errors error;
        public final Optional<Integer> leaderEpoch;

        public PartitionData(long offset,
                             Optional<Integer> leaderEpoch,
                             String metadata,
                             Errors error) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
            this.metadata = metadata;
            this.error = error;
        }

        public boolean hasError() {
            return this.error != Errors.NONE;
        }
    }

    /**
     * Constructor for all versions without throttle time.
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(Errors error, Map<TopicPartition, PartitionData> responseData) {
        this(DEFAULT_THROTTLE_TIME, error, responseData);
    }

    /**
     * Constructor with throttle time
     * @param throttleTimeMs The time in milliseconds that this response was throttled
     * @param error Potential coordinator or group level error code (for api version 2 and later)
     * @param responseData Fetched offset information grouped by topic-partition
     */
    public OffsetFetchResponse(int throttleTimeMs, Errors error, Map<TopicPartition, PartitionData> responseData) {
        this.throttleTimeMs = throttleTimeMs;
        this.responseData = responseData;
        this.error = error;
    }

    public OffsetFetchResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        Errors topLevelError = Errors.NONE;
        this.responseData = new HashMap<>();
        for (Object topicResponseObj : struct.get(TOPICS)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.get(TOPIC_NAME);
            for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.get(PARTITION_ID);
                long offset = partitionResponse.get(COMMITTED_OFFSET);
                String metadata = partitionResponse.get(COMMITTED_METADATA);
                Optional<Integer> leaderEpochOpt = RequestUtils.getLeaderEpoch(partitionResponse, COMMITTED_LEADER_EPOCH);

                Errors error = Errors.forCode(partitionResponse.get(ERROR_CODE));
                if (error != Errors.NONE && !PARTITION_ERRORS.contains(error))
                    topLevelError = error;

                PartitionData partitionData = new PartitionData(offset, leaderEpochOpt, metadata, error);
                this.responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }

        // for version 2 and later use the top-level error code (in ERROR_CODE_KEY_NAME) from the response.
        // for older versions there is no top-level error in the response and all errors are partition errors,
        // so if there is a group or coordinator error at the partition level use that as the top-level error.
        // this way clients can depend on the top-level error regardless of the offset fetch version.
        this.error = struct.hasField(ERROR_CODE) ? Errors.forCode(struct.get(ERROR_CODE)) : topLevelError;
    }

    public void maybeThrowFirstPartitionError() {
        Collection<PartitionData> partitionsData = this.responseData.values();
        for (PartitionData data : partitionsData) {
            if (data.hasError())
                throw data.error.exception();
        }
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    public Errors error() {
        return this.error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public static OffsetFetchResponse parse(ByteBuffer buffer, short version) {
        return new OffsetFetchResponse(ApiKeys.OFFSET_FETCH.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.OFFSET_FETCH.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupPartitionDataByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionData>> entries : topicsData.entrySet()) {
            Struct topicData = struct.instance(TOPICS);
            topicData.set(TOPIC_NAME, entries.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : entries.getValue().entrySet()) {
                PartitionData fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS);
                partitionData.set(PARTITION_ID, partitionEntry.getKey());
                partitionData.set(COMMITTED_OFFSET, fetchPartitionData.offset);
                RequestUtils.setLeaderEpochIfExists(partitionData, COMMITTED_LEADER_EPOCH, fetchPartitionData.leaderEpoch);
                partitionData.set(COMMITTED_METADATA, fetchPartitionData.metadata);
                partitionData.set(ERROR_CODE, fetchPartitionData.error.code());
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPICS, topicArray.toArray());

        if (version > 1)
            struct.set(ERROR_CODE, this.error.code());

        return struct;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }
}
