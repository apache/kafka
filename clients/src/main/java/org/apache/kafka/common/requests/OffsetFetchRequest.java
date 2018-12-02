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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.PARTITION_ID;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class OffsetFetchRequest extends AbstractRequest {
    // top level fields
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Topics to fetch offsets. If the topic array is null fetch offsets for all topics.");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Partitions to fetch offsets.");

    /*
     * Wire formats of version 0 and 1 are the same, but with different functionality.
     * Wire format of version 2 is similar to version 1, with the exception of
     * - accepting 'null' as list of topics
     * - returning a top level error code
     * Version 0 will read the offsets from ZK.
     * Version 1 will read the offsets from Kafka.
     * Version 2 will read the offsets from Kafka, and returns all associated topic partition offsets if
     * a 'null' is passed instead of a list of specific topic partitions. It also returns a top level error code
     * for group or coordinator level errors.
     */
    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID);

    private static final Field TOPICS_V0 = TOPICS.withFields("Topics to fetch offsets.",
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema OFFSET_FETCH_REQUEST_V0 = new Schema(
            GROUP_ID,
            TOPICS_V0);

    // V1 begins support for fetching offsets from the internal __consumer_offsets topic
    private static final Schema OFFSET_FETCH_REQUEST_V1 = OFFSET_FETCH_REQUEST_V0;

    // V2 adds top-level error code to the response as well as allowing a null offset array to indicate fetch
    // of all committed offsets for a group
    private static final Field TOPICS_V2 = TOPICS.nullableWithFields(
            TOPIC_NAME,
            PARTITIONS_V0);
    private static final Schema OFFSET_FETCH_REQUEST_V2 = new Schema(
            GROUP_ID,
            TOPICS_V2);

    // V3 request is the same as v2. Throttle time has been added to v3 response
    private static final Schema OFFSET_FETCH_REQUEST_V3 = OFFSET_FETCH_REQUEST_V2;

    // V4 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema OFFSET_FETCH_REQUEST_V4 = OFFSET_FETCH_REQUEST_V3;

    // V5 adds the leader epoch of the committed offset in the response
    private static final Schema OFFSET_FETCH_REQUEST_V5 = OFFSET_FETCH_REQUEST_V4;

    public static Schema[] schemaVersions() {
        return new Schema[] {OFFSET_FETCH_REQUEST_V0, OFFSET_FETCH_REQUEST_V1, OFFSET_FETCH_REQUEST_V2,
            OFFSET_FETCH_REQUEST_V3, OFFSET_FETCH_REQUEST_V4, OFFSET_FETCH_REQUEST_V5};
    }

    public static class Builder extends AbstractRequest.Builder<OffsetFetchRequest> {
        private static final List<TopicPartition> ALL_TOPIC_PARTITIONS = null;
        private final String groupId;
        private final List<TopicPartition> partitions;

        public Builder(String groupId, List<TopicPartition> partitions) {
            super(ApiKeys.OFFSET_FETCH);
            this.groupId = groupId;
            this.partitions = partitions;
        }

        public static Builder allTopicPartitions(String groupId) {
            return new Builder(groupId, ALL_TOPIC_PARTITIONS);
        }

        public boolean isAllTopicPartitions() {
            return this.partitions == ALL_TOPIC_PARTITIONS;
        }

        @Override
        public OffsetFetchRequest build(short version) {
            if (isAllTopicPartitions() && version < 2)
                throw new UnsupportedVersionException("The broker only supports OffsetFetchRequest " +
                        "v" + version + ", but we need v2 or newer to request all topic partitions.");
            return new OffsetFetchRequest(groupId, partitions, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            String partitionsString = partitions == null ? "<ALL>" : Utils.join(partitions, ",");
            bld.append("(type=OffsetFetchRequest, ").
                    append("groupId=").append(groupId).
                    append(", partitions=").append(partitionsString).
                    append(")");
            return bld.toString();
        }
    }

    private final String groupId;
    private final List<TopicPartition> partitions;

    public static OffsetFetchRequest forAllPartitions(String groupId) {
        return new OffsetFetchRequest.Builder(groupId, null).build((short) 2);
    }

    private OffsetFetchRequest(String groupId, List<TopicPartition> partitions, short version) {
        super(ApiKeys.OFFSET_FETCH, version);
        this.groupId = groupId;
        this.partitions = partitions;
    }

    public OffsetFetchRequest(Struct struct, short version) {
        super(ApiKeys.OFFSET_FETCH, version);

        Object[] topicArray = struct.get(TOPICS);
        if (topicArray != null) {
            partitions = new ArrayList<>();
            for (Object topicResponseObj : topicArray) {
                Struct topicResponse = (Struct) topicResponseObj;
                String topic = topicResponse.get(TOPIC_NAME);
                for (Object partitionResponseObj : topicResponse.get(PARTITIONS)) {
                    Struct partitionResponse = (Struct) partitionResponseObj;
                    int partition = partitionResponse.get(PARTITION_ID);
                    partitions.add(new TopicPartition(topic, partition));
                }
            }
        } else
            partitions = null;


        groupId = struct.get(GROUP_ID);
    }

    public OffsetFetchResponse getErrorResponse(Errors error) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, error);
    }

    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Errors error) {
        short versionId = version();

        Map<TopicPartition, OffsetFetchResponse.PartitionData> responsePartitions = new HashMap<>();
        if (versionId < 2) {
            OffsetFetchResponse.PartitionData partitionError = new OffsetFetchResponse.PartitionData(
                    OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(),
                    OffsetFetchResponse.NO_METADATA,
                    error);

            for (TopicPartition partition : this.partitions)
                responsePartitions.put(partition, partitionError);
        }

        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new OffsetFetchResponse(error, responsePartitions);
            case 3:
            case 4:
            case 5:
                return new OffsetFetchResponse(throttleTimeMs, error, responsePartitions);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.OFFSET_FETCH.latestVersion()));
        }
    }

    @Override
    public OffsetFetchResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(throttleTimeMs, Errors.forException(e));
    }

    public String groupId() {
        return groupId;
    }

    public List<TopicPartition> partitions() {
        return partitions;
    }

    public static OffsetFetchRequest parse(ByteBuffer buffer, short version) {
        return new OffsetFetchRequest(ApiKeys.OFFSET_FETCH.parseRequest(version, buffer), version);
    }

    public boolean isAllPartitions() {
        return partitions == null;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.OFFSET_FETCH.requestSchema(version()));
        struct.set(GROUP_ID, groupId);
        if (partitions != null) {
            Map<String, List<Integer>> topicsData = CollectionUtils.groupPartitionsByTopic(partitions);

            List<Struct> topicArray = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> entries : topicsData.entrySet()) {
                Struct topicData = struct.instance(TOPICS);
                topicData.set(TOPIC_NAME, entries.getKey());
                List<Struct> partitionArray = new ArrayList<>();
                for (Integer partitionId : entries.getValue()) {
                    Struct partitionData = topicData.instance(PARTITIONS);
                    partitionData.set(PARTITION_ID, partitionId);
                    partitionArray.add(partitionData);
                }
                topicData.set(PARTITIONS, partitionArray.toArray());
                topicArray.add(topicData);
            }
            struct.set(TOPICS, topicArray.toArray());
        } else
            struct.set(TOPICS, null);

        return struct;
    }

}
