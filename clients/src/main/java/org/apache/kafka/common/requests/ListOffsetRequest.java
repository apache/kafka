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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetRequestData;
import org.apache.kafka.common.message.ListOffsetRequestData.ListOffsetPartition;
import org.apache.kafka.common.message.ListOffsetRequestData.ListOffsetTopic;
import org.apache.kafka.common.message.ListOffsetResponseData;
import org.apache.kafka.common.message.ListOffsetResponseData.ListOffsetPartitionResponse;
import org.apache.kafka.common.message.ListOffsetResponseData.ListOffsetTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class ListOffsetRequest extends AbstractRequest {
    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;

    public static final int CONSUMER_REPLICA_ID = -1;
    public static final int DEBUGGING_REPLICA_ID = -2;

    private final ListOffsetRequestData data;
    private final Set<TopicPartition> duplicatePartitions;

    public static class Builder extends AbstractRequest.Builder<ListOffsetRequest> {
        private final ListOffsetRequestData data;

        public static Builder forReplica(short allowedVersion, int replicaId) {
            return new Builder((short) 0, allowedVersion, replicaId, IsolationLevel.READ_UNCOMMITTED);
        }

        public static Builder forConsumer(boolean requireTimestamp, IsolationLevel isolationLevel) {
            short minVersion = 0;
            if (isolationLevel == IsolationLevel.READ_COMMITTED)
                minVersion = 2;
            else if (requireTimestamp)
                minVersion = 1;
            return new Builder(minVersion, ApiKeys.LIST_OFFSETS.latestVersion(), CONSUMER_REPLICA_ID, isolationLevel);
        }

        private Builder(short oldestAllowedVersion,
                        short latestAllowedVersion,
                        int replicaId,
                        IsolationLevel isolationLevel) {
            super(ApiKeys.LIST_OFFSETS, oldestAllowedVersion, latestAllowedVersion);
            data = new ListOffsetRequestData()
                      .setIsolationLevel(isolationLevel.id())
                      .setReplicaId(replicaId);
        }

        public Builder setTargetTimes(List<ListOffsetTopic> topics) {
            data.setTopics(topics);
            return this;
        }

        @Override
        public ListOffsetRequest build(short version) {
            return new ListOffsetRequest(version, data);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    private ListOffsetRequest(short version, ListOffsetRequestData data) {
        super(ApiKeys.LIST_OFFSETS, version);
        this.data = data;
        this.duplicatePartitions = Collections.emptySet();
    }

    public ListOffsetRequest(Struct struct, short version) {
        super(ApiKeys.LIST_OFFSETS, version);
        data = new ListOffsetRequestData(struct, version);
        duplicatePartitions = new HashSet<>();
        Set<TopicPartition> partitions = new HashSet<>();
        for (ListOffsetTopic topic : data.topics()) {
            for (ListOffsetPartition partition : topic.partitions()) {
                TopicPartition tp = new TopicPartition(topic.name(), partition.partitionIndex());
                if (!partitions.add(tp)) {
                    duplicatePartitions.add(tp);
                }
            }
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        short errorCode = Errors.forException(e).code();

        List<ListOffsetTopicResponse> responses = new ArrayList<>();
        for (ListOffsetTopic topic : data.topics()) {
            ListOffsetTopicResponse topicResponse = new ListOffsetTopicResponse().setName(topic.name());
            List<ListOffsetPartitionResponse> partitions = new ArrayList<>();
            for (ListOffsetPartition partition : topic.partitions()) {
                ListOffsetPartitionResponse partitionResponse = new ListOffsetPartitionResponse()
                        .setErrorCode(errorCode)
                        .setPartitionIndex(partition.partitionIndex());
                if (versionId == 0) {
                    partitionResponse.setOldStyleOffsets(Collections.emptyList());
                } else {
                    partitionResponse.setOffset(ListOffsetResponse.UNKNOWN_OFFSET)
                                     .setTimestamp(ListOffsetResponse.UNKNOWN_TIMESTAMP);
                }
                partitions.add(partitionResponse);
            }
            topicResponse.setPartitions(partitions);
            responses.add(topicResponse);
        }
        ListOffsetResponseData responseData = new ListOffsetResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setTopics(responses);
        return new ListOffsetResponse(responseData);
    }

    public ListOffsetRequestData data() {
        return data;
    }

    public int replicaId() {
        return data.replicaId();
    }

    public IsolationLevel isolationLevel() {
        return IsolationLevel.forId(data.isolationLevel());
    }

    public List<ListOffsetTopic> topics() {
        return data.topics();
    }

    public Set<TopicPartition> duplicatePartitions() {
        return duplicatePartitions;
    }

    public static ListOffsetRequest parse(ByteBuffer buffer, short version) {
        return new ListOffsetRequest(ApiKeys.LIST_OFFSETS.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    public static List<ListOffsetTopic> toListOffsetTopics(Map<TopicPartition, ListOffsetPartition> timestampsToSearch) {
        Map<String, ListOffsetTopic> topics = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetPartition> entry : timestampsToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            ListOffsetTopic topic = topics.computeIfAbsent(tp.topic(), k -> new ListOffsetTopic().setName(tp.topic()));
            topic.partitions().add(entry.getValue());
        }
        return new ArrayList<>(topics.values());
    }

    public static ListOffsetTopic singletonRequestData(String topic, int partitionIndex, long timestamp, int maxNumOffsets) {
        return new ListOffsetTopic()
                .setName(topic)
                .setPartitions(Collections.singletonList(new ListOffsetPartition()
                        .setPartitionIndex(partitionIndex)
                        .setTimestamp(timestamp)
                        .setMaxNumOffsets(maxNumOffsets)));
    }
}
