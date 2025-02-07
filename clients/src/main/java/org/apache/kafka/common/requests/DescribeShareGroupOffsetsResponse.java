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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeShareGroupOffsetsResponse extends AbstractResponse {
    private final DescribeShareGroupOffsetsResponseData data;
    private final Map<String, Errors> groupLevelErrors = new HashMap<>();

    public DescribeShareGroupOffsetsResponse(DescribeShareGroupOffsetsResponseData data) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);
        this.data = data;
        for (DescribeShareGroupOffsetsResponseGroup group : data.groups()) {
            this.groupLevelErrors.put(group.groupId(), Errors.forCode(group.errorCode()));
        }
    }

    public DescribeShareGroupOffsetsResponse(int throttleTimeMs,
                                             Map<String, Throwable> errorsMap,
                                             Map<String, Map<TopicIdPartition, DescribeShareGroupOffsetsResponsePartition>> responseData) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);
        List<DescribeShareGroupOffsetsResponseGroup> groupList = new ArrayList<>();
        for (Map.Entry<String, Map<TopicIdPartition, DescribeShareGroupOffsetsResponsePartition>> groupEntry : responseData.entrySet()) {
            String groupId = groupEntry.getKey();
            Map<TopicIdPartition, DescribeShareGroupOffsetsResponsePartition> partitionDataMap = groupEntry.getValue();
            Map<String, DescribeShareGroupOffsetsResponseTopic> topicDataMap = new HashMap<>();
            for (Map.Entry<TopicIdPartition, DescribeShareGroupOffsetsResponsePartition> partitionEntry : partitionDataMap.entrySet()) {
                String topicName = partitionEntry.getKey().topic();
                DescribeShareGroupOffsetsResponseTopic topicData =
                    topicDataMap.getOrDefault(topicName,
                        new DescribeShareGroupOffsetsResponseTopic()
                            .setTopicName(topicName)
                            .setTopicId(partitionEntry.getKey().topicId()));
                if (partitionEntry.getValue().errorCode() == Errors.NONE.code()) {
                    topicData.partitions().add(new DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partitionEntry.getKey().partition())
                        .setStartOffset(partitionEntry.getValue().startOffset())
                        .setLeaderEpoch(partitionEntry.getValue().leaderEpoch()));
                } else {
                    topicData.partitions().add(new DescribeShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partitionEntry.getKey().partition())
                        .setErrorCode(partitionEntry.getValue().errorCode())
                        .setErrorMessage(partitionEntry.getValue().errorMessage()));
                }
            }
            short errorCode = Errors.forException(errorsMap.get(groupId)).code();
            groupList.add(new DescribeShareGroupOffsetsResponseGroup()
                .setGroupId(groupId)
                .setTopics(new ArrayList<>(topicDataMap.values()))
                .setErrorCode(errorCode)
                .setErrorMessage(errorCode == Errors.UNKNOWN_SERVER_ERROR.code() ? Errors.forCode(errorCode).message() : errorsMap.get(groupId).getMessage()));
            groupLevelErrors.put(groupId, Errors.forException(errorsMap.get(groupId)));
        }
        this.data = new DescribeShareGroupOffsetsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setGroups(groupList);
    }

    public boolean hasGroupError(String groupId) {
        Errors groupError = groupLevelErrors.get(groupId);
        if (groupError != null) {
            return groupError != Errors.NONE;
        }
        return false;
    }

    public Errors groupError(String groupId) {
        return groupLevelErrors.get(groupId);
    }

    @Override
    public DescribeShareGroupOffsetsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        for (Map.Entry<String, Errors> entry: groupLevelErrors.entrySet()) {
            updateErrorCounts(counts, entry.getValue());
        }
        for (DescribeShareGroupOffsetsResponseGroup group : data.groups()) {
            group.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
        }
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static DescribeShareGroupOffsetsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeShareGroupOffsetsResponse(new DescribeShareGroupOffsetsResponseData(new ByteBufferAccessor(buffer), version));
    }
}
