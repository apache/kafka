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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListOffsetsRequestTest {

    @Test
    public void testDuplicatePartitions() {
        List<ListOffsetsTopic> topics = Collections.singletonList(
                new ListOffsetsTopic()
                    .setName("topic")
                    .setPartitions(Arrays.asList(
                            new ListOffsetsPartition()
                                .setPartitionIndex(0),
                            new ListOffsetsPartition()
                                .setPartitionIndex(0))));
        ListOffsetsRequestData data = new ListOffsetsRequestData()
                .setTopics(topics)
                .setReplicaId(-1);
        ListOffsetsRequest request = ListOffsetsRequest.parse(MessageUtil.toByteBuffer(data, (short) 1), (short) 1);
        assertEquals(Collections.singleton(new TopicPartition("topic", 0)), request.duplicatePartitions());
        assertEquals(0, data.timeoutMs()); // default value
    }

    @Test
    public void testGetErrorResponse() {
        for (short version = 1; version <= ApiKeys.LIST_OFFSETS.latestVersion(); version++) {
            List<ListOffsetsTopic> topics = Collections.singletonList(
                    new ListOffsetsTopic()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new ListOffsetsPartition()
                                    .setPartitionIndex(0))));
            ListOffsetsRequest request = ListOffsetsRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_COMMITTED)
                    .setTargetTimes(topics)
                    .build(version);
            ListOffsetsResponse response = (ListOffsetsResponse) request.getErrorResponse(0, Errors.NOT_LEADER_OR_FOLLOWER.exception());
    
            List<ListOffsetsTopicResponse> v = Collections.singletonList(
                    new ListOffsetsTopicResponse()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new ListOffsetsPartitionResponse()
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setLeaderEpoch(ListOffsetsResponse.UNKNOWN_EPOCH)
                                    .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
                                    .setPartitionIndex(0)
                                    .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP))));
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(v);
            ListOffsetsResponse expectedResponse = new ListOffsetsResponse(data);
            assertEquals(expectedResponse.data().topics(), response.data().topics());
            assertEquals(expectedResponse.throttleTimeMs(), response.throttleTimeMs());
        }
    }

    @Test
    public void testToListOffsetsTopics() {
        ListOffsetsPartition lop0 = new ListOffsetsPartition()
                .setPartitionIndex(0)
                .setCurrentLeaderEpoch(1)
                .setTimestamp(123L);
        ListOffsetsPartition lop1 = new ListOffsetsPartition()
                .setPartitionIndex(1)
                .setCurrentLeaderEpoch(3)
                .setTimestamp(567L);
        Map<TopicPartition, ListOffsetsPartition> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(new TopicPartition("topic", 0), lop0);
        timestampsToSearch.put(new TopicPartition("topic", 1), lop1);
        List<ListOffsetsTopic> listOffsetTopics = ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch);
        assertEquals(1, listOffsetTopics.size());
        ListOffsetsTopic topic = listOffsetTopics.get(0);
        assertEquals("topic", topic.name());
        assertEquals(2, topic.partitions().size());
        assertTrue(topic.partitions().contains(lop0));
        assertTrue(topic.partitions().contains(lop1));
    }

    @Test
    public void testListOffsetsRequestOldestVersion() {
        ListOffsetsRequest.Builder consumerRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED);

        ListOffsetsRequest.Builder requireTimestampRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED);

        ListOffsetsRequest.Builder requestCommittedRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_COMMITTED);

        ListOffsetsRequest.Builder maxTimestampRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, true, false, false);

        ListOffsetsRequest.Builder requireEarliestLocalTimestampRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false, true, false);

        ListOffsetsRequest.Builder requireTieredStorageTimestampRequestBuilder = ListOffsetsRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED, false, false, true);

        assertEquals((short) 0, consumerRequestBuilder.oldestAllowedVersion());
        assertEquals((short) 1, requireTimestampRequestBuilder.oldestAllowedVersion());
        assertEquals((short) 2, requestCommittedRequestBuilder.oldestAllowedVersion());
        assertEquals((short) 7, maxTimestampRequestBuilder.oldestAllowedVersion());
        assertEquals((short) 8, requireEarliestLocalTimestampRequestBuilder.oldestAllowedVersion());
        assertEquals((short) 9, requireTieredStorageTimestampRequestBuilder.oldestAllowedVersion());
    }
}
