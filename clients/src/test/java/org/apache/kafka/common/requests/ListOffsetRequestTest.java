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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.junit.Test;

public class ListOffsetRequestTest {

    @Test
    public void testDuplicatePartitions() {
        List<ListOffsetTopic> topics = Collections.singletonList(
                new ListOffsetTopic()
                    .setName("topic")
                    .setPartitions(Arrays.asList(
                            new ListOffsetPartition()
                                .setPartitionIndex(0),
                            new ListOffsetPartition()
                                .setPartitionIndex(0))));
        ListOffsetRequestData data = new ListOffsetRequestData()
                .setTopics(topics)
                .setReplicaId(-1);
        ListOffsetRequest request = new ListOffsetRequest(data.toStruct((short) 0), (short) 0);
        assertEquals(Collections.singleton(new TopicPartition("topic", 0)), request.duplicatePartitions());
    }

    @Test
    public void testGetErrorResponse() {
        for (short version = 1; version <= ApiKeys.LIST_OFFSETS.latestVersion(); version++) {
            List<ListOffsetTopic> topics = Arrays.asList(
                    new ListOffsetTopic()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new ListOffsetPartition()
                                    .setPartitionIndex(0))));
            ListOffsetRequest request = ListOffsetRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_COMMITTED)
                    .setTargetTimes(topics)
                    .build(version);
            ListOffsetResponse response = (ListOffsetResponse) request.getErrorResponse(0, Errors.NOT_LEADER_OR_FOLLOWER.exception());
    
            List<ListOffsetTopicResponse> v = Collections.singletonList(
                    new ListOffsetTopicResponse()
                        .setName("topic")
                        .setPartitions(Collections.singletonList(
                                new ListOffsetPartitionResponse()
                                    .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                    .setLeaderEpoch(ListOffsetResponse.UNKNOWN_EPOCH)
                                    .setOffset(ListOffsetResponse.UNKNOWN_OFFSET)
                                    .setPartitionIndex(0)
                                    .setTimestamp(ListOffsetResponse.UNKNOWN_TIMESTAMP))));
            ListOffsetResponseData data = new ListOffsetResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(v);
            ListOffsetResponse expectedResponse = new ListOffsetResponse(data);
            assertEquals(expectedResponse.data().topics(), response.data().topics());
            assertEquals(expectedResponse.throttleTimeMs(), response.throttleTimeMs());
        }
    }

    @Test
    public void testGetErrorResponseV0() {
        List<ListOffsetTopic> topics = Arrays.asList(
                new ListOffsetTopic()
                    .setName("topic")
                    .setPartitions(Collections.singletonList(
                            new ListOffsetPartition()
                                .setPartitionIndex(0))));
        ListOffsetRequest request = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(topics)
                .build((short) 0);
        ListOffsetResponse response = (ListOffsetResponse) request.getErrorResponse(0, Errors.NOT_LEADER_OR_FOLLOWER.exception());

        List<ListOffsetTopicResponse> v = Collections.singletonList(
                new ListOffsetTopicResponse()
                    .setName("topic")
                    .setPartitions(Collections.singletonList(
                            new ListOffsetPartitionResponse()
                                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
                                .setOldStyleOffsets(Collections.emptyList())
                                .setPartitionIndex(0))));
        ListOffsetResponseData data = new ListOffsetResponseData()
                .setThrottleTimeMs(0)
                .setTopics(v);
        ListOffsetResponse expectedResponse = new ListOffsetResponse(data);
        assertEquals(expectedResponse.data().topics(), response.data().topics());
        assertEquals(expectedResponse.throttleTimeMs(), response.throttleTimeMs());
    }

    @Test
    public void testToListOffsetTopics() {
        ListOffsetPartition lop0 = new ListOffsetPartition()
                .setPartitionIndex(0)
                .setCurrentLeaderEpoch(1)
                .setMaxNumOffsets(2)
                .setTimestamp(123L);
        ListOffsetPartition lop1 = new ListOffsetPartition()
                .setPartitionIndex(1)
                .setCurrentLeaderEpoch(3)
                .setMaxNumOffsets(4)
                .setTimestamp(567L);
        Map<TopicPartition, ListOffsetPartition> timestampsToSearch = new HashMap<>();
        timestampsToSearch.put(new TopicPartition("topic", 0), lop0);
        timestampsToSearch.put(new TopicPartition("topic", 1), lop1);
        List<ListOffsetTopic> listOffsetTopics = ListOffsetRequest.toListOffsetTopics(timestampsToSearch);
        assertEquals(1, listOffsetTopics.size());
        ListOffsetTopic topic = listOffsetTopics.get(0);
        assertEquals("topic", topic.name());
        assertEquals(2, topic.partitions().size());
        assertTrue(topic.partitions().contains(lop0));
        assertTrue(topic.partitions().contains(lop1));
    }

}
