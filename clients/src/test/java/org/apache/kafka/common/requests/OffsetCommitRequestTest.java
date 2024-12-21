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
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.OffsetCommitRequest.getErrorResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetCommitRequestTest {

    protected static String groupId = "groupId";
    protected static String memberId = "consumerId";
    protected static String groupInstanceId = "groupInstanceId";
    protected static String topicOne = "topicOne";
    protected static String topicTwo = "topicTwo";
    protected static int partitionOne = 1;
    protected static int partitionTwo = 2;
    protected static long offset = 100L;
    protected static short leaderEpoch = 20;
    protected static String metadata = "metadata";

    protected static int throttleTimeMs = 10;

    private static OffsetCommitRequestData data;

    @BeforeEach
    public void setUp() {
        List<OffsetCommitRequestTopic> topics = Arrays.asList(
            new OffsetCommitRequestTopic()
                .setName(topicOne)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitRequestPartition()
                        .setPartitionIndex(partitionOne)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata)
                )),
            new OffsetCommitRequestTopic()
                .setName(topicTwo)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitRequestPartition()
                        .setPartitionIndex(partitionTwo)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata)
                ))
        );
        data = new OffsetCommitRequestData()
                   .setGroupId(groupId)
                   .setTopics(topics);
    }

    @Test
    public void testConstructor() {
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(new TopicPartition(topicOne, partitionOne), offset);
        expectedOffsets.put(new TopicPartition(topicTwo, partitionTwo), offset);

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data);

        for (short version : ApiKeys.OFFSET_COMMIT.allVersions()) {
            OffsetCommitRequest request = builder.build(version);
            assertEquals(expectedOffsets, request.offsets());

            OffsetCommitResponse response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

            assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 2), response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }

    @Test
    public void testVersionSupportForGroupInstanceId() {
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
            new OffsetCommitRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGroupInstanceId(groupInstanceId)
        );

        for (short version : ApiKeys.OFFSET_COMMIT.allVersions()) {
            if (version >= 7) {
                builder.build(version);
            } else {
                final short finalVersion = version;
                assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
            }
        }
    }

    @Test
    public void testGetErrorResponse() {
        OffsetCommitResponseData expectedResponse = new OffsetCommitResponseData()
            .setTopics(Arrays.asList(
                new OffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(Collections.singletonList(
                        new OffsetCommitResponsePartition()
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                            .setPartitionIndex(partitionOne))),
                new OffsetCommitResponseTopic()
                    .setName(topicTwo)
                    .setPartitions(Collections.singletonList(
                        new OffsetCommitResponsePartition()
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                            .setPartitionIndex(partitionTwo)))));

        assertEquals(expectedResponse, getErrorResponse(data, Errors.UNKNOWN_MEMBER_ID));
    }
}
