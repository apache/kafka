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
import org.apache.kafka.common.TopicResolver;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.OffsetCommitRequest.getErrorResponseTopics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetCommitRequestTest {

    protected static String groupId = "groupId";
    protected static String memberId = "consumerId";
    protected static String groupInstanceId = "groupInstanceId";
    protected static String topicOne = "topicOne";
    protected static Uuid topicOneId = Uuid.randomUuid();
    protected static String topicTwo = "topicTwo";
    protected static int partitionOne = 1;
    protected static int partitionTwo = 2;
    protected static long offset = 100L;
    protected static short leaderEpoch = 20;
    protected static String metadata = "metadata";

    protected static int throttleTimeMs = 10;

    private static OffsetCommitRequestData data;
    private static List<OffsetCommitRequestTopic> topics;

    private static OffsetCommitRequestPartition requestPartitionOne;
    private static OffsetCommitRequestPartition requestPartitionTwo;

    @BeforeEach
    public void setUp() {
        requestPartitionOne = new OffsetCommitRequestPartition()
            .setPartitionIndex(partitionOne)
            .setCommittedOffset(offset)
            .setCommittedLeaderEpoch(leaderEpoch)
            .setCommittedMetadata(metadata);

        requestPartitionTwo = new OffsetCommitRequestPartition()
            .setPartitionIndex(partitionTwo)
            .setCommittedOffset(offset)
            .setCommittedLeaderEpoch(leaderEpoch)
            .setCommittedMetadata(metadata);

        topics = Arrays.asList(
            new OffsetCommitRequestTopic()
                .setName(topicOne)
                .setTopicId(topicOneId)
                .setPartitions(Collections.singletonList(requestPartitionOne)),
            new OffsetCommitRequestTopic()
                .setName(topicTwo)
                .setPartitions(Collections.singletonList(requestPartitionTwo))
        );
        data = new OffsetCommitRequestData()
                   .setGroupId(groupId)
                   .setTopics(topics);
    }

    public static Map<TopicPartition, Long> offsets(OffsetCommitRequest request, TopicResolver topicResolver) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (OffsetCommitRequestTopic topic : request.data().topics()) {
            String topicName = topic.name();

            if (request.version() >= 9 && topicName == null) {
                topicName = topicResolver.getTopicName(topic.topicId()).orElseThrow(
                        () -> new UnknownTopicIdException("Topic with ID " + topic.topicId() + " not found."));
            }

            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                offsets.put(new TopicPartition(topicName, partition.partitionIndex()),
                        partition.committedOffset());
            }
        }
        return offsets;
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testConstructor(short version) {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data.duplicate()).build(version);
        OffsetCommitResponse response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

        OffsetCommitRequestData expectedData = data.duplicate();
        if (version < 9) {
            expectedData.topics().forEach(t -> t.setTopicId(Uuid.ZERO_UUID));

        } else {
            expectedData.topics().stream()
                    .filter(t -> !t.topicId().equals(Uuid.ZERO_UUID))
                    .forEach(t -> t.setName(null));
        }

        assertEquals(expectedData, request.data());
        assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 2), response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testGetErrorResponseTopics(short version) {
        List<OffsetCommitResponseTopic> expectedTopics = Arrays.asList(
            new OffsetCommitResponseTopic()
                .setName(version < 9 ? topicOne : null)
                .setTopicId(version < 9 ? Uuid.ZERO_UUID : topicOneId)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitResponsePartition()
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                        .setPartitionIndex(partitionOne))),
            new OffsetCommitResponseTopic()
                .setName(topicTwo)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitResponsePartition()
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                        .setPartitionIndex(partitionTwo)))
        );
        assertEquals(expectedTopics, getErrorResponseTopics(topics, Errors.UNKNOWN_MEMBER_ID, version));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testVersionSupportForGroupInstanceId(short version) {
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
            new OffsetCommitRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGroupInstanceId(groupInstanceId)
        );

        if (version >= 7) {
            builder.build(version);
        } else {
            final short finalVersion = version;
            assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testHandlingOfTopicIdInAllVersions(short version) {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data.duplicate()).build(version);
        List<OffsetCommitRequestTopic> requestTopics = request.data().topics();

        if (version >= 9) {
            // Version >= 9:
            //   Topic ID may be present or not. Both are valid cases. If no topic ID is provided (null or
            //   set to ZERO_UUID), a topic name must be provided and will be used. If a topic ID is provided,
            //   the name will be nullified.
            assertNull(requestTopics.get(0).name());
            assertEquals(topicOneId, requestTopics.get(0).topicId());

            assertEquals(topicTwo, requestTopics.get(1).name());
            assertEquals(Uuid.ZERO_UUID, requestTopics.get(1).topicId());

        } else {
            // Version < 9:
            //   Topic ID may be present or not. They are set to ZERO_UUID in the finalized request. Any other
            //   value would make serialization of the request fail.
            assertEquals(topicOne, requestTopics.get(0).name());
            assertEquals(Uuid.ZERO_UUID, requestTopics.get(0).topicId());

            assertEquals(topicTwo, requestTopics.get(1).name());
            assertEquals(Uuid.ZERO_UUID, requestTopics.get(1).topicId());
        }
    }

    @Test
    public void testTopicIdMustBeSetIfNoTopicNameIsProvided() {
        OffsetCommitRequestTopic topic = new OffsetCommitRequestTopic()
            .setPartitions(Collections.singletonList(requestPartitionOne));
        OffsetCommitRequestData data = new OffsetCommitRequestData()
            .setGroupId(groupId)
            .setTopics(Collections.singletonList(topic));

        assertThrows(InvalidRequestException.class,
                () -> new OffsetCommitRequest.Builder(data.duplicate()).build((short) 9));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testResolvesTopicNameIfRequiredWhenListingOffsets(short version) {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data.duplicate()).build(version);
        List<OffsetCommitRequestTopic> topics = request.data().topics();

        assertEquals(2, topics.stream().flatMap(t -> t.partitions().stream()).count());
        assertEquals(requestPartitionOne, topics.get(0).partitions().get(0));
        assertEquals(requestPartitionTwo, topics.get(1).partitions().get(0));
    }

    @Test
    public void testUnresolvableTopicIdWhenListingOffset() {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data.duplicate()).build((short) 9);
        assertThrows(UnknownTopicIdException.class,
                () -> OffsetCommitRequestTest.offsets(request, TopicResolver.emptyResolver()));
    }
}
