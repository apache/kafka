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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicIdAndNameBiMap;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitResponseTest.NameAndId;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.kafka.common.requests.OffsetCommitRequest.getErrorResponseTopics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetCommitRequestTest {

    protected static String groupId = "groupId";
    protected static String memberId = "consumerId";
    protected static String groupInstanceId = "groupInstanceId";
    protected static String topicOne = "topicOne";
    protected static Uuid topicOneId = Uuid.randomUuid();
    protected static String topicTwo = "topicTwo";
    protected static Uuid topicTwoId = Uuid.randomUuid();
    protected static int partitionOne = 1;
    protected static int partitionTwo = 2;
    protected static long offset = 100L;
    protected static short leaderEpoch = 20;
    protected static String metadata = "metadata";

    protected static int throttleTimeMs = 10;

    private static OffsetCommitRequestData data;
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

        data = new OffsetCommitRequestData()
           .setGroupId(groupId)
           .setTopics(Arrays.asList(
               new OffsetCommitRequestTopic()
                   .setTopicId(topicOneId)
                   .setName(topicOne)
                   .setPartitions(Collections.singletonList(requestPartitionOne)),
               new OffsetCommitRequestTopic()
                   .setTopicId(topicTwoId)
                   .setName(topicTwo)
                   .setPartitions(Collections.singletonList(requestPartitionTwo))
           ));
    }

    public static Map<TopicPartition, Long> offsets(
        OffsetCommitRequest request,
        TopicIdAndNameBiMap topicIdAndNames
    ) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (OffsetCommitRequestTopic topic : request.data().topics()) {
            String topicName = topic.name();

            if (request.version() >= 9) {
                topicName = topicIdAndNames.topicNameOrNull(topic.topicId());
                if (topicName == null) {
                    throw new UnknownTopicIdException("Topic with ID " + topic.topicId() + " not found.");
                }
            }

            for (OffsetCommitRequestData.OffsetCommitRequestPartition partition : topic.partitions()) {
                offsets.put(new TopicPartition(topicName, partition.partitionIndex()), partition.committedOffset());
            }
        }
        return offsets;
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testConstructor(short version) {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data, true).build(version);
        OffsetCommitResponse response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

        assertEquals(data, request.data());
        assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 2), response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @Test
    public void testGetErrorResponseTopics() {
        List<OffsetCommitResponseTopic> expectedTopics = Arrays.asList(
            new OffsetCommitResponseTopic()
                .setName(topicOne)
                .setTopicId(topicOneId)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitResponsePartition()
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                        .setPartitionIndex(partitionOne))),
            new OffsetCommitResponseTopic()
                .setName(topicTwo)
                .setTopicId(topicTwoId)
                .setPartitions(Collections.singletonList(
                    new OffsetCommitResponsePartition()
                        .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                        .setPartitionIndex(partitionTwo)))
        );
        assertEquals(expectedTopics, getErrorResponseTopics(data.topics(), Errors.UNKNOWN_MEMBER_ID));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testVersionSupportForGroupInstanceId(short version) {
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(
            new OffsetCommitRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setGroupInstanceId(groupInstanceId),
            true
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
    public void testResolvesTopicNameIfRequiredWhenListingOffsets(short version) {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data, true).build(version);
        List<OffsetCommitRequestTopic> topics = request.data().topics();

        assertEquals(2, topics.stream().flatMap(t -> t.partitions().stream()).count());
        assertEquals(requestPartitionOne, topics.get(0).partitions().get(0));
        assertEquals(requestPartitionTwo, topics.get(1).partitions().get(0));
    }

    @Test
    public void testUnresolvableTopicIdWhenListingOffset() {
        OffsetCommitRequest request = new OffsetCommitRequest.Builder(data.duplicate(), true).build((short) 9);
        assertThrows(UnknownTopicIdException.class,
            () -> OffsetCommitRequestTest.offsets(request, TopicIdAndNameBiMap.emptyMapping()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void maxAllowedVersionIsEightIfRequestCannotUseTopicIds(boolean canUseTopicIds) {
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(data.duplicate(), canUseTopicIds);
        assertEquals(canUseTopicIds ? 9 : 8, builder.build(builder.latestAllowedVersion()).version());
    }

    /**
     * Compares the two {@link OffsetCommitRequest} independently of the order in which the
     * {@link OffsetCommitRequestTopic} and {@link OffsetCommitRequestPartition} are defined in the response.
     */
    public static void assertRequestEquals(OffsetCommitRequest expectedRequest, OffsetCommitRequest actualRequest) {
        if (expectedRequest.version() > 9 || actualRequest.version() > 9) {
            throw new AssertionError("A new version of OffsetCommitRequest has been detected. Please " +
                    "review the equality contract enforced here and add/remove fields accordingly.");
        }

        OffsetCommitRequestData expected = expectedRequest.data();
        OffsetCommitRequestData actual = actualRequest.data();

        assertEquals(expectedRequest.version(), actualRequest.version());
        assertEquals(expected.groupId(), actual.groupId(), "Group id mismatch");
        assertEquals(expected.groupInstanceId(), actual.groupInstanceId(), "Group instance id mismatch");
        assertEquals(expected.generationId(), actual.generationId(), "Generation id mismatch");
        assertEquals(expected.memberId(), actual.memberId(), "Member id mismatch");
        assertEquals(expected.retentionTimeMs(), actual.retentionTimeMs(), "Retention time mismatch");
        assertEquals(partition(expected), partition(actual));
    }

    private static Map<TopicIdPartition, OffsetCommitRequestPartition> partition(OffsetCommitRequestData requestData) {
        Map<TopicIdPartition, OffsetCommitRequestPartition> topicIdPartitionToCommitResponse = new HashMap<>();
        List<OffsetCommitRequestTopic> topics = requestData.topics();
        for (OffsetCommitRequestTopic topic : topics) {
            List<OffsetCommitRequestPartition> partitions = topic.partitions();
            for (OffsetCommitRequestPartition partition : partitions) {
                topicIdPartitionToCommitResponse.put(
                        new TopicIdPartition(topic.topicId(), partition.partitionIndex(), topic.name()),
                        partition
                );
            }
        }
        return topicIdPartitionToCommitResponse;
    }

    /**
     * Compares the two {@link OffsetCommitResponse} independently of the order in which the
     * {@link OffsetCommitResponseTopic} and {@link OffsetCommitResponsePartition} are defined in the response.
     */
    public static void assertResponseEquals(OffsetCommitResponse expected, OffsetCommitResponse actual) {
        assertEquals(expected.throttleTimeMs(), actual.throttleTimeMs());
        assertEquals(expected.errorCounts(), actual.errorCounts());
        assertEquals(partition(expected), partition(actual));
    }

    private static Map<TopicIdPartition, OffsetCommitResponsePartition> partition(OffsetCommitResponse response) {
        Map<TopicIdPartition, OffsetCommitResponsePartition> topicIdPartitionToCommitResponse = new HashMap<>();
        List<OffsetCommitResponseTopic> topics = response.data().topics();
        for (OffsetCommitResponseTopic topic : topics) {
            List<OffsetCommitResponsePartition> partitions = topic.partitions();
            for (OffsetCommitResponsePartition partition : partitions) {
                topicIdPartitionToCommitResponse.put(
                        new TopicIdPartition(topic.topicId(), partition.partitionIndex(), topic.name()),
                        partition
                );
            }
        }
        return topicIdPartitionToCommitResponse;
    }
}
