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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetCommitResponseTest {

    protected final int throttleTimeMs = 10;

    protected final String topicOne = "topic1";
    protected final Uuid topicOneId = Uuid.randomUuid();
    protected final int partitionOne = 1;
    protected final int partitionTwo = 2;
    protected final Errors errorOne = Errors.COORDINATOR_NOT_AVAILABLE;
    protected final Errors errorTwo = Errors.NOT_COORDINATOR;
    protected final String topicTwo = "topic2";
    protected final int partitionThree = 3;
    protected final int partitionFour = 4;
    protected final String topicThree = "topic3";
    protected final Uuid topicThreeId = Uuid.randomUuid();
    protected final int partitionFive = 5;
    protected final int partitionSix = 6;
    protected final String topicFour = "topic4";
    protected final Uuid topicFourId = Uuid.randomUuid();
    protected final int partitionSeven = 7;
    protected final int partitionEight = 8;
    protected final String topicFive = "topic5";
    protected final Uuid topicFiveId = Uuid.randomUuid();
    protected final int partitionNine = 9;
    protected final int partitionTen = 10;
    protected final String topicSix = "topic6";
    protected final Uuid topicSixId = Uuid.randomUuid();
    protected final int partitionEleven = 11;
    protected final int partitionTwelve = 12;
    protected final Uuid topicSevenId = Uuid.randomUuid();
    protected final int partitionThirteen = 13;
    protected final int partitionFourteen = 14;
    protected final Uuid topicEightId = Uuid.randomUuid();
    protected final int partitionFifteen = 15;
    protected final int partitionSixteen = 16;

    protected TopicPartition tp1 = new TopicPartition(topicOne, partitionOne);
    protected TopicPartition tp2 = new TopicPartition(topicTwo, partitionTwo);
    protected Map<Errors, Integer> expectedErrorCounts;
    protected Map<TopicPartition, Errors> errorsMap;

    @BeforeEach
    public void setUp() {
        expectedErrorCounts = new HashMap<>();
        expectedErrorCounts.put(errorOne, 1);
        expectedErrorCounts.put(errorTwo, 1);

        errorsMap = new HashMap<>();
        errorsMap.put(tp1, errorOne);
        errorsMap.put(tp2, errorTwo);
    }

    @Test
    public void testConstructorWithErrorResponse() {
        OffsetCommitResponse response = new OffsetCommitResponse(throttleTimeMs, errorsMap);

        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testParse(short version) {
        OffsetCommitResponseData data = new OffsetCommitResponseData()
            .setTopics(asList(
                new OffsetCommitResponseTopic().setPartitions(
                    singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionOne)
                        .setErrorCode(errorOne.code()))),
                new OffsetCommitResponseTopic().setPartitions(
                    singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionTwo)
                        .setErrorCode(errorTwo.code())))
            ))
            .setThrottleTimeMs(throttleTimeMs);

        ByteBuffer buffer = MessageUtil.toByteBuffer(data, version);
        OffsetCommitResponse response = OffsetCommitResponse.parse(buffer, version);
        assertEquals(expectedErrorCounts, response.errorCounts());

        if (version >= 3) {
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        } else {
            assertEquals(DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
        }

        assertEquals(version >= 4, response.shouldClientThrottle(version));
    }
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testOffsetCommitResponseBuilder(short version) {
        OffsetCommitResponse.Builder builder = new OffsetCommitResponse.Builder()
            // Both topic name and id are defined.
            .addPartition(topicOne, topicOneId, partitionOne, Errors.NONE)
            .addPartition(topicOne, topicOneId, partitionTwo, Errors.NONE)

            // Only topic name is defined.
            .addPartition(topicTwo, Uuid.ZERO_UUID, partitionThree, Errors.NONE)
            .addPartition(topicTwo, Uuid.ZERO_UUID, partitionFour, Errors.NONE)

            // Topic name is defined, topic id is defined on the second method call.
            .addPartition(topicThree, Uuid.ZERO_UUID, partitionFive, Errors.NONE)
            .addPartition(topicThree, topicThreeId, partitionSix, Errors.NONE)

            // Topic name is defined, topic id is defined on the second method call.
            .addPartition(topicFour, topicFourId, partitionSeven, Errors.NONE)
            .addPartition(topicFour, Uuid.ZERO_UUID, partitionEight, Errors.NONE)

            // Topic name and id are both defined.
            .addPartitions(topicFive, topicFiveId, asList(partitionNine, partitionTen), identity(), Errors.NONE)

            // Only topic name is defined.
            .addPartitions(topicSix, topicSixId, asList(partitionEleven, partitionTwelve), identity(), Errors.NONE);

        if (version >= 9) {
            // Undefined topic names are only supported from version 9 when a topic ID is provided.
            builder.addPartitions(
                null,
                topicSevenId,
                asList(partitionThirteen, partitionFourteen),
                identity(),
                Errors.UNKNOWN_TOPIC_ID);

            // Another undefined topic name. Adding it here to validate it is not overwriting topicSeven.
            builder.addPartitions(
                null,
                topicEightId,
                asList(partitionFifteen, partitionSixteen),
                identity(),
                Errors.UNKNOWN_TOPIC_OR_PARTITION);
        }

        OffsetCommitResponseTopic responseTopicOne = new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicOneId : Uuid.ZERO_UUID)
            .setName(version >= 9 ? null : topicOne)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionOne),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionTwo))
            );

        OffsetCommitResponseTopic responseTopicTwo = new OffsetCommitResponseTopic()
            .setTopicId(Uuid.ZERO_UUID)
            .setName(topicTwo)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionThree),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionFour))
            );

        OffsetCommitResponseTopic responseTopicThree = new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicThreeId : Uuid.ZERO_UUID)
            .setName(version >= 9 ? null : topicThree)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionFive),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionSix))
            );

        OffsetCommitResponseTopic responseTopicFour = new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicFourId : Uuid.ZERO_UUID)
            .setName(version >= 9 ? null : topicFour)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionSeven),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionEight))
            );

        OffsetCommitResponseTopic responseTopicFive = new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicFiveId : Uuid.ZERO_UUID)
            .setName(version >= 9 ? null : topicFive)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionNine),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionTen))
            );

        OffsetCommitResponseTopic responseTopicSix = new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicSixId : Uuid.ZERO_UUID)
            .setName(version >= 9 ? null : topicSix)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(partitionEleven),
                new OffsetCommitResponsePartition().setPartitionIndex(partitionTwelve))
            );

        OffsetCommitResponseTopic responseTopicSeven = new OffsetCommitResponseTopic()
            .setTopicId(topicSevenId)
            .setName(null)
            .setPartitions(asList(
                new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionThirteen)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()),
                new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionFourteen)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()))
            );

        OffsetCommitResponseTopic responseTopicEight = new OffsetCommitResponseTopic()
            .setTopicId(topicEightId)
            .setName(null)
            .setPartitions(asList(
                new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionFifteen)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()),
                new OffsetCommitResponsePartition()
                    .setPartitionIndex(partitionSixteen)
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()))
            );

        List<OffsetCommitResponseTopic> expectedTopics = new ArrayList<>(asList(
            responseTopicOne,
            responseTopicTwo,
            responseTopicThree,
            responseTopicFour,
            responseTopicFive,
            responseTopicSix));

        if (version >= 9) {
            expectedTopics.addAll(asList(responseTopicSeven, responseTopicEight));
        }

        assertEquals(new OffsetCommitResponseData().setTopics(expectedTopics), builder.build(version).data());
    }

    @Test
    public void testExceptionIsThrownIfInconsistentIdIsProvided() {
        OffsetCommitResponse.Builder builder = new OffsetCommitResponse.Builder()
            .addPartition(topicOne, topicOneId, partitionOne, Errors.NONE);

        assertThrows(IllegalArgumentException.class,
                () -> builder.addPartition(topicOne, topicThreeId, partitionTwo, Errors.NONE));
    }

    @Test
    public void testExceptionIsThrownIfAddPartitionIsCalledWithATopicName() {
        assertThrows(IllegalArgumentException.class,
                () -> new OffsetCommitResponse.Builder().addPartition(null, topicOneId, partitionOne, Errors.NONE));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testExceptionIsThrownIfTopicNameIsNullPriorVersion9(short version) {
        OffsetCommitResponse.Builder builder = new OffsetCommitResponse.Builder()
            .addPartitions(null, topicOneId, asList(partitionOne, partitionTwo), identity(), Errors.NONE);

        if (version < 9) {
            assertThrows(InvalidRequestException.class, () -> builder.build(version));

        } else {
            OffsetCommitResponseTopic responseTopic = new OffsetCommitResponseTopic()
                .setTopicId(topicOneId)
                .setName(null)
                .setPartitions(asList(
                        new OffsetCommitResponsePartition().setPartitionIndex(partitionOne),
                        new OffsetCommitResponsePartition().setPartitionIndex(partitionTwo))
                );

            assertEquals(
                new OffsetCommitResponseData().setTopics(singletonList(responseTopic)),
                builder.build(version).data());
        }
    }
}
