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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_ID;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetCommitResponseTest {

    protected final int throttleTimeMs = 10;

    protected final String topic1 = "topic1";
    protected final Uuid topic1Id = Uuid.randomUuid();
    protected final int partitionOne = 1;
    protected final int partitionTwo = 2;
    protected final Errors errorOne = Errors.COORDINATOR_NOT_AVAILABLE;
    protected final Errors errorTwo = Errors.NOT_COORDINATOR;
    protected final String topic2 = "topic2";
    protected final int p3 = 3;
    protected final int p4 = 4;
    protected final String topic3 = "topic3";
    protected final Uuid topic3Id = Uuid.randomUuid();
    protected final int p5 = 5;
    protected final int p6 = 6;
    protected final String topic4 = "topic4";
    protected final Uuid topic4Id = Uuid.randomUuid();
    protected final int p7 = 7;
    protected final int p8 = 8;
    protected final String topic5 = "topic5";
    protected final Uuid topic5Id = Uuid.randomUuid();
    protected final int p9 = 9;
    protected final int p10 = 10;
    protected final String topic6 = "topic6";
    protected final Uuid topic6Id = Uuid.randomUuid();
    protected final int p11 = 11;
    protected final int p12 = 12;
    protected final Uuid topic7Id = Uuid.randomUuid();
    protected final int p13 = 13;
    protected final int p14 = 14;
    protected final Uuid topic8Id = Uuid.randomUuid();
    protected final int p15 = 15;
    protected final int p16 = 16;

    protected TopicPartition tp1 = new TopicPartition(topic1, partitionOne);
    protected TopicPartition tp2 = new TopicPartition(topic2, partitionTwo);
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
            .setTopics(Arrays.asList(
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partitionOne)
                        .setErrorCode(errorOne.code()))),
                new OffsetCommitResponseTopic().setPartitions(
                    Collections.singletonList(new OffsetCommitResponsePartition()
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
            .addPartition(topic1, topic1Id, partitionOne, Errors.NONE)
            .addPartition(topic1, topic1Id, partitionTwo, Errors.NONE)

            // Only topic name is defined.
            .addPartition(topic2, Uuid.ZERO_UUID, p3, Errors.NONE)
            .addPartition(topic2, Uuid.ZERO_UUID, p4, Errors.NONE)

            // Topic name is defined, topic id is defined on the second method call.
            .addPartition(topic3, Uuid.ZERO_UUID, p5, Errors.NONE)
            .addPartition(topic3, topic3Id, p6, Errors.NONE)

            // Topic name is defined, topic id is defined on the second method call.
            .addPartition(topic4, topic4Id, p7, Errors.NONE)
            .addPartition(topic4, Uuid.ZERO_UUID, p8, Errors.NONE)

            // Topic name and id are both defined.
            .addPartitions(topic5, topic5Id, asList(p9, p10), identity(), Errors.NONE)

            // Only topic name is defined.
            .addPartitions(topic6, topic6Id, asList(p11, p12), identity(), Errors.NONE);

        if (version >= 9) {
            // Undefined topic names are only supported from version 9 when a topic ID is provided.
            builder.addPartitions(null, topic7Id, asList(p13, p14), identity(), UNKNOWN_TOPIC_ID)
            // Add another topic to confirm topic 7 is not overwritten.
                .addPartitions(null, topic8Id, asList(p15, p16), identity(), UNKNOWN_TOPIC_OR_PARTITION);
        }

        List<OffsetCommitResponseTopic> expectedTopics = new ArrayList<>(asList(
            createResponseTopic(version, topic1, topic1Id, partitionOne, partitionTwo, Errors.NONE),
            createResponseTopic(version, topic2, Uuid.ZERO_UUID, p3, p4, Errors.NONE),
            createResponseTopic(version, topic3, topic3Id, p5, p6, Errors.NONE),
            createResponseTopic(version, topic4, topic4Id, p7, p8, Errors.NONE),
            createResponseTopic(version, topic5, topic5Id, p9, p10, Errors.NONE),
            createResponseTopic(version, topic6, topic6Id, p11, p12, Errors.NONE)
        ));

        if (version >= 9) {
            expectedTopics.addAll(asList(
                createResponseTopic(version, null, topic7Id, p13, p14, UNKNOWN_TOPIC_ID),
                createResponseTopic(version, null, topic8Id, p15, p16, UNKNOWN_TOPIC_OR_PARTITION)
            ));
        }

        assertEquals(new OffsetCommitResponseData().setTopics(expectedTopics), builder.build(version).data());
    }

    @Test
    public void testExceptionIsThrownIfInconsistentIdIsProvided() {
        OffsetCommitResponse.Builder builder = new OffsetCommitResponse.Builder()
            .addPartition(topic1, topic1Id, partitionOne, Errors.NONE);

        assertThrows(IllegalArgumentException.class,
            () -> builder.addPartition(topic1, topic3Id, partitionTwo, Errors.NONE));
    }

    @Test
    public void testExceptionIsThrownIfAddPartitionIsCalledWithATopicName() {
        assertThrows(IllegalArgumentException.class,
            () -> new OffsetCommitResponse.Builder().addPartition(null, topic1Id, partitionOne, Errors.NONE));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testExceptionIsThrownIfTopicNameIsNullPriorVersion9(short version) {
        OffsetCommitResponse.Builder builder = new OffsetCommitResponse.Builder()
            .addPartitions(null, topic1Id, asList(partitionOne, partitionTwo), identity(), Errors.NONE);

        if (version < 9) {
            assertThrows(InvalidRequestException.class, () -> builder.build(version));

        } else {
            OffsetCommitResponseTopic responseTopic = new OffsetCommitResponseTopic()
                .setTopicId(topic1Id)
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

    private static OffsetCommitResponseTopic createResponseTopic(
            short version, String topicName, Uuid topicId, int firstPartition, int secondPartition, Errors error) {
        return new OffsetCommitResponseTopic()
            .setTopicId(version >= 9 ? topicId : Uuid.ZERO_UUID)
            .setName(version >= 9 && !topicId.equals(Uuid.ZERO_UUID) ? null : topicName)
            .setPartitions(asList(
                new OffsetCommitResponsePartition().setPartitionIndex(firstPartition).setErrorCode(error.code()),
                new OffsetCommitResponsePartition().setPartitionIndex(secondPartition).setErrorCode(error.code()))
            );
    }
}
