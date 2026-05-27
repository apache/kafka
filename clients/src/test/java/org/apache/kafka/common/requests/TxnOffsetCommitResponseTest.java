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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TxnOffsetCommitResponseTest extends OffsetCommitResponseTest {

    private final Uuid topicOneId = Uuid.randomUuid();
    private final Uuid topicTwoId = Uuid.randomUuid();

    @Test
    @Override
    public void testConstructorWithErrorResponse() {
        TxnOffsetCommitResponse response = new TxnOffsetCommitResponse(throttleTimeMs, errorsMap);

        assertEquals(expectedErrorCounts, response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @Test
    @Override
    public void testParse() {
        TxnOffsetCommitResponseData data = new TxnOffsetCommitResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic().setPartitions(List.of(
                    new TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(partitionOne)
                        .setErrorCode(errorOne.code()))),
                new TxnOffsetCommitResponseTopic().setPartitions(List.of(
                    new TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(partitionTwo)
                        .setErrorCode(errorTwo.code())))));

        for (short version : ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            TxnOffsetCommitResponse response = TxnOffsetCommitResponse.parse(
                MessageUtil.toByteBufferAccessor(data, version), version);
            assertEquals(expectedErrorCounts, response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
            assertEquals(version >= 1, response.shouldClientThrottle(version));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderAddPartition(boolean useTopicIds) {
        var topicOneIdOrZero = useTopicIds ? topicOneId : Uuid.ZERO_UUID;
        var topicTwoIdOrZero = useTopicIds ? topicTwoId : Uuid.ZERO_UUID;

        var builder = TxnOffsetCommitResponse.newBuilder(useTopicIds);
        builder.addPartition(topicOneIdOrZero, topicOne, partitionOne, errorOne);
        builder.addPartition(topicOneIdOrZero, topicOne, partitionTwo, errorTwo);
        builder.addPartition(topicTwoIdOrZero, topicTwo, partitionOne, errorOne);

        var expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()),
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code()))),
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicTwoIdOrZero)
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code())))));

        assertEquals(expected, builder.build().data());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderAddPartitions(boolean useTopicIds) {
        var topicOneIdOrZero = useTopicIds ? topicOneId : Uuid.ZERO_UUID;

        var builder = TxnOffsetCommitResponse.newBuilder(useTopicIds);
        builder.addPartitions(
            topicOneIdOrZero,
            topicOne,
            List.of(
                new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(partitionOne),
                new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(partitionTwo)
            ),
            TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition::partitionIndex,
            errorOne
        );

        var expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()),
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorOne.code())))));

        assertEquals(expected, builder.build().data());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderMergeIntoEmpty(boolean useTopicIds) {
        var topicOneIdOrZero = useTopicIds ? topicOneId : Uuid.ZERO_UUID;

        var newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code())))));

        var response = TxnOffsetCommitResponse.newBuilder(useTopicIds)
            .merge(newData)
            .build();

        assertEquals(newData, response.data());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderMergeAddsNewTopic(boolean useTopicIds) {
        var topicOneIdOrZero = useTopicIds ? topicOneId : Uuid.ZERO_UUID;
        var topicTwoIdOrZero = useTopicIds ? topicTwoId : Uuid.ZERO_UUID;

        var builder = TxnOffsetCommitResponse.newBuilder(useTopicIds);
        builder.addPartition(topicOneIdOrZero, topicOne, partitionOne, errorOne);

        var newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicTwoIdOrZero)
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        var expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()))),
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicTwoIdOrZero)
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        assertEquals(expected, builder.merge(newData).build().data());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderMergeAppendsToExistingTopic(boolean useTopicIds) {
        var topicOneIdOrZero = useTopicIds ? topicOneId : Uuid.ZERO_UUID;

        var builder = TxnOffsetCommitResponse.newBuilder(useTopicIds);
        builder.addPartition(topicOneIdOrZero, topicOne, partitionOne, errorOne);

        var newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        var expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneIdOrZero)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()),
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        assertEquals(expected, builder.merge(newData).build().data());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBuilderRejectsNullKey(boolean useTopicIds) {
        var builder = TxnOffsetCommitResponse.newBuilder(useTopicIds);
        assertThrows(IllegalArgumentException.class,
            () -> builder.addPartition(useTopicIds ? null : Uuid.ZERO_UUID, useTopicIds ? topicOne : null, partitionOne, errorOne));
    }
}
