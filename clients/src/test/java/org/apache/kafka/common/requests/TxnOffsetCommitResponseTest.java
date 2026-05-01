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

import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TxnOffsetCommitResponseTest extends OffsetCommitResponseTest {

    @Test
    @Override
    public void testConstructorWithErrorResponse() {
        TxnOffsetCommitResponse response = new TxnOffsetCommitResponse(throttleTimeMs, errorsMap);

        assertEquals(errorsMap, response.errors());
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

    @Test
    public void testBuilderAddPartition() {
        TxnOffsetCommitResponse.Builder builder = TxnOffsetCommitResponse.newBuilder();
        builder.addPartition(topicOne, partitionOne, errorOne);
        builder.addPartition(topicOne, partitionTwo, errorTwo);
        builder.addPartition(topicTwo, partitionOne, errorOne);

        TxnOffsetCommitResponseData expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()),
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code()))),
                new TxnOffsetCommitResponseTopic()
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code())))));

        assertEquals(expected, builder.build().data());
    }

    @Test
    public void testBuilderAddPartitions() {
        TxnOffsetCommitResponse.Builder builder = TxnOffsetCommitResponse.newBuilder();
        builder.addPartitions(topicOne, List.of(partitionOne, partitionTwo), p -> p, errorOne);

        TxnOffsetCommitResponseData expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
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

    @Test
    public void testBuilderMergeIntoEmpty() {
        TxnOffsetCommitResponseData newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code())))));

        TxnOffsetCommitResponse response = TxnOffsetCommitResponse.newBuilder()
            .merge(newData)
            .build();

        assertEquals(newData, response.data());
    }

    @Test
    public void testBuilderMergeAddsNewTopic() {
        TxnOffsetCommitResponse.Builder builder = TxnOffsetCommitResponse.newBuilder();
        builder.addPartition(topicOne, partitionOne, errorOne);

        TxnOffsetCommitResponseData newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        TxnOffsetCommitResponseData expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(errorOne.code()))),
                new TxnOffsetCommitResponseTopic()
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        assertEquals(expected, builder.merge(newData).build().data());
    }

    @Test
    public void testBuilderMergeAppendsToExistingTopic() {
        TxnOffsetCommitResponse.Builder builder = TxnOffsetCommitResponse.newBuilder();
        builder.addPartition(topicOne, partitionOne, errorOne);

        TxnOffsetCommitResponseData newData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(errorTwo.code())))));

        TxnOffsetCommitResponseData expected = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseTopic()
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

    @Test
    public void testTopicNameBuilderRejectsNullTopicName() {
        TxnOffsetCommitResponse.Builder builder = TxnOffsetCommitResponse.newBuilder();
        assertThrows(IllegalArgumentException.class,
            () -> builder.addPartition(null, partitionOne, errorOne));
    }

}
