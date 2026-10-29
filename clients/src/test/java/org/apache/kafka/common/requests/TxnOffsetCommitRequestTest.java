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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TxnOffsetCommitRequestTest extends OffsetCommitRequestTest {

    private static final Map<TopicPartition, CommittedOffset> OFFSETS = new HashMap<>();
    private static TxnOffsetCommitRequest.Builder builder;
    private static TxnOffsetCommitRequest.Builder builderWithGroupMetadata;

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        OFFSETS.clear();
        OFFSETS.put(new TopicPartition(topicOne, partitionOne),
            new CommittedOffset(
                offset,
                metadata,
                Optional.of((int) leaderEpoch)));
        OFFSETS.put(new TopicPartition(topicTwo, partitionTwo),
            new CommittedOffset(
                offset,
                metadata,
                Optional.of((int) leaderEpoch)));

        String transactionalId = "transactionalId";
        int producerId = 10;
        short producerEpoch = 1;
        TxnOffsetCommitRequestData data = new TxnOffsetCommitRequestData()
            .setTransactionalId(transactionalId)
            .setGroupId(groupId)
            .setProducerId(producerId)
            .setProducerEpoch(producerEpoch)
            .setTopics(TxnOffsetCommitRequest.getTopics(OFFSETS));
        builder = TxnOffsetCommitRequest.Builder.forTopicNames(data, true);

        int generationId = 5;
        TxnOffsetCommitRequestData dataWithGroupMetadata = new TxnOffsetCommitRequestData()
            .setTransactionalId(transactionalId)
            .setGroupId(groupId)
            .setProducerId(producerId)
            .setProducerEpoch(producerEpoch)
            .setMemberId(memberId)
            .setGenerationIdOrMemberEpoch(generationId)
            .setGroupInstanceId(groupInstanceId)
            .setTopics(TxnOffsetCommitRequest.getTopics(OFFSETS));
        builderWithGroupMetadata = TxnOffsetCommitRequest.Builder.forTopicNames(dataWithGroupMetadata, true);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT, toVersion = 5)
    public void testConstructor(short version) {
        List<TxnOffsetCommitRequestTopic> expectedTopics = List.of(
            new TxnOffsetCommitRequestTopic()
                .setName(topicOne)
                .setPartitions(List.of(
                    new TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(partitionOne)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata))),
            new TxnOffsetCommitRequestTopic()
                .setName(topicTwo)
                .setPartitions(List.of(
                    new TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(partitionTwo)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata))));

        final TxnOffsetCommitRequest request;
        if (version < 3) {
            request = builder.build(version);
        } else {
            request = builderWithGroupMetadata.build(version);
        }
        assertEquals(OFFSETS, request.offsets());
        assertEquals(expectedTopics, TxnOffsetCommitRequest.getTopics(request.offsets()));

        var response = request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

        assertEquals(Map.of(Errors.NOT_COORDINATOR, 2), response.errorCounts());
        assertEquals(throttleTimeMs, response.throttleTimeMs());
    }

    @Test
    @Override
    public void testGetErrorResponse() {
        var topicOneId = Uuid.randomUuid();
        var topicTwoId = Uuid.randomUuid();

        var data = new TxnOffsetCommitRequestData()
            .setTransactionalId("transactionalId")
            .setGroupId(groupId)
            .setProducerId(10L)
            .setProducerEpoch((short) 1)
            .setTopics(List.of(
                new TxnOffsetCommitRequestTopic()
                    .setTopicId(topicOneId)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(partitionOne)
                            .setCommittedOffset(offset))),
                new TxnOffsetCommitRequestTopic()
                    .setTopicId(topicTwoId)
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(partitionTwo)
                            .setCommittedOffset(offset)))));

        var expectedResponseData = new TxnOffsetCommitResponseData()
            .setTopics(List.of(
                new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setTopicId(topicOneId)
                    .setName(topicOne)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionOne)
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()))),
                new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setTopicId(topicTwoId)
                    .setName(topicTwo)
                    .setPartitions(List.of(
                        new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                            .setPartitionIndex(partitionTwo)
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())))));

        assertEquals(expectedResponseData, TxnOffsetCommitRequest.getErrorResponse(data, Errors.UNKNOWN_MEMBER_ID));

        var request = TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(data, true).build();
        var response = request.getErrorResponse(throttleTimeMs, Errors.UNKNOWN_MEMBER_ID.exception());
        assertEquals(expectedResponseData.setThrottleTimeMs(throttleTimeMs), response.data());
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT, toVersion = 5)
    public void testVersionSupportForGroupMetadata(short version) {
        assertDoesNotThrow(() -> builder.build(version));
        if (version >= 3) {
            assertDoesNotThrow(() -> builderWithGroupMetadata.build(version));
        } else {
            assertEquals("Broker doesn't support group metadata commit API on version " + version +
                ", minimum supported request version is 3 which requires brokers to be on version 2.5 or above.",
                assertThrows(UnsupportedVersionException.class, () -> builderWithGroupMetadata.build(version)).getMessage());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
    public void testForTopicIdsOrNamesWithTopicNameOnly(short version) {
        var data = new TxnOffsetCommitRequestData()
            .setTransactionalId("tx")
            .setGroupId(groupId)
            .setProducerId(1)
            .setProducerEpoch((short) 0)
            .setTopics(List.of(
                new TxnOffsetCommitRequestTopic()
                    .setName("foo")
                    .setPartitions(List.of(
                        new TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(0L)))));

        if (version >= 6) {
            assertThrows(UnsupportedVersionException.class,
                () -> TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(data, true).build(version));
        } else {
            assertDoesNotThrow(
                () -> TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(data, true).build(version));
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
    public void testForTopicIdsOrNamesWithTopicIdOnly(short version) {
        var topicId = Uuid.randomUuid();
        var data = new TxnOffsetCommitRequestData()
            .setTransactionalId("tx")
            .setGroupId(groupId)
            .setProducerId(1)
            .setProducerEpoch((short) 0)
            .setTopics(List.of(
                new TxnOffsetCommitRequestTopic()
                    .setTopicId(topicId)
                    .setPartitions(List.of(
                        new TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(0L)))));

        if (version >= 6) {
            var request = TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(data, true).build(version);
            assertEquals(data, request.data());
        } else {
            assertThrows(UnsupportedVersionException.class,
                () -> TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(data, true).build(version));
        }
    }

    @Test
    public void testForTopicNamesCapsAtTransactionV1WhenTransactionV2IsDisabled() {
        var builder = TxnOffsetCommitRequest.Builder.forTopicNames(
            new TxnOffsetCommitRequestData(),
            false
        );
        assertEquals(TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2, builder.latestAllowedVersion());
    }

    @Test
    public void testForTopicNamesCapsAtV5WhenTransactionV2IsEnabled() {
        var builder = TxnOffsetCommitRequest.Builder.forTopicNames(
            new TxnOffsetCommitRequestData(),
            true
        );
        assertEquals((short) 5, builder.latestAllowedVersion());
    }

    @Test
    public void testForTopicIdsOrNamesCapsAtTransactionV1WhenTransactionV2IsDisabled() {
        var builder = TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(
            new TxnOffsetCommitRequestData(),
            false
        );
        assertEquals(TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2, builder.latestAllowedVersion());
    }

    @Test
    public void testForTopicIdsOrNamesUsesLatestVersionWhenTransactionV2IsEnabled() {
        var builder = TxnOffsetCommitRequest.Builder.forTopicIdsOrNames(
            new TxnOffsetCommitRequestData(),
            true
        );
        assertEquals(ApiKeys.TXN_OFFSET_COMMIT.latestVersion(), builder.latestAllowedVersion());
    }
}
