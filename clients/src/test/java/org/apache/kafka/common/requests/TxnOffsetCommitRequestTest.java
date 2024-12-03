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
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.TxnOffsetCommitRequest.getErrorResponse;
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
        builder = new TxnOffsetCommitRequest.Builder(
            transactionalId,
            groupId,
            producerId,
            producerEpoch,
            OFFSETS
        );

        int generationId = 5;
        builderWithGroupMetadata = new TxnOffsetCommitRequest.Builder(
            transactionalId,
            groupId,
            producerId,
            producerEpoch,
            OFFSETS,
            memberId,
            generationId,
            Optional.of(groupInstanceId)
        );
    }

    @Test
    @Override
    public void testConstructor() {

        Map<TopicPartition, Errors> errorsMap = new HashMap<>();
        errorsMap.put(new TopicPartition(topicOne, partitionOne), Errors.NOT_COORDINATOR);
        errorsMap.put(new TopicPartition(topicTwo, partitionTwo), Errors.NOT_COORDINATOR);

        List<TxnOffsetCommitRequestTopic> expectedTopics = Arrays.asList(
            new TxnOffsetCommitRequestTopic()
                .setName(topicOne)
                .setPartitions(Collections.singletonList(
                    new TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(partitionOne)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata)
                )),
            new TxnOffsetCommitRequestTopic()
                .setName(topicTwo)
                .setPartitions(Collections.singletonList(
                    new TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(partitionTwo)
                        .setCommittedOffset(offset)
                        .setCommittedLeaderEpoch(leaderEpoch)
                        .setCommittedMetadata(metadata)
                ))
        );

        for (short version : ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            final TxnOffsetCommitRequest request;
            if (version < 3) {
                request = builder.build(version);
            } else {
                request = builderWithGroupMetadata.build(version);
            }
            assertEquals(OFFSETS, request.offsets());
            assertEquals(expectedTopics, TxnOffsetCommitRequest.getTopics(request.offsets()));

            TxnOffsetCommitResponse response =
                request.getErrorResponse(throttleTimeMs, Errors.NOT_COORDINATOR.exception());

            assertEquals(errorsMap, response.errors());
            assertEquals(Collections.singletonMap(Errors.NOT_COORDINATOR, 2), response.errorCounts());
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }

    @Test
    @Override
    public void testGetErrorResponse() {
        TxnOffsetCommitResponseData expectedResponse = new TxnOffsetCommitResponseData()
            .setTopics(Arrays.asList(
                new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(topicOne)
                    .setPartitions(Collections.singletonList(
                        new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                            .setPartitionIndex(partitionOne))),
                new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
                    .setName(topicTwo)
                    .setPartitions(Collections.singletonList(
                        new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                            .setPartitionIndex(partitionTwo)))));

        assertEquals(expectedResponse, getErrorResponse(builderWithGroupMetadata.data, Errors.UNKNOWN_MEMBER_ID));
    }

    @Test
    public void testVersionSupportForGroupMetadata() {
        for (short version : ApiKeys.TXN_OFFSET_COMMIT.allVersions()) {
            assertDoesNotThrow(() -> builder.build(version));
            if (version >= 3) {
                assertDoesNotThrow(() -> builderWithGroupMetadata.build(version));
            } else {
                assertEquals("Broker doesn't support group metadata commit API on version " + version +
                    ", minimum supported request version is 3 which requires brokers to be on version 2.5 or above.",
                    assertThrows(UnsupportedVersionException.class, () -> builderWithGroupMetadata.build(version)).getMessage());
            }
        }
    }
}
