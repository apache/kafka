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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.params.ParameterizedTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetFetchRequestTest {
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testWithMultipleGroups(short version) {
        var data = new OffsetFetchRequestData()
            .setGroups(List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp1")
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName("foo")
                            .setTopicId(Uuid.randomUuid())
                            .setPartitionIndexes(List.of(0, 1, 2))
                    )),
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp2")
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName("bar")
                            .setTopicId(Uuid.randomUuid())
                            .setPartitionIndexes(List.of(0, 1, 2))
                    ))
            ));
        var builder = OffsetFetchRequest.Builder.forTopicIdsOrNames(data, false);

        if (version < 8) {
            assertThrows(OffsetFetchRequest.NoBatchedOffsetFetchRequestException.class, () -> builder.build(version));
        } else {
            assertEquals(data, builder.build(version).data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testThrowOnFetchStableOffsetsUnsupported(short version) {
        var builder = OffsetFetchRequest.Builder.forTopicIdsOrNames(
            new OffsetFetchRequestData()
                .setRequireStable(true)
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setTopicId(Uuid.randomUuid())
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                )),
            true
        );

        if (version < 7) {
            assertThrows(UnsupportedVersionException.class, () -> builder.build(version));
        } else {
            builder.build(version);
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testSingleGroup(short version) {
        var data = new OffsetFetchRequestData()
            .setGroups(List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp1")
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName("foo")
                            .setTopicId(Uuid.randomUuid())
                            .setPartitionIndexes(List.of(0, 1, 2))
                    ))
            ));
        var builder = OffsetFetchRequest.Builder.forTopicIdsOrNames(data, false);

        if (version < 8) {
            var expectedRequest = new OffsetFetchRequestData()
                .setGroupId("grp1")
                .setTopics(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestTopic()
                        .setName("foo")
                        .setPartitionIndexes(List.of(0, 1, 2))
                ));
            assertEquals(expectedRequest, builder.build(version).data());
        } else {
            assertEquals(data, builder.build(version).data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testSingleGroupWithAllTopics(short version) {
        var data = new OffsetFetchRequestData()
            .setGroups(List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp1")
                    .setTopics(null)
            ));
        var builder = OffsetFetchRequest.Builder.forTopicIdsOrNames(data, false);

        if (version < 2) {
            assertThrows(UnsupportedVersionException.class, () -> builder.build(version));
        } else if (version < 8) {
            var expectedRequest = new OffsetFetchRequestData()
                .setGroupId("grp1")
                .setTopics(null);
            assertEquals(expectedRequest, builder.build(version).data());
        } else {
            assertEquals(data, builder.build(version).data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testGetErrorResponse(short version) {
        var request = OffsetFetchRequest.Builder.forTopicIdsOrNames(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setTopicId(Uuid.randomUuid())
                                .setPartitionIndexes(List.of(0, 1))
                        ))
                )),
            false
        ).build(version);

        if (version < 2) {
            var expectedResponse = new OffsetFetchResponseData()
                .setThrottleTimeMs(1000)
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopic()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.INVALID_GROUP_ID.code())
                                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                                .setMetadata(OffsetFetchResponse.NO_METADATA)
                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH),
                            new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                .setPartitionIndex(1)
                                .setErrorCode(Errors.INVALID_GROUP_ID.code())
                                .setCommittedOffset(OffsetFetchResponse.INVALID_OFFSET)
                                .setMetadata(OffsetFetchResponse.NO_METADATA)
                                .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                        ))
                ));
            assertEquals(expectedResponse, request.getErrorResponse(1000, Errors.INVALID_GROUP_ID.exception()).data());
        } else if (version < 8) {
            var expectedResponse = new OffsetFetchResponseData()
                .setThrottleTimeMs(1000)
                .setErrorCode(Errors.INVALID_GROUP_ID.code());
            assertEquals(expectedResponse, request.getErrorResponse(1000, Errors.INVALID_GROUP_ID.exception()).data());
        } else {
            var expectedResponse = new OffsetFetchResponseData()
                .setThrottleTimeMs(1000)
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId("grp1")
                        .setErrorCode(Errors.INVALID_GROUP_ID.code())
                ));
            assertEquals(expectedResponse, request.getErrorResponse(1000, Errors.INVALID_GROUP_ID.exception()).data());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testGroups(short version) {
        var request = OffsetFetchRequest.Builder.forTopicIdsOrNames(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(List.of(
                            new OffsetFetchRequestData.OffsetFetchRequestTopics()
                                .setName("foo")
                                .setTopicId(Uuid.randomUuid())
                                .setPartitionIndexes(List.of(0, 1, 2))
                        ))
                )),
            false
        ).build(version);

        if (version < 8) {
            var expectedGroups = List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp1")
                    .setTopics(List.of(
                        new OffsetFetchRequestData.OffsetFetchRequestTopics()
                            .setName("foo")
                            .setPartitionIndexes(List.of(0, 1, 2))
                    ))
            );
            assertEquals(expectedGroups, request.groups());
        } else {
            assertEquals(request.data().groups(), request.groups());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 2)
    public void testGroupsWithAllTopics(short version) {
        var request = OffsetFetchRequest.Builder.forTopicIdsOrNames(
            new OffsetFetchRequestData()
                .setGroups(List.of(
                    new OffsetFetchRequestData.OffsetFetchRequestGroup()
                        .setGroupId("grp1")
                        .setTopics(null)
                )),
            false
        ).build(version);

        if (version < 8) {
            var expectedGroups = List.of(
                new OffsetFetchRequestData.OffsetFetchRequestGroup()
                    .setGroupId("grp1")
                    .setTopics(null)
            );
            assertEquals(expectedGroups, request.groups());
        } else {
            assertEquals(request.data().groups(), request.groups());
        }
    }
}
