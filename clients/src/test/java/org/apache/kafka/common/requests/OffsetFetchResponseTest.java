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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;

import org.junit.jupiter.params.ParameterizedTest;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;
import static org.apache.kafka.common.requests.OffsetFetchResponse.INVALID_OFFSET;
import static org.apache.kafka.common.requests.OffsetFetchResponse.NO_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetFetchResponseTest {
    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testBuilderWithSingleGroup(short version) {
        var group = new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId("group")
            .setTopics(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                    .setName("foo")
                    .setPartitions(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                            .setPartitionIndex(0)
                            .setCommittedOffset(10)
                            .setCommittedLeaderEpoch(5)
                            .setMetadata("metadata")
                    ))
            ));

        if (version < 8) {
            assertEquals(
                new OffsetFetchResponseData()
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopic()
                            .setName("foo")
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                    .setPartitionIndex(0)
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(5)
                                    .setMetadata("metadata")
                            ))
                    )),
                new OffsetFetchResponse.Builder(group).build(version).data()
            );
        } else {
            assertEquals(
                new OffsetFetchResponseData()
                    .setGroups(List.of(group)),
                new OffsetFetchResponse.Builder(group).build(version).data()
            );
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testBuilderWithMultipleGroups(short version) {
        var groups = List.of(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group1")
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setCommittedLeaderEpoch(5)
                                .setMetadata("metadata")
                        ))
                )),
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group2")
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setCommittedLeaderEpoch(5)
                                .setMetadata("metadata")
                        ))
                ))
        );

        if (version < 8) {
            assertThrows(UnsupportedVersionException.class,
                () -> new OffsetFetchResponse.Builder(groups).build(version));
        } else {
            assertEquals(
                new OffsetFetchResponseData()
                    .setGroups(groups),
                new OffsetFetchResponse.Builder(groups).build(version).data()
            );
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testGroupWithSingleGroup(short version) {
        var data = new OffsetFetchResponseData();

        if (version < 8) {
            data.setTopics(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseTopic()
                    .setName("foo")
                    .setPartitions(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponsePartition()
                            .setPartitionIndex(0)
                            .setCommittedOffset(10)
                            .setCommittedLeaderEpoch(5)
                            .setMetadata("metadata")
                    ))
            ));
        } else {
            data.setGroups(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId("foo")
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName("foo")
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(0)
                                    .setCommittedOffset(10)
                                    .setCommittedLeaderEpoch(5)
                                    .setMetadata("metadata")
                            ))
                    ))
            ));
        }

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("foo")
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setCommittedLeaderEpoch(5)
                                .setMetadata("metadata")
                        ))
                )),
            new OffsetFetchResponse(data, version).group("foo")
        );
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH, fromVersion = 8)
    public void testGroupWithMultipleGroups(short version) {
        var groups = List.of(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group1")
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setCommittedLeaderEpoch(5)
                                .setMetadata("metadata")
                        ))
                )),
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("group2")
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName("bar")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(0)
                                .setCommittedOffset(10)
                                .setCommittedLeaderEpoch(5)
                                .setMetadata("metadata")
                        ))
                ))
        );

        var response = new OffsetFetchResponse(
            new OffsetFetchResponseData().setGroups(groups),
            version
        );

        groups.forEach(group ->
            assertEquals(group, response.group(group.groupId()))
        );
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testGroupWithSingleGroupWithTopLevelError(short version) {
        var data = new OffsetFetchResponseData();

        if (version < 2) {
            data.setTopics(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseTopic()
                    .setName("foo")
                    .setPartitions(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponsePartition()
                            .setPartitionIndex(0)
                            .setErrorCode(Errors.INVALID_GROUP_ID.code())
                    ))
            ));
        } else if (version < 8) {
            data.setErrorCode(Errors.INVALID_GROUP_ID.code());
        } else {
            data.setGroups(List.of(
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId("foo")
                    .setErrorCode(Errors.INVALID_GROUP_ID.code())
            ));
        }

        assertEquals(
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("foo")
                .setErrorCode(Errors.INVALID_GROUP_ID.code()),
            new OffsetFetchResponse(data, version).group("foo")
        );
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testSingleGroupWithError(short version) {
        var group = new OffsetFetchRequestData.OffsetFetchRequestGroup()
            .setGroupId("group1")
            .setTopics(List.of(
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName("foo")
                    .setPartitionIndexes(List.of(0))
            ));

        if (version < 2) {
            assertEquals(
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId("group1")
                    .setTopics(List.of(
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName("foo")
                            .setPartitions(List.of(
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.INVALID_GROUP_ID.code())
                                    .setCommittedOffset(INVALID_OFFSET)
                                    .setMetadata(NO_METADATA)
                                    .setCommittedLeaderEpoch(NO_PARTITION_LEADER_EPOCH)
                            ))
                    )),
                OffsetFetchResponse.groupError(group, Errors.INVALID_GROUP_ID, version)
            );
        } else {
            assertEquals(
                new OffsetFetchResponseData.OffsetFetchResponseGroup()
                    .setGroupId("group1")
                    .setErrorCode(Errors.INVALID_GROUP_ID.code()),
                OffsetFetchResponse.groupError(group, Errors.INVALID_GROUP_ID, version)
            );
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testErrorCounts(short version) {
        if (version < 2) {
            var data = new OffsetFetchResponseData()
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopic()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.UNSTABLE_OFFSET_COMMIT.code())
                                .setCommittedOffset(INVALID_OFFSET)
                                .setMetadata(NO_METADATA)
                                .setCommittedLeaderEpoch(NO_PARTITION_LEADER_EPOCH)
                        ))
                ));
            assertEquals(
                Map.of(Errors.UNSTABLE_OFFSET_COMMIT, 1),
                new OffsetFetchResponse(data, version).errorCounts()
            );
        } else if (version < 8) {
            // Version 2 returns a top level error code for group or coordinator level errors.
            var data = new OffsetFetchResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseTopic()
                        .setName("foo")
                        .setPartitions(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.UNSTABLE_OFFSET_COMMIT.code())
                                .setCommittedOffset(INVALID_OFFSET)
                                .setMetadata(NO_METADATA)
                                .setCommittedLeaderEpoch(NO_PARTITION_LEADER_EPOCH)
                        ))
                ));
            assertEquals(
                Map.of(
                    Errors.NONE, 1,
                    Errors.UNSTABLE_OFFSET_COMMIT, 1
                ),
                new OffsetFetchResponse(data, version).errorCounts()
            );
        } else {
            // Version 8 adds support for fetching offsets for multiple groups at a time.
            var data = new OffsetFetchResponseData()
                .setGroups(List.of(
                    new OffsetFetchResponseData.OffsetFetchResponseGroup()
                        .setGroupId("group1")
                        .setErrorCode(Errors.NONE.code())
                        .setTopics(List.of(
                            new OffsetFetchResponseData.OffsetFetchResponseTopics()
                                .setName("foo")
                                .setPartitions(List.of(
                                    new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                        .setPartitionIndex(0)
                                        .setErrorCode(Errors.UNSTABLE_OFFSET_COMMIT.code())
                                        .setCommittedOffset(INVALID_OFFSET)
                                        .setMetadata(NO_METADATA)
                                        .setCommittedLeaderEpoch(NO_PARTITION_LEADER_EPOCH)
                                ))
                        ))
                ));
            assertEquals(
                Map.of(
                    Errors.NONE, 1,
                    Errors.UNSTABLE_OFFSET_COMMIT, 1
                ),
                new OffsetFetchResponse(data, version).errorCounts()
            );
        }
    }
}
