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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchRequest.Builder;
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.apache.kafka.test.MoreAssertions.assertRequestEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetFetchRequestTest {

    private final String topicOne = "topic1";
    private final Uuid topicOneId = Uuid.randomUuid();
    private final int partitionOne = 1;
    private final String topicTwo = "topic2";
    private final Uuid topicTwoId = Uuid.randomUuid();
    private final int partitionTwo = 2;
    private final String topicThree = "topic3";
    private final Uuid topicThreeId = Uuid.randomUuid();
    private final String group1 = "group1";
    private final String group2 = "group2";
    private final String group3 = "group3";
    private final String group4 = "group4";
    private final String group5 = "group5";
    private List<String> groups = asList(group1, group2, group3, group4, group5);

    private final List<Integer> listOfVersionsNonBatchOffsetFetch = asList(0, 1, 2, 3, 4, 5, 6, 7);

    private final TopicResolver topicResolver = new TopicResolver.Builder()
        .add(topicOne, topicOneId)
        .add(topicTwo, topicTwoId)
        .add(topicThree, topicThreeId)
        .build();

    private OffsetFetchRequest.Builder builder;

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testConstructor(short version) {
        List<TopicPartition> partitions = asList(
            new TopicPartition(topicOne, partitionOne),
            new TopicPartition(topicTwo, partitionTwo));
        int throttleTimeMs = 10;

        Map<TopicPartition, PartitionData> expectedData = new HashMap<>();
        for (TopicPartition partition : partitions) {
            expectedData.put(partition, new PartitionData(
                OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(),
                OffsetFetchResponse.NO_METADATA,
                Errors.NONE
            ));
        }

        if (version < 8) {
            builder = new OffsetFetchRequest.Builder(
                group1,
                false,
                partitions,
                false,
                TopicResolver.emptyResolver());
            OffsetFetchRequest request = builder.build(version);

            OffsetFetchRequestData data = new OffsetFetchRequestData()
                .setGroupId(group1)
                .setTopics(asList(
                    new OffsetFetchRequestTopic()
                        .setName(topicOne)
                        .setPartitionIndexes(asList(partitionOne)),
                    new OffsetFetchRequestTopic()
                        .setName(topicTwo)
                        .setPartitionIndexes(asList(partitionTwo))));

            assertRequestEquals(new OffsetFetchRequest(data, version), request);
            assertFalse(builder.isAllTopicPartitions());
            assertFalse(request.isAllPartitions());

            OffsetFetchResponse response = request.getErrorResponse(throttleTimeMs, Errors.NONE);
            assertEquals(Errors.NONE, response.error());
            assertFalse(response.hasError());
            assertEquals(Collections.singletonMap(Errors.NONE, version <= (short) 1 ? 3 : 1), response.errorCounts(),
                "Incorrect error count for version " + version);

            if (version <= 1) {
                assertEquals(expectedData, response.responseDataV0ToV7());
            }

            if (version >= 3) {
                assertEquals(throttleTimeMs, response.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
            }
        } else {
            builder = new Builder(
                Collections.singletonMap(group1, partitions),
                false,
                false,
                topicResolver);
            OffsetFetchRequest request = builder.build(version);

            OffsetFetchRequestData data = new OffsetFetchRequestData()
                .setGroups(asList(
                    new OffsetFetchRequestGroup()
                        .setGroupId(group1)
                        .setTopics(asList(
                            new OffsetFetchRequestTopics()
                                .setName(topicOne)
                                .setTopicId(topicOneId)
                                .setPartitionIndexes(asList(partitionOne)),
                            new OffsetFetchRequestTopics()
                                .setName(topicTwo)
                                .setTopicId(topicTwoId)
                                .setPartitionIndexes(asList(partitionTwo))
                        ))
                ));

            assertRequestEquals(new OffsetFetchRequest(data, version), request);

            OffsetFetchResponse response = request.getErrorResponse(throttleTimeMs, Errors.NONE);
            assertEquals(Errors.NONE, response.groupLevelError(group1));
            assertFalse(response.groupHasError(group1));
            assertEquals(Collections.singletonMap(Errors.NONE, 1), response.errorCounts(),
                "Incorrect error count for version " + version);
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testConstructorWithMultipleGroups(short version) {
        List<TopicPartition> topic1Partitions = asList(
            new TopicPartition(topicOne, partitionOne),
            new TopicPartition(topicOne, partitionTwo));
        List<TopicPartition> topic2Partitions = asList(
            new TopicPartition(topicTwo, partitionOne),
            new TopicPartition(topicTwo, partitionTwo));
        List<TopicPartition> topic3Partitions = asList(
            new TopicPartition(topicThree, partitionOne),
            new TopicPartition(topicThree, partitionTwo));
        Map<String, List<TopicPartition>> groupToTp = new HashMap<>();
        groupToTp.put(group1, topic1Partitions);
        groupToTp.put(group2, topic2Partitions);
        groupToTp.put(group3, topic3Partitions);
        groupToTp.put(group4, null);
        groupToTp.put(group5, null);
        int throttleTimeMs = 10;

        if (version >= 8) {
            builder = new Builder(groupToTp, false, false, topicResolver);
            OffsetFetchRequest request = builder.build(version);

            OffsetFetchRequestData data = new OffsetFetchRequestData()
                .setGroups(asList(
                    new OffsetFetchRequestGroup()
                        .setGroupId(group1)
                        .setTopics(asList(
                            new OffsetFetchRequestTopics()
                                .setName(topicOne)
                                .setTopicId(topicOneId)
                                .setPartitionIndexes(asList(partitionOne, partitionTwo))
                        )),
                    new OffsetFetchRequestGroup()
                        .setGroupId(group2)
                        .setTopics(asList(
                            new OffsetFetchRequestTopics()
                                .setName(topicTwo)
                                .setTopicId(topicTwoId)
                                .setPartitionIndexes(asList(partitionOne, partitionTwo))
                        )),
                    new OffsetFetchRequestGroup()
                        .setGroupId(group3)
                        .setTopics(asList(
                            new OffsetFetchRequestTopics()
                                .setName(topicThree)
                                .setTopicId(topicThreeId)
                                .setPartitionIndexes(asList(partitionOne, partitionTwo))
                        )),
                    new OffsetFetchRequestGroup().setGroupId(group4).setTopics(null),
                    new OffsetFetchRequestGroup().setGroupId(group5).setTopics(null)
                ));

            assertRequestEquals(new OffsetFetchRequest(data, version), request);

            OffsetFetchResponse response = request.getErrorResponse(throttleTimeMs, Errors.NONE);
            for (String group : groups) {
                assertEquals(Errors.NONE, response.groupLevelError(group));
                assertFalse(response.groupHasError(group));
            }
            assertEquals(Collections.singletonMap(Errors.NONE, 5), response.errorCounts(),
                "Incorrect error count for version " + version);
            assertEquals(throttleTimeMs, response.throttleTimeMs());
        }
    }

    @Test
    public void testBuildThrowForUnsupportedBatchRequest() {
        for (int version : listOfVersionsNonBatchOffsetFetch) {
            Map<String, List<TopicPartition>> groupPartitionMap = new HashMap<>();
            groupPartitionMap.put(group1, null);
            groupPartitionMap.put(group2, null);
            builder = new Builder(groupPartitionMap, true, false, topicResolver);
            final short finalVersion = (short) version;
            assertThrows(NoBatchedOffsetFetchRequestException.class, () -> builder.build(finalVersion));
        }
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
    public void testConstructorFailForUnsupportedRequireStable(short version) {
        if (version < 8) {
            // The builder needs to be initialized every cycle as the internal data `requireStable` flag is flipped.
            builder = new OffsetFetchRequest.Builder(
                group1,
                true,
                null,
                false,
                TopicResolver.emptyResolver());
            final short finalVersion = version;
            if (version < 2) {
                assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
            } else {
                OffsetFetchRequest request = builder.build(finalVersion);
                assertEquals(group1, request.groupId());
                assertNull(request.partitions());
                assertTrue(request.isAllPartitions());
                if (version < 7) {
                    assertFalse(request.requireStable());
                } else {
                    assertTrue(request.requireStable());
                }
            }
        } else {
            builder = new Builder(Collections.singletonMap(group1, null), true, false, topicResolver);
            OffsetFetchRequest request = builder.build(version);

            OffsetFetchRequestData data = new OffsetFetchRequestData()
                .setRequireStable(true)
                .setGroups(asList(
                    new OffsetFetchRequestGroup()
                        .setGroupId(group1)
                        .setTopics(null)));

            assertRequestEquals(new OffsetFetchRequest(data, version), request);
        }
    }

    @Test
    public void testBuildThrowForUnsupportedRequireStable() {
        for (int version : listOfVersionsNonBatchOffsetFetch) {
            builder = new OffsetFetchRequest.Builder(
                group1,
                true,
                null,
                true,
                TopicResolver.emptyResolver());
            if (version < 7) {
                final short finalVersion = (short) version;
                assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
            } else {
                OffsetFetchRequest request = builder.build((short) version);
                assertTrue(request.requireStable());
            }
        }
    }
}
