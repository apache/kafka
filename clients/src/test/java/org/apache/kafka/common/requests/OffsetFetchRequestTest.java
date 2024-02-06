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
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchRequest.Builder;
import org.apache.kafka.common.requests.OffsetFetchRequest.NoBatchedOffsetFetchRequestException;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OffsetFetchRequestTest {

    private final String topicOne = "topic1";
    private final int partitionOne = 1;
    private final String topicTwo = "topic2";
    private final int partitionTwo = 2;
    private final String topicThree = "topic3";
    private final String group1 = "group1";
    private final String group2 = "group2";
    private final String group3 = "group3";
    private final String group4 = "group4";
    private final String group5 = "group5";
    private List<String> groups = Arrays.asList(group1, group2, group3, group4, group5);

    private final List<Integer> listOfVersionsNonBatchOffsetFetch = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7);


    private OffsetFetchRequest.Builder builder;

    @Test
    public void testConstructor() {
        List<TopicPartition> partitions = Arrays.asList(
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

        for (short version : ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                builder = new OffsetFetchRequest.Builder(
                    group1,
                    false,
                    partitions,
                    false);
                OffsetFetchRequest request = builder.build(version);
                assertFalse(request.isAllPartitions());
                assertEquals(group1, request.groupId());
                assertEquals(partitions, request.partitions());

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
                builder = new Builder(Collections.singletonMap(group1, partitions), false, false);
                OffsetFetchRequest request = builder.build(version);
                Map<String, List<TopicPartition>> groupToPartitionMap =
                    request.groupIdsToPartitions();
                Map<String, List<OffsetFetchRequestTopics>> groupToTopicMap =
                    request.groupIdsToTopics();
                assertFalse(request.isAllPartitionsForGroup(group1));
                assertTrue(groupToPartitionMap.containsKey(group1) && groupToTopicMap.containsKey(
                    group1));
                assertEquals(partitions, groupToPartitionMap.get(group1));
                OffsetFetchResponse response = request.getErrorResponse(throttleTimeMs, Errors.NONE);
                assertEquals(Errors.NONE, response.groupLevelError(group1));
                assertFalse(response.groupHasError(group1));
                assertEquals(Collections.singletonMap(Errors.NONE, 1), response.errorCounts(),
                    "Incorrect error count for version " + version);
                assertEquals(throttleTimeMs, response.throttleTimeMs());
            }
        }
    }

    @Test
    public void testConstructorWithMultipleGroups() {
        List<TopicPartition> topic1Partitions = Arrays.asList(
            new TopicPartition(topicOne, partitionOne),
            new TopicPartition(topicOne, partitionTwo));
        List<TopicPartition> topic2Partitions = Arrays.asList(
            new TopicPartition(topicTwo, partitionOne),
            new TopicPartition(topicTwo, partitionTwo));
        List<TopicPartition> topic3Partitions = Arrays.asList(
            new TopicPartition(topicThree, partitionOne),
            new TopicPartition(topicThree, partitionTwo));
        Map<String, List<TopicPartition>> groupToTp = new HashMap<>();
        groupToTp.put(group1, topic1Partitions);
        groupToTp.put(group2, topic2Partitions);
        groupToTp.put(group3, topic3Partitions);
        groupToTp.put(group4, null);
        groupToTp.put(group5, null);
        int throttleTimeMs = 10;

        for (short version : ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version >= 8) {
                builder = new Builder(groupToTp, false, false);
                OffsetFetchRequest request = builder.build(version);
                Map<String, List<TopicPartition>> groupToPartitionMap =
                    request.groupIdsToPartitions();
                Map<String, List<OffsetFetchRequestTopics>> groupToTopicMap =
                    request.groupIdsToTopics();
                assertEquals(groupToTp.keySet(), groupToTopicMap.keySet());
                assertEquals(groupToTp.keySet(), groupToPartitionMap.keySet());
                assertFalse(request.isAllPartitionsForGroup(group1));
                assertFalse(request.isAllPartitionsForGroup(group2));
                assertFalse(request.isAllPartitionsForGroup(group3));
                assertTrue(request.isAllPartitionsForGroup(group4));
                assertTrue(request.isAllPartitionsForGroup(group5));
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
    }

    @Test
    public void testBuildThrowForUnsupportedBatchRequest() {
        for (int version : listOfVersionsNonBatchOffsetFetch) {
            Map<String, List<TopicPartition>> groupPartitionMap = new HashMap<>();
            groupPartitionMap.put(group1, null);
            groupPartitionMap.put(group2, null);
            builder = new Builder(groupPartitionMap, true, false);
            final short finalVersion = (short) version;
            assertThrows(NoBatchedOffsetFetchRequestException.class, () -> builder.build(finalVersion));
        }
    }

    @Test
    public void testConstructorFailForUnsupportedRequireStable() {
        for (short version : ApiKeys.OFFSET_FETCH.allVersions()) {
            if (version < 8) {
                // The builder needs to be initialized every cycle as the internal data `requireStable` flag is flipped.
                builder = new OffsetFetchRequest.Builder(group1, true, null, false);
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
                builder = new Builder(Collections.singletonMap(group1, null), true, false);
                OffsetFetchRequest request = builder.build(version);
                Map<String, List<TopicPartition>> groupToPartitionMap =
                    request.groupIdsToPartitions();
                Map<String, List<OffsetFetchRequestTopics>> groupToTopicMap =
                    request.groupIdsToTopics();
                assertTrue(groupToPartitionMap.containsKey(group1) && groupToTopicMap.containsKey(
                    group1));
                assertNull(groupToPartitionMap.get(group1));
                assertTrue(request.isAllPartitionsForGroup(group1));
                assertTrue(request.requireStable());
            }
        }
    }

    @Test
    public void testBuildThrowForUnsupportedRequireStable() {
        for (int version : listOfVersionsNonBatchOffsetFetch) {
            builder = new OffsetFetchRequest.Builder(group1, true, null, true);
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
