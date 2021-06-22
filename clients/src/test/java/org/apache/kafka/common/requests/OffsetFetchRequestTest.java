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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.junit.jupiter.api.BeforeEach;
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
    private final String groupId = "groupId";

    private OffsetFetchRequest.Builder builder;
    private List<TopicPartition> partitions;

    @BeforeEach
    public void setUp() {
        partitions = Arrays.asList(new TopicPartition(topicOne, partitionOne),
                                   new TopicPartition(topicTwo, partitionTwo));
        builder = new OffsetFetchRequest.Builder(
            groupId,
            false,
            partitions,
            false);
    }

    @Test
    public void testConstructor() {
        assertFalse(builder.isAllTopicPartitions());
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
            OffsetFetchRequest request = builder.build(version);
            assertFalse(request.isAllPartitions());
            assertEquals(groupId, request.groupId());
            assertEquals(partitions, request.partitions());

            OffsetFetchResponse response = request.getErrorResponse(throttleTimeMs, Errors.NONE);
            assertEquals(Errors.NONE, response.error());
            assertFalse(response.hasError());
            assertEquals(Collections.singletonMap(Errors.NONE, version <= (short) 1 ? 3 : 1), response.errorCounts(),
                "Incorrect error count for version " + version);

            if (version <= 1) {
                assertEquals(expectedData, response.responseData());
            }

            if (version >= 3) {
                assertEquals(throttleTimeMs, response.throttleTimeMs());
            } else {
                assertEquals(DEFAULT_THROTTLE_TIME, response.throttleTimeMs());
            }
        }
    }

    @Test
    public void testConstructorFailForUnsupportedRequireStable() {
        for (short version : ApiKeys.OFFSET_FETCH.allVersions()) {
            // The builder needs to be initialized every cycle as the internal data `requireStable` flag is flipped.
            builder = new OffsetFetchRequest.Builder(groupId, true, null, false);
            final short finalVersion = version;
            if (version < 2) {
                assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
            } else {
                OffsetFetchRequest request = builder.build(finalVersion);
                assertEquals(groupId, request.groupId());
                assertNull(request.partitions());
                assertTrue(request.isAllPartitions());
                if (version < 7) {
                    assertFalse(request.requireStable());
                } else {
                    assertTrue(request.requireStable());
                }
            }
        }
    }

    @Test
    public void testBuildThrowForUnsupportedRequireStable() {
        for (short version : ApiKeys.OFFSET_FETCH.allVersions()) {
            builder = new OffsetFetchRequest.Builder(groupId, true, null, true);
            if (version < 7) {
                final short finalVersion = version;
                assertThrows(UnsupportedVersionException.class, () -> builder.build(finalVersion));
            } else {
                OffsetFetchRequest request = builder.build(version);
                assertTrue(request.requireStable());
            }
        }
    }
}
