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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;

import com.dynatrace.hash4j.hashing.Hashing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignmentWithEpochs;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignmentWithEpochs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class UtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(UtilsTest.class);
    private static final String GROUP_ID = "test-group";
    private static final Uuid FOO_TOPIC_ID = Uuid.randomUuid();
    private static final String FOO_TOPIC_NAME = "foo";
    private static final String BAR_TOPIC_NAME = "bar";
    private static final int FOO_NUM_PARTITIONS = 2;
    private static final CoordinatorMetadataImage FOO_METADATA_IMAGE = new MetadataImageBuilder()
            .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
            .addRacks()
            .buildCoordinatorMetadataImage();

    @Test
    void testNonExistingTopicName() {
        assertEquals(0, Utils.computeTopicHash("unknown", FOO_METADATA_IMAGE));
    }

    @Test
    void testComputeTopicHash() {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        long expected = Hashing.xxh3_64().hashStream()
            .putByte((byte) 0)
            .putLong(FOO_TOPIC_ID.getMostSignificantBits())
            .putLong(FOO_TOPIC_ID.getLeastSignificantBits())
            .putString(FOO_TOPIC_NAME)
            .putInt(FOO_NUM_PARTITIONS)
            .putInt(0) // partition 0
            .putInt(5) // length of rack0
            .putString("rack0") // The first rack in partition 0
            .putInt(5) // length of rack1
            .putString("rack1") // The second rack in partition 0
            .putInt(1) // partition 1
            .putInt(5) // length of rack0
            .putString("rack1") // The first rack in partition 1
            .putInt(5) // length of rack1
            .putString("rack2") // The second rack in partition 1
            .getAsLong();
        assertEquals(expected, result);
    }

    @Test
    void testComputeTopicHashWithDifferentMagicByte() {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        long expected = Hashing.xxh3_64().hashStream()
            .putByte((byte) 1) // different magic byte
            .putLong(FOO_TOPIC_ID.getMostSignificantBits())
            .putLong(FOO_TOPIC_ID.getLeastSignificantBits())
            .putString(FOO_TOPIC_NAME)
            .putInt(FOO_NUM_PARTITIONS)
            .putInt(0) // partition 0
            .putInt(5) // length of rack0
            .putString("rack0") // The first rack in partition 0
            .putInt(5) // length of rack1
            .putString("rack1") // The second rack in partition 0
            .putInt(1) // partition 1
            .putInt(5) // length of rack0
            .putString("rack1") // The first rack in partition 1
            .putInt(5) // length of rack1
            .putString("rack2") // The second rack in partition 1
            .getAsLong();
        assertNotEquals(expected, result);
    }

    @Test
    void testComputeTopicHashWithLeastSignificantBitsFirst() {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        long expected = Hashing.xxh3_64().hashStream()
            .putByte((byte) 0)
            .putLong(FOO_TOPIC_ID.getLeastSignificantBits()) // different order
            .putLong(FOO_TOPIC_ID.getMostSignificantBits())
            .putString(FOO_TOPIC_NAME)
            .putInt(FOO_NUM_PARTITIONS)
            .putInt(0) // partition 0
            .putInt(5) // length of rack0
            .putString("rack0") // The first rack in partition 0
            .putInt(5) // length of rack1
            .putString("rack1") // The second rack in partition 0
            .putInt(1) // partition 1
            .putInt(5) // length of rack0
            .putString("rack1") // The first rack in partition 1
            .putInt(5) // length of rack1
            .putString("rack2") // The second rack in partition 1
            .getAsLong();
        assertNotEquals(expected, result);
    }

    @Test
    void testComputeTopicHashWithDifferentPartitionOrder() {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        long expected = Hashing.xxh3_64().hashStream()
            .putByte((byte) 1)
            .putLong(FOO_TOPIC_ID.getMostSignificantBits())
            .putLong(FOO_TOPIC_ID.getLeastSignificantBits())
            .putString(FOO_TOPIC_NAME)
            .putInt(FOO_NUM_PARTITIONS)
            .putInt(1) // partition 1
            .putInt(5) // length of rack0
            .putString("rack1") // The first rack in partition 1
            .putInt(5) // length of rack1
            .putString("rack2") // The second rack in partition 1
            .putInt(0) // partition 0
            .putInt(5) // length of rack0
            .putString("rack0") // The first rack in partition 0
            .putInt(5) // length of rack1
            .putString("rack1") // The second rack in partition 0
            .getAsLong();
        assertNotEquals(expected, result);
    }

    @Test
    void testComputeTopicHashWithDifferentRackOrder() {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        long expected = Hashing.xxh3_64().hashStream()
            .putByte((byte) 0)
            .putLong(FOO_TOPIC_ID.getMostSignificantBits())
            .putLong(FOO_TOPIC_ID.getLeastSignificantBits())
            .putString(FOO_TOPIC_NAME)
            .putInt(FOO_NUM_PARTITIONS)
            .putInt(0) // partition 0
            .putInt(5) // length of rack0
            .putString("rack1") // The second rack in partition 0
            .putInt(5) // length of rack1
            .putString("rack0") // The first rack in partition 0
            .putInt(1) // partition 1
            .putInt(5) // length of rack0
            .putString("rack1") // The first rack in partition 1
            .putInt(5) // length of rack1
            .putString("rack2") // The second rack in partition 1
            .getAsLong();
        assertNotEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("differentFieldGenerator")
    void testComputeTopicHashWithDifferentField(CoordinatorMetadataImage differentImage) {
        long result = Utils.computeTopicHash(FOO_TOPIC_NAME, FOO_METADATA_IMAGE);

        assertNotEquals(
            Utils.computeTopicHash(FOO_TOPIC_NAME, differentImage),
            result
        );
    }

    private static Stream<Arguments> differentFieldGenerator() {
        return Stream.of(
            Arguments.of(
                new MetadataImageBuilder() // different topic id
                    .addTopic(Uuid.randomUuid(), FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
                    .addRacks()
                    .buildCoordinatorMetadataImage()
            ),
            Arguments.of(new MetadataImageBuilder() // different topic name
                    .addTopic(FOO_TOPIC_ID, "bar", FOO_NUM_PARTITIONS)
                    .addRacks()
                    .buildCoordinatorMetadataImage()
            ),
            Arguments.of(new MetadataImageBuilder() // different partitions
                    .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, 1)
                    .addRacks()
                    .buildCoordinatorMetadataImage()
            ),
            Arguments.of(new MetadataImageBuilder() // different racks
                    .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
                    .buildCoordinatorMetadataImage()
            )
        );
    }

    @Test
    void testComputeGroupHashWithEmptyMap() {
        assertEquals(0, Utils.computeGroupHash(Map.of()));
    }

    @Test
    void testComputeGroupHashWithDifferentOrder() {
        Map<String, Long> ascendTopicHashes = new LinkedHashMap<>();
        ascendTopicHashes.put(BAR_TOPIC_NAME, 123L);
        ascendTopicHashes.put(FOO_TOPIC_NAME, 456L);

        Map<String, Long> descendTopicHashes = new LinkedHashMap<>();
        descendTopicHashes.put(FOO_TOPIC_NAME, 456L);
        descendTopicHashes.put(BAR_TOPIC_NAME, 123L);
        assertEquals(Utils.computeGroupHash(ascendTopicHashes), Utils.computeGroupHash(descendTopicHashes));
    }

    @Test
    void testComputeGroupHashWithSameKeyButDifferentValue() {
        Map<String, Long> map1 = Map.of(
            BAR_TOPIC_NAME, 123L,
            FOO_TOPIC_NAME, 456L
        );

        Map<String, Long> map2 = Map.of(
            BAR_TOPIC_NAME, 456L,
            FOO_TOPIC_NAME, 123L
        );
        assertNotEquals(Utils.computeGroupHash(map1), Utils.computeGroupHash(map2));
    }

    @Test
    void testAssignmentFromTopicPartitionsWithNegativeDefaultEpoch() {
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitions = List.of(
            new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(FOO_TOPIC_ID)
                .setPartitions(Arrays.asList(0, 1, 2))
        );

        Map<Uuid, Map<Integer, Integer>> result = Utils.assignmentFromTopicPartitions(
            LOG,
            GROUP_ID,
            topicPartitions,
            LEAVE_GROUP_STATIC_MEMBER_EPOCH // -2
        );

        // Verify epoch is adjusted to 0
        assertEquals(
            mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 0, 0, 1, 2)
            ),
            result
        );
    }

    @Test
    void testAssignmentFromTopicPartitionsWithEpochsProvided() {
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitions = List.of(
            new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(FOO_TOPIC_ID)
                .setPartitions(Arrays.asList(0, 1, 2))
                .setAssignmentEpochs(Arrays.asList(5, 6, 7))
        );

        Map<Uuid, Map<Integer, Integer>> result = Utils.assignmentFromTopicPartitions(
            LOG,
            GROUP_ID,
            topicPartitions,
            LEAVE_GROUP_STATIC_MEMBER_EPOCH // -2
        );

        // Verify assignment epochs are used
        assertEquals(
            mkAssignmentWithEpochs(
                mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 5, 0),
                mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 6, 1),
                mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 7, 2)
            ),
            result
        );
    }

    @Test
    void testAssignmentFromTopicPartitionsWithUnequalEpochLength() {
        // Empty array epochs list
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> topicPartitions = List.of(
            new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                .setTopicId(FOO_TOPIC_ID)
                .setPartitions(Arrays.asList(0, 1, 2))
                .setAssignmentEpochs(List.of(0))
        );

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(UtilsTest.class)) {
            Map<Uuid, Map<Integer, Integer>> result = Utils.assignmentFromTopicPartitions(LOG, GROUP_ID, topicPartitions, 7);
            // Verify fallback to default epoch for empty epochs list
            assertEquals(
                mkAssignmentWithEpochs(
                    mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 7, 0),
                    mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 7, 1),
                    mkTopicAssignmentWithEpochs(FOO_TOPIC_ID, 7, 2)
                ),
                result
            );
            // Verify error log includes group id
            assertEquals(1, appender.getMessages("ERROR").stream()
                .filter(msg -> msg.contains("[GroupId " + GROUP_ID + "] Size of assignment epochs 1 is not equal to partitions 3 for topic "
                    + FOO_TOPIC_ID))
                .count());
        }
    }
}
