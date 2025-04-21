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
import org.apache.kafka.image.MetadataImage;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class GroupTest {
    private static final Uuid FOO_TOPIC_ID = Uuid.randomUuid();
    private static final String FOO_TOPIC_NAME = "foo";
    private static final String BAR_TOPIC_NAME = "bar";
    private static final int FOO_NUM_PARTITIONS = 2;
    private static final MetadataImage FOO_METADATA_IMAGE = new MetadataImageBuilder()
        .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
        .addRacks()
        .build();

    @Test
    void testComputeTopicHash() {
        long result = Group.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        HashFunction hf = Hashing.murmur3_128();
        Hasher topicHasher = hf.newHasher()
            .putByte((byte) 0) // magic byte
            .putLong(FOO_TOPIC_ID.hashCode()) // topic Id
            .putString(FOO_TOPIC_NAME, StandardCharsets.UTF_8) // topic name
            .putInt(FOO_NUM_PARTITIONS) // number of partitions
            .putInt(0) // partition 0
            .putString("rack0;rack1", StandardCharsets.UTF_8) // rack of partition 0
            .putInt(1) // partition 1
            .putString("rack1;rack2", StandardCharsets.UTF_8); // rack of partition 1
        assertEquals(topicHasher.hash().asLong(), result);
    }

    @Test
    void testComputeTopicHashWithDifferentMagicByte() {
        long result = Group.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        HashFunction hf = Hashing.murmur3_128();
        Hasher topicHasher = hf.newHasher()
            .putByte((byte) 1) // different magic byte
            .putLong(FOO_TOPIC_ID.hashCode()) // topic Id
            .putString(FOO_TOPIC_NAME, StandardCharsets.UTF_8) // topic name
            .putInt(FOO_NUM_PARTITIONS) // number of partitions
            .putInt(0) // partition 0
            .putString("rack0;rack1", StandardCharsets.UTF_8) // rack of partition 0
            .putInt(1) // partition 1
            .putString("rack1;rack2", StandardCharsets.UTF_8); // rack of partition 1
        assertNotEquals(topicHasher.hash().asLong(), result);
    }

    @Test
    void testComputeTopicHashWithDifferentPartitionOrder() {
        long result = Group.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        HashFunction hf = Hashing.murmur3_128();
        Hasher topicHasher = hf.newHasher()
            .putByte((byte) 0) // magic byte
            .putLong(FOO_TOPIC_ID.hashCode()) // topic Id
            .putString(FOO_TOPIC_NAME, StandardCharsets.UTF_8) // topic name
            .putInt(FOO_NUM_PARTITIONS) // number of partitions
            // different partition order
            .putInt(1) // partition 1
            .putString("rack1;rack2", StandardCharsets.UTF_8) // rack of partition 1
            .putInt(0) // partition 0
            .putString("rack0;rack1", StandardCharsets.UTF_8); // rack of partition 0
        assertNotEquals(topicHasher.hash().asLong(), result);
    }

    @Test
    void testComputeTopicHashWithDifferentRackOrder() {
        long result = Group.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        HashFunction hf = Hashing.murmur3_128();
        Hasher topicHasher = hf.newHasher()
            .putByte((byte) 0) // magic byte
            .putLong(FOO_TOPIC_ID.hashCode()) // topic Id
            .putString(FOO_TOPIC_NAME, StandardCharsets.UTF_8) // topic name
            .putInt(FOO_NUM_PARTITIONS) // number of partitions
            .putInt(0) // partition 0
            .putString("rack1;rack0", StandardCharsets.UTF_8) // different rack order of partition 0
            .putInt(1) // partition 1
            .putString("rack1;rack2", StandardCharsets.UTF_8); // rack of partition 1
        assertNotEquals(topicHasher.hash().asLong(), result);
    }

    @ParameterizedTest
    @MethodSource("differentFieldGenerator")
    void testComputeTopicHashWithDifferentField(MetadataImage differentImage, Uuid topicId) {
        long result = Group.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        assertNotEquals(
            Group.computeTopicHash(
                differentImage.topics().getTopic(topicId),
                differentImage.cluster()
            ),
            result
        );
    }

    private static Stream<Arguments> differentFieldGenerator() {
        Uuid differentTopicId = Uuid.randomUuid();
        return Stream.of(
            Arguments.of(new MetadataImageBuilder() // different topic id
                .addTopic(differentTopicId, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
                .addRacks()
                .build(),
                differentTopicId
            ),
            Arguments.of(new MetadataImageBuilder() // different topic name
                .addTopic(FOO_TOPIC_ID, "bar", FOO_NUM_PARTITIONS)
                .addRacks()
                .build(),
                FOO_TOPIC_ID
            ),
            Arguments.of(new MetadataImageBuilder() // different partitions
                .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, 1)
                .addRacks()
                .build(),
                FOO_TOPIC_ID
            ),
            Arguments.of(new MetadataImageBuilder() // different racks
                .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
                .build(),
                FOO_TOPIC_ID
            )
        );
    }

    @Test
    void testComputeGroupHash() {
        long result = Group.computeGroupHash(Map.of(
            BAR_TOPIC_NAME, 123L,
            FOO_TOPIC_NAME, 456L
        ));

        long expected = Hashing.combineOrdered(List.of(
            HashCode.fromLong(123L),
            HashCode.fromLong(456L)
        )).asLong();
        assertEquals(expected, result);
    }

    @Test
    void testComputeGroupHashWithDifferentOrder() {
        long result = Group.computeGroupHash(Map.of(
            BAR_TOPIC_NAME, 123L,
            FOO_TOPIC_NAME, 456L
        ));

        long unexpected = Hashing.combineOrdered(List.of(
            HashCode.fromLong(456L),
            HashCode.fromLong(123L)
        )).asLong();
        assertNotEquals(unexpected, result);
    }
}
