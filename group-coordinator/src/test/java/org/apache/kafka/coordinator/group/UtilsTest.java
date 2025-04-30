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
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.image.MetadataImage;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class UtilsTest {
    private static final Uuid FOO_TOPIC_ID = Uuid.randomUuid();
    private static final String FOO_TOPIC_NAME = "foo";
    private static final String BAR_TOPIC_NAME = "bar";
    private static final int FOO_NUM_PARTITIONS = 2;
    private static final MetadataImage FOO_METADATA_IMAGE = new MetadataImageBuilder()
        .addTopic(FOO_TOPIC_ID, FOO_TOPIC_NAME, FOO_NUM_PARTITIONS)
        .addRacks()
        .build();
    private static final XXHash64 LZ4_HASH_INSTANCE = XXHashFactory.fastestInstance().hash64();

    @Test
    void testComputeTopicHash() throws IOException {
        long result = Utils.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(512);
             DataOutputStream dos = new DataOutputStream(bbos)) {
            dos.writeByte(0); // magic byte
            dos.writeLong(FOO_TOPIC_ID.hashCode()); // topic ID
            dos.writeUTF(FOO_TOPIC_NAME); // topic name
            dos.writeInt(FOO_NUM_PARTITIONS); // number of partitions
            dos.writeInt(0); // partition 0
            dos.writeUTF("0:rack0,1:rack1"); // rack of partition 0
            dos.writeInt(1); // partition 1
            dos.writeUTF("0:rack1,1:rack2"); // rack of partition 1
            dos.flush();
            ByteBuffer topicBytes = bbos.buffer().flip();
            assertEquals(LZ4_HASH_INSTANCE.hash(topicBytes, 0), result);
        }
    }

    @Test
    void testComputeTopicHashWithDifferentMagicByte() throws IOException {
        long result = Utils.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(512);
             DataOutputStream dos = new DataOutputStream(bbos)) {
            dos.writeByte(1); // different magic byte
            dos.writeLong(FOO_TOPIC_ID.hashCode()); // topic ID
            dos.writeUTF(FOO_TOPIC_NAME); // topic name
            dos.writeInt(FOO_NUM_PARTITIONS); // number of partitions
            dos.writeInt(0); // partition 0
            dos.writeUTF("0:rack0,1:rack1"); // rack of partition 0
            dos.writeInt(1); // partition 1
            dos.writeUTF("0:rack1,1:rack2"); // rack of partition 1
            dos.flush();
            ByteBuffer topicBytes = bbos.buffer().flip();
            assertNotEquals(LZ4_HASH_INSTANCE.hash(topicBytes, 0), result);
        }
    }

    @Test
    void testComputeTopicHashWithDifferentPartitionOrder() throws IOException {
        long result = Utils.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(512);
             DataOutputStream dos = new DataOutputStream(bbos)) {
            dos.writeByte(0); // magic byte
            dos.writeLong(FOO_TOPIC_ID.hashCode()); // topic ID
            dos.writeUTF(FOO_TOPIC_NAME); // topic name
            dos.writeInt(FOO_NUM_PARTITIONS); // number of partitions
            // different partition order
            dos.writeInt(1); // partition 1
            dos.writeUTF("0:rack1,1:rack2"); // rack of partition 1
            dos.writeInt(0); // partition 0
            dos.writeUTF("0:rack0,1:rack1"); // rack of partition 0
            dos.flush();
            ByteBuffer topicBytes = bbos.buffer().flip();
            assertNotEquals(LZ4_HASH_INSTANCE.hash(topicBytes, 0), result);
        }
    }

    @Test
    void testComputeTopicHashWithDifferentRackOrder() throws IOException {
        long result = Utils.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(512);
             DataOutputStream dos = new DataOutputStream(bbos)) {
            dos.writeByte(0); // magic byte
            dos.writeLong(FOO_TOPIC_ID.hashCode()); // topic ID
            dos.writeUTF(FOO_TOPIC_NAME); // topic name
            dos.writeInt(FOO_NUM_PARTITIONS); // number of partitions
            dos.writeInt(0); // partition 0
            dos.writeUTF("0:rack1,1:rack0"); // different rack order of partition 0
            dos.writeInt(1); // partition 1
            dos.writeUTF("0:rack1,1:rack2"); // rack of partition 1
            dos.flush();
            ByteBuffer topicBytes = bbos.buffer().flip();
            assertNotEquals(LZ4_HASH_INSTANCE.hash(topicBytes, 0), result);
        }
    }

    @ParameterizedTest
    @MethodSource("differentFieldGenerator")
    void testComputeTopicHashWithDifferentField(MetadataImage differentImage, Uuid topicId) throws IOException {
        long result = Utils.computeTopicHash(FOO_METADATA_IMAGE.topics().getTopic(FOO_TOPIC_ID), FOO_METADATA_IMAGE.cluster());

        assertNotEquals(
            Utils.computeTopicHash(
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
}
