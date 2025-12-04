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
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopicCollection;
import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OffsetsForLeaderEpochRequestTest {

    @Test
    public void testForConsumerRequiresVersion3() {
        OffsetsForLeaderEpochRequest.Builder builder = OffsetsForLeaderEpochRequest.Builder.forConsumer(new OffsetForLeaderTopicCollection());
        for (short version = 0; version < 3; version++) {
            final short v = version;
            assertThrows(UnsupportedVersionException.class, () -> builder.build(v));
        }

        for (short version = 3; version <= ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(); version++) {
            OffsetsForLeaderEpochRequest request = builder.build(version);
            assertEquals(OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID, request.replicaId());
        }
    }

    @Test
    public void testForFollower() {
        short version = 4;
        int replicaId = 1;
        OffsetsForLeaderEpochRequest.Builder builder = OffsetsForLeaderEpochRequest.Builder.forFollower(
                new OffsetForLeaderTopicCollection(), replicaId);
        OffsetsForLeaderEpochRequest request = builder.build();
        OffsetsForLeaderEpochRequest parsed = OffsetsForLeaderEpochRequest.parse(request.serialize(), version);
        assertEquals(replicaId, parsed.replicaId());
    }

    @Test
    public void testVersion4BelowFailUseTopicId() {
        // Version 4 and below require topic names, not topic IDs
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        OffsetForLeaderTopic topic = new OffsetForLeaderTopic()
                .setTopicId(Uuid.randomUuid())  // Only topicId, no topic name
                .setPartitions(Collections.singletonList(
                        new OffsetForLeaderPartition()
                                .setPartition(0)
                                .setLeaderEpoch(1)
                ));
        topics.add(topic);

        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData()
                .setReplicaId(1)
                .setTopics(topics);

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                (short) 2, (short) 4, data);

        // Should throw UnsupportedVersionException when building version 4 without topic names
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 4));
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 3));
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 2));
    }

    @Test
    public void testVersion4SucceedsWithTopicNames() {
        // Version 4 should succeed when topic names are provided
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        OffsetForLeaderTopic topic = new OffsetForLeaderTopic()
                .setTopic("test-topic")  // Topic name provided
                .setPartitions(Collections.singletonList(
                        new OffsetForLeaderPartition()
                                .setPartition(0)
                                .setLeaderEpoch(1)
                ));
        topics.add(topic);

        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData()
                .setReplicaId(1)
                .setTopics(topics);

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                (short) 2, (short) 4, data);

        // Should succeed for version 4 with topic names
        OffsetsForLeaderEpochRequest request = builder.build((short) 4);
        assertEquals(1, request.replicaId());
    }

    @Test
    public void testVersion5RequiresTopicIds() {
        // Version 5 and above require topic IDs, not topic names
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        OffsetForLeaderTopic topic = new OffsetForLeaderTopic()
                .setTopic("test-topic")  // Only topic name, no topicId
                .setPartitions(Collections.singletonList(
                        new OffsetForLeaderPartition()
                                .setPartition(0)
                                .setLeaderEpoch(1)
                ));
        topics.add(topic);

        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData()
                .setReplicaId(-1)
                .setTopics(topics);

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                (short) 3, (short) 5, data);

        // Should throw UnsupportedVersionException when building version 5 without topic IDs
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 5));
    }

    @Test
    public void testVersion5SucceedsWithTopicIds() {
        // Version 5 should succeed when topic IDs are provided
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        OffsetForLeaderTopic topic = new OffsetForLeaderTopic()
                .setTopicId(Uuid.randomUuid())  // Topic ID provided
                .setPartitions(Collections.singletonList(
                        new OffsetForLeaderPartition()
                                .setPartition(0)
                                .setLeaderEpoch(1)
                ));
        topics.add(topic);

        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData()
                .setReplicaId(-1)
                .setTopics(topics);

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                (short) 3, (short) 5, data);

        // Should succeed for version 5 with topic IDs
        OffsetsForLeaderEpochRequest request = builder.build((short) 5);
        assertEquals(-1, request.replicaId());
    }

    @Test
    public void testVersion5RejectsZeroTopicId() {
        // Version 5 should reject ZERO_UUID as topic ID
        OffsetForLeaderTopicCollection topics = new OffsetForLeaderTopicCollection();
        OffsetForLeaderTopic topic = new OffsetForLeaderTopic()
                .setTopicId(Uuid.ZERO_UUID)  // Invalid: ZERO_UUID
                .setPartitions(Collections.singletonList(
                        new OffsetForLeaderPartition()
                                .setPartition(0)
                                .setLeaderEpoch(1)
                ));
        topics.add(topic);

        OffsetForLeaderEpochRequestData data = new OffsetForLeaderEpochRequestData()
                .setReplicaId(-1)
                .setTopics(topics);

        OffsetsForLeaderEpochRequest.Builder builder = new OffsetsForLeaderEpochRequest.Builder(
                (short) 3, (short) 5, data);

        // Should throw UnsupportedVersionException when using ZERO_UUID
        assertThrows(UnsupportedVersionException.class, () -> builder.build((short) 5));
    }
}
