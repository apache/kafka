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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit test to verify that consumer deserialization logic works regardless of key format.
 * This validates that new code can handle both old format (null key) and new format (with key).
 */
public class RemoteLogMetadataDeserializationTest {
    private static final String METADATA_TOPIC = "__remote_log_metadata";
    private static final int SEG_SIZE = 1048576;
    private final Time time = Time.SYSTEM;

    /**
     * Test that consumer logic can deserialize messages regardless of key format.
     * This test verifies that the deserialization depends only on the value field.
     */
    @Test
    public void testConsumerCanDeserializeMessagesWithAnyKeyFormat() {
        TopicIdPartition topicIdPartition = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-key-format", 0)
        );

        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        long endOffset = 500L;
        int brokerLeaderEpoch = 1;

        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                segmentId,
                0L,
                endOffset,
                -1L,
                0,
                time.milliseconds(),
                SEG_SIZE,
                Collections.singletonMap(0, 0L),
                brokerLeaderEpoch
        );

        RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
        byte[] serializedValue = serde.serialize(metadata);

        // Test 1: Simulate old format (null key)
        ConsumerRecord<byte[], byte[]> oldFormatRecord = new ConsumerRecord<>(
                METADATA_TOPIC,
                0,
                100L,
                null,  // Old format: null key
                serializedValue
        );

        RemoteLogMetadata deserializedOld = assertDoesNotThrow(() -> serde.deserialize(oldFormatRecord.value()),
                "Should be able to deserialize message with null key");
        assertNotNull(deserializedOld);
        assertEquals(segmentId, ((RemoteLogSegmentMetadata) deserializedOld).remoteLogSegmentId());
        assertNull(oldFormatRecord.key(), "Old format should have null key");

        // Test 2: Simulate new format (with key)
        String newFormatKey = metadata.metadataKey();
        ConsumerRecord<byte[], byte[]> newFormatRecord = new ConsumerRecord<>(
                METADATA_TOPIC,
                0,
                101L,
                newFormatKey.getBytes(),  // New format: has key
                serializedValue
        );

        RemoteLogMetadata deserializedNew = assertDoesNotThrow(() -> serde.deserialize(newFormatRecord.value()),
                "Should be able to deserialize message with key");
        assertNotNull(deserializedNew);
        assertEquals(segmentId, ((RemoteLogSegmentMetadata) deserializedNew).remoteLogSegmentId());
        assertNotNull(newFormatRecord.key(), "New format should have key");
        assertEquals(newFormatKey, new String(newFormatRecord.key()));

        // Test 3: Verify both deserializations produce the same metadata
        assertEquals(
                ((RemoteLogSegmentMetadata) deserializedOld).remoteLogSegmentId(),
                ((RemoteLogSegmentMetadata) deserializedNew).remoteLogSegmentId(),
                "Deserialized metadata should be the same regardless of key format"
        );
    }
}
