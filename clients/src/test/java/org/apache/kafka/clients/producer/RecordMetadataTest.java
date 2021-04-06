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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RecordMetadataTest {

    @Test
    public void testConstructionWithMissingRelativeOffset() {
        TopicPartition tp = new TopicPartition("foo", 0);
        long timestamp = 2340234L;
        int keySize = 3;
        int valueSize = 5;

        RecordMetadata metadata = new RecordMetadata(tp, -1L, -1L, timestamp, keySize, valueSize);
        assertEquals(tp.topic(), metadata.topic());
        assertEquals(tp.partition(), metadata.partition());
        assertEquals(timestamp, metadata.timestamp());
        assertFalse(metadata.hasOffset());
        assertEquals(-1L, metadata.offset());
        assertEquals(keySize, metadata.serializedKeySize());
        assertEquals(valueSize, metadata.serializedValueSize());
    }

    @Test
    public void testConstructionWithRelativeOffset() {
        TopicPartition tp = new TopicPartition("foo", 0);
        long timestamp = 2340234L;
        int keySize = 3;
        int valueSize = 5;
        long baseOffset = 15L;
        long relativeOffset = 3L;

        RecordMetadata metadata = new RecordMetadata(tp, baseOffset, relativeOffset, timestamp, keySize, valueSize);
        assertEquals(tp.topic(), metadata.topic());
        assertEquals(tp.partition(), metadata.partition());
        assertEquals(timestamp, metadata.timestamp());
        assertEquals(baseOffset + relativeOffset, metadata.offset());
        assertEquals(keySize, metadata.serializedKeySize());
        assertEquals(valueSize, metadata.serializedValueSize());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testNullChecksumDoesNotThrow() {
        long timestamp = 2340234L;
        int keySize = 3;
        int valueSize = 5;
        new RecordMetadata(new TopicPartition("foo", 0), 15L, 3L, timestamp, null,
                keySize, valueSize);
    }

}
