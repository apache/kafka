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

package org.apache.kafka.storage.internals.checkpoint;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionMetadataReadBufferTest {

    @Test
    void testReadValidPartitionMetadata() throws IOException {
        Uuid topicId = Uuid.randomUuid();
        PartitionMetadataReadBuffer readBuffer = new PartitionMetadataReadBuffer(
            "/tmp/partition.metadata",
            reader("version: 0\ntopic_id: " + topicId + "\n")
        );

        PartitionMetadata metadata = readBuffer.read();
        assertEquals(0, metadata.version());
        assertEquals(topicId, metadata.topicId());
    }

    @Test
    void testReadFailsForEmptyFile() {
        PartitionMetadataReadBuffer readBuffer = new PartitionMetadataReadBuffer(
            "/tmp/partition.metadata",
            reader("")
        );

        IOException exception = assertThrows(IOException.class, readBuffer::read);
        assertTrue(exception.getMessage().contains("Malformed line in partition metadata file"));
    }

    @Test
    void testReadFailsWhenTopicIdLineIsMissing() {
        PartitionMetadataReadBuffer readBuffer = new PartitionMetadataReadBuffer(
            "/tmp/partition.metadata",
            reader("version: 0\n")
        );

        IOException exception = assertThrows(IOException.class, readBuffer::read);
        assertTrue(exception.getMessage().contains("Malformed line in partition metadata file"));
    }

    @Test
    void testReadFailsWhenTopicIdIsMalformed() {
        PartitionMetadataReadBuffer readBuffer = new PartitionMetadataReadBuffer(
            "/tmp/partition.metadata",
            reader("version: 0\ntopic_id: not-a-uuid\n")
        );

        IOException exception = assertThrows(IOException.class, readBuffer::read);
        assertTrue(exception.getMessage().contains("Malformed line in partition metadata file"));
    }

    @Test
    void testReadFailsWhenTopicIdIsZeroUuid() {
        PartitionMetadataReadBuffer readBuffer = new PartitionMetadataReadBuffer(
            "/tmp/partition.metadata",
            reader("version: 0\ntopic_id: " + Uuid.ZERO_UUID + "\n")
        );

        IOException exception = assertThrows(IOException.class, readBuffer::read);
        assertTrue(exception.getMessage().contains("Invalid topic ID in partition metadata file"));
    }

    private BufferedReader reader(String content) {
        return new BufferedReader(new StringReader(content));
    }
}
