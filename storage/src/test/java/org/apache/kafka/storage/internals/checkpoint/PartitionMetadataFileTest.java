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
import org.apache.kafka.common.errors.InconsistentTopicIdException;

import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionMetadataFileTest  {
    private final File dir = TestUtils.tempDirectory();
    private final File file = PartitionMetadataFile.newFile(dir);

    @Test
    public void testSetRecordWithDifferentTopicId() {
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(file, null);
        Uuid topicId = Uuid.randomUuid();
        partitionMetadataFile.record(topicId);
        Uuid differentTopicId = Uuid.randomUuid();
        assertThrows(InconsistentTopicIdException.class, () -> partitionMetadataFile.record(differentTopicId));
    }

    @Test
    public void testSetRecordWithSameTopicId() {
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(file, null);
        Uuid topicId = Uuid.randomUuid();
        partitionMetadataFile.record(topicId);
        assertDoesNotThrow(() -> partitionMetadataFile.record(topicId));

        Uuid sameTopicId = Uuid.fromString(topicId.toString());
        partitionMetadataFile.record(sameTopicId);
        assertDoesNotThrow(() -> partitionMetadataFile.record(topicId));
    }

    @Test
    public void testMaybeFlushWithTopicIdPresent() throws IOException {
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(file, null);

        Uuid topicId = Uuid.randomUuid();
        partitionMetadataFile.record(topicId);
        partitionMetadataFile.maybeFlush();

        // The following content is encoded by PartitionMetadata#encode, which is invoked during the flush
        List<String> lines = Files.readAllLines(file.toPath());
        assertEquals(2, lines.size());
        assertEquals("version: 0", lines.get(0));
        assertEquals("topic_id: " + topicId, lines.get(1));
    }

    @Test
    public void testMaybeFlushWithNoTopicIdPresent() {
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(file, null);
        partitionMetadataFile.maybeFlush();

        assertEquals(0, file.length());
    }

    @Test
    public void testRead() {
        LogDirFailureChannel channel = Mockito.mock(LogDirFailureChannel.class);
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(file, channel);

        Uuid topicId = Uuid.randomUuid();
        partitionMetadataFile.record(topicId);
        partitionMetadataFile.maybeFlush();

        PartitionMetadata metadata = partitionMetadataFile.read();
        assertEquals(0, metadata.version());
        assertEquals(topicId, metadata.topicId());
    }
}