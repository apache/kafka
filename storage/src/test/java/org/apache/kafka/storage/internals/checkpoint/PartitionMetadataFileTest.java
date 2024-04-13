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
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PartitionMetadataFileTest  {
    private final File dir = assertDoesNotThrow(() -> Files.createTempDirectory("tmp")).toFile();

    @AfterEach
    public void tearDown() {
        assertDoesNotThrow(() -> Utils.delete(dir));
    }

    @Test
    public void testSetRecordWithDifferentTopicId() {
        File partitionMetadata = PartitionMetadataFile.newFile(dir);
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(partitionMetadata, null);
        Uuid topicId = Uuid.randomUuid();
        assertDoesNotThrow(() -> partitionMetadataFile.record(topicId));
        Uuid differentTopicId = Uuid.randomUuid();
        assertThrows(InconsistentTopicIdException.class, () -> partitionMetadataFile.record(differentTopicId));
    }

    @Test
    public void testSetRecordWithSameTopicId() {
        File partitionMetadata = PartitionMetadataFile.newFile(dir);
        PartitionMetadataFile partitionMetadataFile = new PartitionMetadataFile(partitionMetadata, null);
        Uuid topicId = Uuid.randomUuid();
        assertDoesNotThrow(() -> partitionMetadataFile.record(topicId));
        assertDoesNotThrow(() -> partitionMetadataFile.record(topicId));
    }
}
