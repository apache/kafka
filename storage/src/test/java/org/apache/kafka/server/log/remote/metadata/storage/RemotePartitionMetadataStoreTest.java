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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RemoteStorageNotReadyException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RemotePartitionMetadataStoreTest {

    Time time = new MockTime();
    Path logDirPath = Paths.get(TestUtils.tempDirectory().getPath());
    RemotePartitionMetadataStore metadataStore = new RemotePartitionMetadataStore(logDirPath);

    @Test
    void shouldThrowRetriableErrorWhenNotInitialized() throws RemoteStorageException, IOException {
        int brokerId = 0;
        String topic = "test";
        int partition = 0;
        Uuid topicId = Uuid.randomUuid();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, topicPartition);
        int startOffset = 0;
        int endOffset = 99;
        int segmentSizeInBytes = 1024;

        Files.createDirectories(logDirPath.resolve(topicPartition.toString()));
        metadataStore.maybeLoadPartition(topicIdPartition);
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(0, 0L);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset,
                time.milliseconds(), brokerId, time.milliseconds(), segmentSizeInBytes, segmentLeaderEpochs);
        metadataStore.handleRemoteLogSegmentMetadata(metadata);
        assertThrows(RemoteStorageNotReadyException.class, () -> metadataStore.listRemoteLogSegments(topicIdPartition));
        metadataStore.markInitialized(topicIdPartition);
        assertEquals(metadata, metadataStore.listRemoteLogSegments(topicIdPartition).next());
    }
}