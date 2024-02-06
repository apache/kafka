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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileBasedRemoteLogMetadataCacheTest {

    @Test
    public void testFileBasedRemoteLogMetadataCacheWithUnreferencedSegments() throws Exception {
        TopicIdPartition partition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 0));
        int brokerId = 0;
        Path path = TestUtils.tempDirectory().toPath();

        // Create file based metadata cache.
        FileBasedRemoteLogMetadataCache cache = new FileBasedRemoteLogMetadataCache(partition, path);

        // Add a segment with start offset as 0 for leader epoch 0.
        RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(partition, Uuid.randomUuid());
        RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(segmentId1,
                                                                          0, 100, System.currentTimeMillis(), brokerId, System.currentTimeMillis(),
                                                                          1024 * 1024, Collections.singletonMap(0, 0L));
        cache.addCopyInProgressSegment(metadata1);
        RemoteLogSegmentMetadataUpdate metadataUpdate1 = new RemoteLogSegmentMetadataUpdate(
                segmentId1, System.currentTimeMillis(),
                Optional.of(new CustomMetadata(new byte[]{0, 1, 2, 3})),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        cache.updateRemoteLogSegmentMetadata(metadataUpdate1);
        Optional<RemoteLogSegmentMetadata> receivedMetadata = cache.remoteLogSegmentMetadata(0, 0L);
        assertTrue(receivedMetadata.isPresent());
        assertEquals(metadata1.createWithUpdates(metadataUpdate1), receivedMetadata.get());

        // Add a new segment with start offset as 0 for leader epoch 0, which should replace the earlier segment.
        RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(partition, Uuid.randomUuid());
        RemoteLogSegmentMetadata metadata2 = new RemoteLogSegmentMetadata(segmentId2,
                                                                          0, 900, System.currentTimeMillis(), brokerId, System.currentTimeMillis(),
                                                                          1024 * 1024, Collections.singletonMap(0, 0L));
        cache.addCopyInProgressSegment(metadata2);
        RemoteLogSegmentMetadataUpdate metadataUpdate2 = new RemoteLogSegmentMetadataUpdate(
                segmentId2, System.currentTimeMillis(),
                Optional.of(new CustomMetadata(new byte[]{4, 5, 6, 7})),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        cache.updateRemoteLogSegmentMetadata(metadataUpdate2);

        // Fetch segment for leader epoch:0 and start offset:0, it should be the newly added segment.
        Optional<RemoteLogSegmentMetadata> receivedMetadata2 = cache.remoteLogSegmentMetadata(0, 0L);
        assertTrue(receivedMetadata2.isPresent());
        assertEquals(metadata2.createWithUpdates(metadataUpdate2), receivedMetadata2.get());
        // Flush the cache to the file.
        cache.flushToFile(0, 0L);

        // Create a new cache with loading from the stored path.
        FileBasedRemoteLogMetadataCache loadedCache = new FileBasedRemoteLogMetadataCache(partition, path);

        // Fetch segment for leader epoch:0 and start offset:0, it should be metadata2.
        // This ensures that the ordering of metadata is taken care after loading from the stored snapshots.
        Optional<RemoteLogSegmentMetadata> receivedMetadataAfterLoad = loadedCache.remoteLogSegmentMetadata(0, 0L);
        assertTrue(receivedMetadataAfterLoad.isPresent());
        assertEquals(metadata2.createWithUpdates(metadataUpdate2), receivedMetadataAfterLoad.get());
    }
}
