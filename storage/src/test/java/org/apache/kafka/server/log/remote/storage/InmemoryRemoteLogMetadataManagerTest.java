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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataCache;
import org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataCacheTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class covers basic unit tests for {@link InmemoryRemoteLogMetadataManager}. InmemoryRemoteLogMetadataManager is
 * used only in integration tests but not in production code. It mostly uses {@link RemoteLogMetadataCache} and it has
 * broad test coverage with {@link RemoteLogMetadataCacheTest}.
 */
public class InmemoryRemoteLogMetadataManagerTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    private final Time time = new MockTime(1);

    @Test
    public void testFetchSegments() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // 1.Create a segment with state COPY_SEGMENT_STARTED, and this segment should not be available.
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(0, 101L);
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 101L, 200L, -1L, BROKER_ID_0,
                time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segmentMetadata);

        // Search should not return the above segment.
        Assertions.assertFalse(rlmm.remoteLogSegmentMetadata(TP0, 0, 150).isPresent());

        // 2.Move that segment to COPY_SEGMENT_FINISHED state and this segment should be available.
        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_1);
        rlmm.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);
        RemoteLogSegmentMetadata expectedSegmentMetadata = segmentMetadata.createWithUpdates(segmentMetadataUpdate);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segmentMetadataForOffset150 = rlmm.remoteLogSegmentMetadata(TP0, 0, 150);
        Assertions.assertEquals(Optional.of(expectedSegmentMetadata), segmentMetadataForOffset150);
    }

    @Test
    public void testRemotePartitionDeletion() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // Create remote log segment metadata and add them to RLMM.

        // segment 0
        // offsets: [0-100]
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
        segmentLeaderEpochs.put(0, 0L);
        segmentLeaderEpochs.put(1, 20L);
        segmentLeaderEpochs.put(2, 50L);
        segmentLeaderEpochs.put(3, 80L);
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, 0L, 100L,
                -1L, BROKER_ID_0, time.milliseconds(), SEG_SIZE, segmentLeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segmentMetadata);
        RemoteLogSegmentMetadataUpdate segmentMetadataUpdate = new RemoteLogSegmentMetadataUpdate(
                segmentId, time.milliseconds(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_1);
        rlmm.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);

        RemoteLogSegmentMetadata expectedSegMetadata = segmentMetadata.createWithUpdates(segmentMetadataUpdate);

        // Check that the segment exists in RLMM.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset30Epoch1 = rlmm.remoteLogSegmentMetadata(TP0, 1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadata), segMetadataForOffset30Epoch1);

        // Mark the partition for deletion.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_MARKED));

        Optional<RemoteLogSegmentMetadata> segmentMetadataAfterDelMark = rlmm.remoteLogSegmentMetadata(TP0,
                1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadata), segmentMetadataAfterDelMark);

        // Set the partition deletion state as started. Partition and segments should still be accessible as they are not
        // yet deleted.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_STARTED));

        Optional<RemoteLogSegmentMetadata> segmentMetadataAfterDelStart = rlmm.remoteLogSegmentMetadata(TP0,
                1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadata), segmentMetadataAfterDelStart);

        // Set the partition deletion state as finished. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_FINISHED));

        Assertions.assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));
    }

    private RemotePartitionDeleteMetadata createRemotePartitionDeleteMetadata(RemotePartitionDeleteState state) {
        return new RemotePartitionDeleteMetadata(TP0, state, time.milliseconds(), BROKER_ID_0);
    }
}