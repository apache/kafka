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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataSnapshotFile.Snapshot;

public class RemoteLogMetadataSnapshotFileTest {

    File logDir = TestUtils.tempDirectory();
    File partitionDir = new File(logDir, "test-0");
    RemoteLogMetadataSnapshotFile snapshotFile = new RemoteLogMetadataSnapshotFile(partitionDir.toPath());

    @BeforeEach
    public void setUp() {
        partitionDir.mkdirs();
    }

    @Test
    public void testReadWhenSnapshotFileDoesNotExist() throws IOException {
        assertFalse(snapshotFile.read().isPresent());
    }

    @Test
    public void testReadWhenSnapshotFileIsEmpty() throws IOException {
        assertTrue(snapshotFile.getMetadataStoreFile().createNewFile());
        assertFalse(snapshotFile.read().isPresent());
    }

    @Test
    public void testWriteEmptySnapshot() throws IOException {
        int metadataPartition = 0;
        long metadataOffset = 99L;
        List<RemoteLogSegmentMetadataSnapshot> metadataSnapshots = Collections.emptyList();
        Snapshot snapshot = new Snapshot(metadataPartition, metadataOffset, metadataSnapshots);
        snapshotFile.write(snapshot);
        assertTrue(snapshotFile.read().isPresent());
        assertTrue(snapshotFile.read().get().remoteLogSegmentMetadataSnapshots().isEmpty());
    }

    @Test
    public void testWriteSnapshot() throws IOException {
        int metadataPartition = 0;
        long metadataOffset = 99L;
        List<RemoteLogSegmentMetadataSnapshot> metadataSnapshots = new ArrayList<>();
        metadataSnapshots.add(segmentSnapshot(0, 100, Collections.singletonMap(1, 0L)));
        metadataSnapshots.add(segmentSnapshot(101, 200, Collections.singletonMap(2, 101L)));
        Snapshot expectedSnapshot = new Snapshot(metadataPartition, metadataOffset, metadataSnapshots);

        snapshotFile.write(expectedSnapshot);
        assertTrue(snapshotFile.read().isPresent());
        assertEquals(expectedSnapshot, snapshotFile.read().get());
    }

    @Test
    public void testOverwriteEmptySnapshot() throws IOException {
        int metadataPartition = 0;
        long metadataOffset = 99L;
        List<RemoteLogSegmentMetadataSnapshot> metadataSnapshots = Collections.emptyList();
        Snapshot snapshot = new Snapshot(metadataPartition, metadataOffset, metadataSnapshots);
        snapshotFile.write(snapshot);
        assertTrue(snapshotFile.read().isPresent());
        assertTrue(snapshotFile.read().get().remoteLogSegmentMetadataSnapshots().isEmpty());

        List<RemoteLogSegmentMetadataSnapshot> newMetadataSnapshots = new ArrayList<>();
        newMetadataSnapshots.add(segmentSnapshot(0, 100, Collections.singletonMap(1, 0L)));
        newMetadataSnapshots.add(segmentSnapshot(101, 200, Collections.singletonMap(2, 101L)));
        Snapshot newSnapshot = new Snapshot(metadataPartition, metadataOffset, newMetadataSnapshots);
        snapshotFile.write(newSnapshot);
        assertTrue(snapshotFile.read().isPresent());
        assertEquals(newSnapshot, snapshotFile.read().get());
    }

    private RemoteLogSegmentMetadataSnapshot segmentSnapshot(long startOffset,
                                                             long endOffset,
                                                             Map<Integer, Long> segmentLeaderEpochs) {
        CustomMetadata customMetadata = new CustomMetadata(new byte[]{'a'});
        return new RemoteLogSegmentMetadataSnapshot(
                Uuid.randomUuid(), startOffset, endOffset, 1L, 1, 100, 1024,
                Optional.of(customMetadata), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segmentLeaderEpochs);
    }
}
