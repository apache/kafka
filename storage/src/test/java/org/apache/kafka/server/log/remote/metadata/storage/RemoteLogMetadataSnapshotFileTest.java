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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class RemoteLogMetadataSnapshotFileTest {

    @Test
    public void testEmptyCommittedLogMetadataFile() throws Exception {
        File metadataStoreDir = TestUtils.tempDirectory("_rlmm_committed");
        RemoteLogMetadataSnapshotFile snapshotFile = new RemoteLogMetadataSnapshotFile(metadataStoreDir.toPath());

        // There should be an empty snapshot as nothing is written into it.
        Assertions.assertFalse(snapshotFile.read().isPresent());
    }

    @Test
    public void testEmptySnapshotWithCommittedLogMetadataFile() throws Exception {
        File metadataStoreDir = TestUtils.tempDirectory("_rlmm_committed");
        RemoteLogMetadataSnapshotFile snapshotFile = new RemoteLogMetadataSnapshotFile(metadataStoreDir.toPath());

        snapshotFile.write(new RemoteLogMetadataSnapshotFile.Snapshot(0, 0L, Collections.emptyList()));

        // There should be an empty snapshot as the written snapshot did not have any remote log segment metadata.
        Assertions.assertTrue(snapshotFile.read().isPresent());
        Assertions.assertTrue(snapshotFile.read().get().remoteLogSegmentMetadataSnapshots().isEmpty());
    }

    @Test
    public void testWriteReadCommittedLogMetadataFile() throws Exception {
        File metadataStoreDir = TestUtils.tempDirectory("_rlmm_committed");
        RemoteLogMetadataSnapshotFile snapshotFile = new RemoteLogMetadataSnapshotFile(metadataStoreDir.toPath());

        List<RemoteLogSegmentMetadataSnapshot> remoteLogSegmentMetadatas = new ArrayList<>();
        long startOffset = 0;
        for (int i = 0; i < 100; i++) {
            long endOffset = startOffset + 100L;
            CustomMetadata customMetadata = new CustomMetadata(new byte[]{(byte) i});
            remoteLogSegmentMetadatas.add(
                    new RemoteLogSegmentMetadataSnapshot(Uuid.randomUuid(), startOffset, endOffset,
                                                         System.currentTimeMillis(), 1, 100, 1024,
                                                         Optional.of(customMetadata),
                                                         RemoteLogSegmentState.COPY_SEGMENT_FINISHED, Collections.singletonMap(i, startOffset)
                    ));
            startOffset = endOffset + 1;
        }

        RemoteLogMetadataSnapshotFile.Snapshot snapshot = new RemoteLogMetadataSnapshotFile.Snapshot(0, 120,
                                                                                                     remoteLogSegmentMetadatas);
        snapshotFile.write(snapshot);

        Optional<RemoteLogMetadataSnapshotFile.Snapshot> maybeReadSnapshot = snapshotFile.read();
        Assertions.assertTrue(maybeReadSnapshot.isPresent());

        Assertions.assertEquals(snapshot, maybeReadSnapshot.get());
        Assertions.assertEquals(new HashSet<>(snapshot.remoteLogSegmentMetadataSnapshots()),
                                new HashSet<>(maybeReadSnapshot.get().remoteLogSegmentMetadataSnapshots()));
    }
}
