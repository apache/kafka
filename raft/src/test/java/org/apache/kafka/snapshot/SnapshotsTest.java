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
package org.apache.kafka.snapshot;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

final public class SnapshotsTest {

    @Test
    public void testValidSnapshotFilename() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(
            TestUtils.RANDOM.nextInt(Integer.MAX_VALUE),
            TestUtils.RANDOM.nextInt(Integer.MAX_VALUE)
        );
        Path path = Snapshots.snapshotPath(FileSystems.getDefault().getPath("/root"), snapshotId);
        SnapshotPath snapshotPath = Snapshots.parse(path).get();

        assertEquals(path, snapshotPath.path);
        assertEquals(snapshotId, snapshotPath.snapshotId);
        assertEquals(false, snapshotPath.partial);
    }

    @Test
    public void testValidPartialSnapshotFilename() throws IOException {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(
            TestUtils.RANDOM.nextInt(Integer.MAX_VALUE),
            TestUtils.RANDOM.nextInt(Integer.MAX_VALUE)
        );

        Path path = Snapshots.createTempFile(TestUtils.tempDirectory().toPath(), snapshotId);
        // Delete it as we only need the path for testing
        Files.delete(path);

        SnapshotPath snapshotPath = Snapshots.parse(path).get();

        assertEquals(path, snapshotPath.path);
        assertEquals(snapshotId, snapshotPath.snapshotId);
        assertEquals(true, snapshotPath.partial);
    }

    @Test
    public void testInvalidSnapshotFilenames() {
        Path root = FileSystems.getDefault().getPath("/");
        // Doesn't parse log files
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("00000000000000000000.log")));
        // Doesn't parse producer snapshots
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("00000000000000000000.snapshot")));
        // Doesn't parse offset indexes
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("00000000000000000000.index")));
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("00000000000000000000.timeindex")));
        // Leader epoch checkpoint
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("leader-epoch-checkpoint")));
        // partition metadata
        assertEquals(Optional.empty(), Snapshots.parse(root.resolve("partition.metadata")));
    }
}
