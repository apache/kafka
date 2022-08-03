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
package org.apache.kafka.metadata.util;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;


class LocalDirectorySourceTest {
    private static String runSelect(
        boolean considerSnapshots,
        long lastReadOffset,
        String... names
    ) {
        List<Path> paths = new ArrayList<>();
        for (String name : names) {
            paths.add(Paths.get(name));
        }
        return LocalDirectorySource.selectNextFileName(considerSnapshots, lastReadOffset, paths.iterator());
    }

    private final static String[] LISTING1 = new String[] {
        "00000000000000000000.index",
        "00000000000000000000.log",
        "00000000000000000000.timeindex",
        "00000000000000004758.snapshot",
        "00000000000000004794.snapshot",
        "leader-epoch-checkpoint",
        "partition.metadata",
        "quorum-state"
    };

    @Test
    public void testSelectNextFileName() {
        assertEquals("00000000000000000000.log", runSelect(false, -1, LISTING1));
        assertEquals("00000000000000000000.log", runSelect(true, -1, LISTING1));
        assertEquals("", runSelect(false, 1000, LISTING1));
    }

    private final static String[] LISTING2 = new String[] {
            "00000000000000000000.index",
            "00000000000000000000.log",
            "00000000000000000000.timeindex",
            "00000000000000004501-0000000020.checkpoint",
            "00000000000000004567.index",
            "00000000000000004567.log",
            "00000000000000004567.timeindex",
            "00000000000000004758.snapshot",
            "00000000000000004794.snapshot",
            "leader-epoch-checkpoint",
            "partition.metadata",
            "quorum-state"
    };

    @Test
    public void testSelectNextFileNameWithSnapshots() {
        assertEquals("00000000000000000000.log", runSelect(false, -1, LISTING2));
        assertEquals("00000000000000004501-0000000020.checkpoint", runSelect(true, -1, LISTING2));
        assertEquals("00000000000000004567.log", runSelect(false, 4501, LISTING2));
        assertEquals("", runSelect(false, 6000, LISTING2));
    }

    @Test
    public void testExtractLogFileOffsetFromName() {
        assertEquals(0L, LocalDirectorySource.extractLogFileOffsetFromName("00000000000000000000.log"));
        assertEquals(449000400000L, LocalDirectorySource.extractLogFileOffsetFromName("00000000449000400000.log"));
    }

    @Test
    public void testExtractSnapshotFileOffsetFromName() {
        assertEquals(4501L, LocalDirectorySource.
                extractSnapshotFileOffsetFromName("00000000000000004501-0000000020.checkpoint"));
    }

    @Test
    public void testCalculateSnapshotId() {
        assertEquals(new OffsetAndEpoch(4501L, 20), LocalDirectorySource.
                calculateSnapshotId("00000000000000004501-0000000020.checkpoint"));
    }

    @Test
    public void testParseControlRecords() {
        assertEquals(Optional.empty(),
            LocalDirectorySource.parseControlRecords(Batch.data(1000L, 123, 456L, 200, Arrays.asList(
                new ApiMessageAndVersion(new SnapshotHeaderRecord(), (short) 0)))));
        assertEquals(Optional.of(new LeaderAndEpoch(OptionalInt.of(5), 123)),
            LocalDirectorySource.parseControlRecords(Batch.data(1000L, 123, 456L, 200, Arrays.asList(
                new ApiMessageAndVersion(new SnapshotHeaderRecord(), (short) 0),
                new ApiMessageAndVersion(new LeaderChangeMessage().setLeaderId(5), (short) 0)))));
        assertEquals(Optional.of(new LeaderAndEpoch(OptionalInt.empty(), 123)),
            LocalDirectorySource.parseControlRecords(Batch.data(1000L, 123, 456L, 200, Arrays.asList(
                    new ApiMessageAndVersion(new SnapshotHeaderRecord(), (short) 0),
                    new ApiMessageAndVersion(new LeaderChangeMessage().setLeaderId(6), (short) 0),
                    new ApiMessageAndVersion(new LeaderChangeMessage().setLeaderId(-1), (short) 0)))));
    }
}
