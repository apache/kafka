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

import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class stores a list of EpochEntry(LeaderEpoch + Offsets) to memory
 *
 * The motivation for this class is to allow remote log manager to create the RemoteLogSegmentMetadata(RLSM)
 * with the correct leader epoch info for a specific segment. To do that, we need to rely on the LeaderEpochCheckpointCache
 * to truncate from start and end, to get the epoch info. However, we don't really want to truncate the epochs in cache
 * (and write to checkpoint file in the end). So, we introduce this InMemoryLeaderEpochCheckpoint to feed into LeaderEpochCheckpointCache,
 * and when we truncate the epoch for RLSM, we can do them in memory without affecting the checkpoint file, and without interacting with file system.
 */
public class InMemoryLeaderEpochCheckpoint implements LeaderEpochCheckpoint {
    private List<EpochEntry> epochs = Collections.emptyList();

    public void write(Collection<EpochEntry> epochs, boolean ignored) {
        this.epochs = new ArrayList<>(epochs);
    }

    public List<EpochEntry> read() {
        return Collections.unmodifiableList(epochs);
    }

    public ByteBuffer readAsByteBuffer() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));) {
            CheckpointFile.CheckpointWriteBuffer<EpochEntry> writeBuffer = new CheckpointFile.CheckpointWriteBuffer<>(writer, 0, LeaderEpochCheckpointFile.FORMATTER);
            writeBuffer.write(epochs);
            writer.flush();
        }

        return ByteBuffer.wrap(stream.toByteArray());
    }
}
