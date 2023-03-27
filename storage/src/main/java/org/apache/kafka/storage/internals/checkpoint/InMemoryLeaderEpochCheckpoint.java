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
import java.util.List;

/**
 * This class stores a list of EpochEntry(LeaderEpoch + Offsets) to memory
 */
public class InMemoryLeaderEpochCheckpoint implements LeaderEpochCheckpoint {
    private List<EpochEntry> epochs = new ArrayList<>();

    public void write(Collection<EpochEntry> epochs) {
        this.epochs.addAll(epochs);
    }

    public List<EpochEntry> read() {
        return epochs;
    }

    public ByteBuffer readAsByteBuffer() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
        CheckpointFile.CheckpointWriteBuffer<EpochEntry> writeBuffer = new CheckpointFile.CheckpointWriteBuffer<>(writer, 0, LeaderEpochCheckpointFile.FORMATTER);
        try {
            writeBuffer.write(epochs);
            writer.flush();
            return ByteBuffer.wrap(stream.toByteArray());
        } finally {
            writer.close();
        }
    }
}