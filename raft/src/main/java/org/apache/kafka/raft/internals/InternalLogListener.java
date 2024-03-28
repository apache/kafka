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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.ReplicatedLog;
import org.apache.kafka.server.common.serialization.RecordSerde;
import java.util.OptionalLong;
import java.util.Optional;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import static org.apache.kafka.raft.KafkaRaftClient.MAX_BATCH_SIZE_BYTES;

// TODO: File an issue to remove the RecordSerde. The internal listener should just skip data record batches
// TODO: Document this class and methods
// TODO: Add unnitest for it
final class InternalLogListener {
    private final ReplicatedLog log;
    private final RecordSerde<?> serde;
    private final BufferSupplier bufferSupplier;

    private final VoterSetHistory voterSetHistory;
    private final History<Short> kraftVersion = new TreeMapHistory<>();

    private long nextOffset = 0;

    InternalLogListener(
        Optional<VoterSet> staticVoterSet,
        ReplicatedLog log,
        RecordSerde<?> serde,
        BufferSupplier bufferSupplier
    ) {
        this.log = log;
        this.voterSetHistory = new VoterSetHistory(staticVoterSet);
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
    }

    void updateListerner() {
        maybeLoadSnapshot();
        maybeLoadLog();
    }

    private void maybeLoadLog() {
        while (log.endOffset().offset > nextOffset) {
            LogFetchInfo info = log.read(nextOffset, Isolation.UNCOMMITTED);
            try (RecordsIterator<?> iterator = new RecordsIterator<>(
                    info.records,
                    serde,
                    bufferSupplier,
                    MAX_BATCH_SIZE_BYTES,
                    true // Validate batch CRC
                )
            ) {
                while (iterator.hasNext()) {
                    Batch<?> batch = iterator.next();
                    handleBatch(batch, OptionalLong.empty());
                    nextOffset = batch.lastOffset() + 1;
                }
            }
        }
    }

    private void maybeLoadSnapshot() {
        Optional<RawSnapshotReader> rawSnapshot = log.latestSnapshot();
        if (rawSnapshot.isPresent() && (nextOffset == 0 || nextOffset > log.startOffset())) {
            // Clear the current state
            kraftVersion.clear();
            voterSetHistory.clear();

            // Load the snapshot since the listener is at the start of the log or the log doesn't have the next entry.
            try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                    rawSnapshot.get(),
                    serde,
                    bufferSupplier,
                    MAX_BATCH_SIZE_BYTES,
                    true // Validate batch CRC
                )
            ) {
                OptionalLong currentOffset = OptionalLong.of(reader.lastContainedLogOffset());
                while (reader.hasNext()) {
                    Batch<?> batch = reader.next();
                    handleBatch(batch, currentOffset);
                }

                nextOffset = reader.lastContainedLogOffset() + 1;
            }
        }
    }

    private void handleBatch(Batch<?> batch, OptionalLong overrideOffset) {
        int index = 0;
        for (ControlRecord record : batch.controlRecords()) {
            long currentOffset = overrideOffset.orElse(batch.baseOffset() + index);
            switch (record.type()) {
                case VOTERS:
                    voterSetHistory.addAt(currentOffset, VoterSet.fromVotersRecord((VotersRecord) record.message()));
                    break;

                case KRAFT_VERSION:
                    kraftVersion.addAt(currentOffset, ((KRaftVersionRecord) record.message()).kRaftVersion());
                    break;

                default:
                    // Skip the rest of the control records
                    break;
            }
            ++index;
        }
    }
}
