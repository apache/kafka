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

import java.util.Optional;
import java.util.OptionalLong;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.ReplicatedLog;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;

// TODO: File an issue to remove the RecordSerde. The internal listener should just skip data record batches
// TODO: Document this class and methods
// TODO: Add unnitest for it
final public class InternalLogListener {
    private final ReplicatedLog log;
    private final RecordSerde<?> serde;
    private final BufferSupplier bufferSupplier;
    private final Logger logger;
    private final int maxBatchSizeBytes;

    // These are objects are synchronized using the perspective object monitor. The two actors
    // are the KRaft driver and the RaftClient callers
    private final VoterSetHistory voterSetHistory;
    private final History<Short> kraftVersion = new TreeMapHistory<>();

    // This synchronization is enough because
    // 1. The write operation updateListener only sets the value without reading and updates to
    // voterSetHistory or kraftVersion are done before setting the nextOffset
    //
    // 2. The read operations lastVoterSet, voterSetAtOffset and kraftVersionAtOffset read
    // the nextOffset first before reading voterSetHistory or kraftVersion
    private volatile long nextOffset = 0;

    public InternalLogListener(
        Optional<VoterSet> staticVoterSet,
        ReplicatedLog log,
        RecordSerde<?> serde,
        BufferSupplier bufferSupplier,
        int maxBatchSizeBytes,
        LogContext logContext
    ) {
        this.log = log;
        this.voterSetHistory = new VoterSetHistory(staticVoterSet);
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.logger = logContext.logger(this.getClass());
    }

    public void updateListener() {
        maybeLoadSnapshot();
        maybeLoadLog();
    }

    public VoterSet lastVoterSet() {
        synchronized (voterSetHistory) {
            return voterSetHistory.lastValue();
        }
    }

    public Optional<VoterSet> voterSetAtOffset(long offset) {
        long fixedNextOffset = nextOffset;
        if (offset >= fixedNextOffset) {
            throw new IllegalArgumentException(
                String.format(
                    "Attempting the read the voter set at an offset (%d) which kraft hasn't seen (%d)",
                    offset,
                    fixedNextOffset - 1
                )
            );
        }

        synchronized (voterSetHistory) {
            return voterSetHistory.valueAt(offset);
        }
    }

    public short kraftVersionAtOffset(long offset) {
        long fixedNextOffset = nextOffset;
        if (offset >= fixedNextOffset) {
            throw new IllegalArgumentException(
                String.format(
                    "Attempting the read the kraft.version at an offset (%d) which kraft hasn't seen (%d)",
                    offset,
                    fixedNextOffset - 1
                )
            );
        }

        synchronized (kraftVersion) {
            return kraftVersion.valueAt(offset).orElse((short) 0);
        }
    }

    private void maybeLoadLog() {
        while (log.endOffset().offset > nextOffset) {
            LogFetchInfo info = log.read(nextOffset, Isolation.UNCOMMITTED);
            try (RecordsIterator<?> iterator = new RecordsIterator<>(
                    info.records,
                    serde,
                    bufferSupplier,
                    maxBatchSizeBytes,
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
        if ((nextOffset == 0 || nextOffset < log.startOffset()) && log.latestSnapshot().isPresent()) {
            RawSnapshotReader rawSnapshot = log.latestSnapshot().get();
            // Clear the current state
            synchronized (kraftVersion) {
                kraftVersion.clear();
            }
            synchronized (voterSetHistory) {
                voterSetHistory.clear();
            }

            // Load the snapshot since the listener is at the start of the log or the log doesn't have the next entry.
            try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                    rawSnapshot,
                    serde,
                    bufferSupplier,
                    maxBatchSizeBytes,
                    true // Validate batch CRC
                )
            ) {
                logger.info(
                    "Loading snapshot ({}) since log start offset ({}) is greater than the internal listener's next offset ({})",
                    reader.snapshotId(),
                    log.startOffset(),
                    nextOffset
                );
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
                    synchronized (voterSetHistory) {
                        voterSetHistory.addAt(currentOffset, VoterSet.fromVotersRecord((VotersRecord) record.message()));
                    }
                    break;

                case KRAFT_VERSION:
                    synchronized (kraftVersion) {
                        kraftVersion.addAt(currentOffset, ((KRaftVersionRecord) record.message()).kRaftVersion());
                    }
                    break;

                default:
                    // Skip the rest of the control records
                    break;
            }
            ++index;
        }
    }
}
