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
package org.apache.kafka.raft;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Collections.singletonList;

public class ReplicatedCounter implements RaftClient.Listener<Integer> {
    private final int nodeId;
    private final Logger log;
    private final RaftClient<Integer> client;
    private final int snapshotDelayInRecords = 10;

    private int committed = 0;
    private int uncommitted = 0;
    private OptionalInt claimedEpoch = OptionalInt.empty();
    private long lastOffsetSnapshotted = -1;

    private int handleSnapshotCalls = 0;

    public ReplicatedCounter(
        int nodeId,
        RaftClient<Integer> client,
        LogContext logContext
    ) {
        this.nodeId = nodeId;
        this.client = client;
        log = logContext.logger(ReplicatedCounter.class);
    }

    public synchronized boolean isWritable() {
        return claimedEpoch.isPresent();
    }

    public synchronized void increment() {
        if (!claimedEpoch.isPresent()) {
            throw new KafkaException("Counter is not currently writable");
        }

        int epoch = claimedEpoch.getAsInt();
        uncommitted += 1;
        try {
            long offset = client.scheduleAppend(epoch, singletonList(uncommitted));
            log.debug("Scheduled append of record {} with epoch {} at offset {}",
                uncommitted, epoch, offset);
        } catch (NotLeaderException e) {
            log.info("Appending failed, transition to resigned", e);
            client.resign(epoch);
        }
    }

    @Override
    public synchronized void handleCommit(BatchReader<Integer> reader) {
        try {
            int initialCommitted = committed;
            long lastCommittedOffset = -1;
            int lastCommittedEpoch = 0;
            long lastCommittedTimestamp = -1;

            while (reader.hasNext()) {
                Batch<Integer> batch = reader.next();
                log.debug(
                    "Handle commit of batch with records {} at base offset {}",
                    batch.records(),
                    batch.baseOffset()
                );
                for (Integer nextCommitted: batch.records()) {
                    if (nextCommitted != committed + 1) {
                        throw new AssertionError(
                            String.format(
                                "Expected next committed value to be %s, but instead found %s on node %s",
                                committed + 1,
                                nextCommitted,
                                nodeId
                            )
                        );
                    }
                    committed = nextCommitted;
                }

                lastCommittedOffset = batch.lastOffset();
                lastCommittedEpoch = batch.epoch();
                lastCommittedTimestamp = batch.appendTimestamp();
            }
            log.debug("Counter incremented from {} to {}", initialCommitted, committed);

            if (lastOffsetSnapshotted + snapshotDelayInRecords < lastCommittedOffset) {
                log.debug(
                    "Generating new snapshot with committed offset {} and epoch {} since the previoud snapshot includes {}",
                    lastCommittedOffset,
                    lastCommittedEpoch,
                    lastOffsetSnapshotted
                );
                Optional<SnapshotWriter<Integer>> snapshot = client.createSnapshot(
                    lastCommittedOffset,
                    lastCommittedEpoch,
                    lastCommittedTimestamp);
                if (snapshot.isPresent()) {
                    try {
                        snapshot.get().append(singletonList(committed));
                        snapshot.get().freeze();
                        lastOffsetSnapshotted = lastCommittedOffset;
                    } finally {
                        snapshot.get().close();
                    }
                } else {
                    lastOffsetSnapshotted = lastCommittedOffset;
                }
            }
        } finally {
            reader.close();
        }
    }

    @Override
    public synchronized void handleSnapshot(SnapshotReader<Integer> reader) {
        try {
            log.debug("Loading snapshot {}", reader.snapshotId());
            while (reader.hasNext()) {
                Batch<Integer> batch = reader.next();
                if (batch.records().size() != 1) {
                    throw new AssertionError(
                        String.format(
                            "Expected the snapshot at %s to only contain one record %s",
                            reader.snapshotId(),
                            batch.records()
                        )
                    );
                }

                for (Integer value : batch) {
                    log.debug("Setting value: {}", value);
                    committed = value;
                    uncommitted = value;
                }
            }
            lastOffsetSnapshotted = reader.lastContainedLogOffset();
            handleSnapshotCalls += 1;
            log.debug("Finished loading snapshot. Set value: {}", committed);
        } finally {
            reader.close();
        }
    }

    @Override
    public synchronized void handleLeaderChange(LeaderAndEpoch newLeader) {
        if (newLeader.isLeader(nodeId)) {
            log.debug("Counter uncommitted value initialized to {} after claiming leadership in epoch {}",
                committed, newLeader);
            uncommitted = committed;
            claimedEpoch = OptionalInt.of(newLeader.epoch());
        } else {
            log.debug("Counter uncommitted value reset after resigning leadership");
            uncommitted = -1;
            claimedEpoch = OptionalInt.empty();
        }
        handleSnapshotCalls = 0;
    }

    /** Use handleSnapshotCalls to verify leader is never asked to load snapshot */
    public int handleSnapshotCalls() {
        return handleSnapshotCalls;
    }
}
