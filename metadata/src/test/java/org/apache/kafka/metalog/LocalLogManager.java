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

package org.apache.kafka.metalog;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.raft.internals.MemoryBatchReader;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.MockRawSnapshotReader;
import org.apache.kafka.snapshot.MockRawSnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


/**
 * The LocalLogManager is a test implementation that relies on the contents of memory.
 */
public final class LocalLogManager implements RaftClient<ApiMessageAndVersion>, AutoCloseable {
    interface LocalBatch {
        int epoch();
        int size();
    }

    static class LeaderChangeBatch implements LocalBatch {
        private final LeaderAndEpoch newLeader;

        LeaderChangeBatch(LeaderAndEpoch newLeader) {
            this.newLeader = newLeader;
        }

        @Override
        public int epoch() {
            return newLeader.epoch();
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LeaderChangeBatch)) return false;
            LeaderChangeBatch other = (LeaderChangeBatch) o;
            if (!other.newLeader.equals(newLeader)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(newLeader);
        }

        @Override
        public String toString() {
            return "LeaderChangeBatch(newLeader=" + newLeader + ")";
        }
    }

    static class LocalRecordBatch implements LocalBatch {
        private final int leaderEpoch;
        private final List<ApiMessageAndVersion> records;

        LocalRecordBatch(int leaderEpoch, List<ApiMessageAndVersion> records) {
            this.leaderEpoch = leaderEpoch;
            this.records = records;
        }

        @Override
        public int epoch() {
            return leaderEpoch;
        }

        @Override
        public int size() {
            return records.size();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LocalRecordBatch)) return false;
            LocalRecordBatch other = (LocalRecordBatch) o;
            if (!other.records.equals(records)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(records);
        }

        @Override
        public String toString() {
            return "LocalRecordBatch(records=" + records + ")";
        }
    }

    public static class SharedLogData {
        private final Logger log = LoggerFactory.getLogger(SharedLogData.class);

        /**
         * Maps node IDs to the matching log managers.
         */
        private final HashMap<Integer, LocalLogManager> logManagers = new HashMap<>();

        /**
         * Maps offsets to record batches.
         */
        private final TreeMap<Long, LocalBatch> batches = new TreeMap<>();

        /**
         * The current leader.
         */
        private LeaderAndEpoch leader = new LeaderAndEpoch(OptionalInt.empty(), 0);

        /**
         * The start offset of the last batch that was created, or -1 if no batches have
         * been created.
         */
        private long prevOffset;

        /**
         * Maps committed offset to snapshot reader.
         */
        private NavigableMap<Long, RawSnapshotReader> snapshots = new TreeMap<>();

        public SharedLogData(Optional<RawSnapshotReader> snapshot) {
            if (snapshot.isPresent()) {
                RawSnapshotReader initialSnapshot = snapshot.get();
                prevOffset = initialSnapshot.snapshotId().offset - 1;
                snapshots.put(prevOffset, initialSnapshot);
            } else {
                prevOffset = -1;
            }
        }

        synchronized void registerLogManager(LocalLogManager logManager) {
            if (logManagers.put(logManager.nodeId, logManager) != null) {
                throw new RuntimeException("Can't have multiple LocalLogManagers " +
                    "with id " + logManager.nodeId());
            }
            electLeaderIfNeeded();
        }

        synchronized void unregisterLogManager(LocalLogManager logManager) {
            if (!logManagers.remove(logManager.nodeId, logManager)) {
                throw new RuntimeException("Log manager " + logManager.nodeId() +
                    " was not found.");
            }
        }

        synchronized long tryAppend(int nodeId, long epoch, LocalBatch batch) {
            if (epoch != leader.epoch()) {
                log.trace("tryAppend(nodeId={}, epoch={}): the provided epoch does not " +
                    "match the current leader epoch of {}.", nodeId, epoch, leader.epoch());
                return Long.MAX_VALUE;
            }
            if (!leader.isLeader(nodeId)) {
                log.trace("tryAppend(nodeId={}, epoch={}): the given node id does not " +
                    "match the current leader id of {}.", nodeId, epoch, leader.leaderId());
                return Long.MAX_VALUE;
            }
            log.trace("tryAppend(nodeId={}): appending {}.", nodeId, batch);
            long offset = append(batch);
            electLeaderIfNeeded();
            return offset;
        }

        synchronized long append(LocalBatch batch) {
            prevOffset += batch.size();
            log.debug("append(batch={}, prevOffset={})", batch, prevOffset);
            batches.put(prevOffset, batch);
            if (batch instanceof LeaderChangeBatch) {
                LeaderChangeBatch leaderChangeBatch = (LeaderChangeBatch) batch;
                leader = leaderChangeBatch.newLeader;
            }
            for (LocalLogManager logManager : logManagers.values()) {
                logManager.scheduleLogCheck();
            }
            return prevOffset;
        }

        synchronized void electLeaderIfNeeded() {
            if (leader.leaderId().isPresent() || logManagers.isEmpty()) {
                return;
            }
            int nextLeaderIndex = ThreadLocalRandom.current().nextInt(logManagers.size());
            Iterator<Integer> iter = logManagers.keySet().iterator();
            Integer nextLeaderNode = null;
            for (int i = 0; i <= nextLeaderIndex; i++) {
                nextLeaderNode = iter.next();
            }
            LeaderAndEpoch newLeader = new LeaderAndEpoch(OptionalInt.of(nextLeaderNode), leader.epoch() + 1);
            log.info("Elected new leader: {}.", newLeader);
            append(new LeaderChangeBatch(newLeader));
        }

        synchronized Entry<Long, LocalBatch> nextBatch(long offset) {
            Entry<Long, LocalBatch> entry = batches.higherEntry(offset);
            if (entry == null) {
                return null;
            }
            return new SimpleImmutableEntry<>(entry.getKey(), entry.getValue());
        }

        /**
         * Optionally return a snapshot reader if the offset if less than the first batch.
         */
        synchronized Optional<RawSnapshotReader> nextSnapshot(long offset) {
            return Optional.ofNullable(snapshots.lastEntry()).flatMap(entry -> {
                if (offset <= entry.getKey()) {
                    return Optional.of(entry.getValue());
                }

                return Optional.empty();
            });
        }

        /**
         * Stores a new snapshot and notifies all threads waiting for a snapshot.
         */
        synchronized void addSnapshot(RawSnapshotReader newSnapshot) {
            if (newSnapshot.snapshotId().offset - 1 > prevOffset) {
                log.error(
                    "Ignored attempt to add a snapshot {} that is greater than the latest offset {}",
                    newSnapshot,
                    prevOffset
                );
            } else {
                snapshots.put(newSnapshot.snapshotId().offset - 1, newSnapshot);
                this.notifyAll();
            }
        }

        /**
         * Returns the snapshot whos last offset is the committed offset.
         *
         * If such snapshot doesn't exists it wait until it does.
         */
        synchronized RawSnapshotReader waitForSnapshot(long committedOffset) throws InterruptedException {
            while (true) {
                RawSnapshotReader reader = snapshots.get(committedOffset);
                if (reader != null) {
                    return reader;
                } else {
                    this.wait();
                }
            }
        }
    }

    private static class MetaLogListenerData {
        private long offset = -1;
        private final RaftClient.Listener<ApiMessageAndVersion> listener;

        MetaLogListenerData(RaftClient.Listener<ApiMessageAndVersion> listener) {
            this.listener = listener;
        }
    }

    private final Logger log;

    /**
     * The node ID of this local log manager. Each log manager must have a unique ID.
     */
    private final int nodeId;

    /**
     * A reference to the in-memory state that unites all the log managers in use.
     */
    private final SharedLogData shared;

    /**
     * The event queue used by this local log manager.
     */
    private final EventQueue eventQueue;

    /**
     * Whether this LocalLogManager has been initialized.
     */
    private boolean initialized = false;

    /**
     * Whether this LocalLogManager has been shut down.
     */
    private boolean shutdown = false;

    /**
     * An offset that the log manager will not read beyond. This exists only for testing
     * purposes.
     */
    private long maxReadOffset = Long.MAX_VALUE;

    /**
     * The listener objects attached to this local log manager.
     */
    private final List<MetaLogListenerData> listeners = new ArrayList<>();

    /**
     * The current leader, as seen by this log manager.
     */
    private volatile LeaderAndEpoch leader = new LeaderAndEpoch(OptionalInt.empty(), 0);

    public LocalLogManager(LogContext logContext,
                           int nodeId,
                           SharedLogData shared,
                           String threadNamePrefix) {
        this.log = logContext.logger(LocalLogManager.class);
        this.nodeId = nodeId;
        this.shared = shared;
        this.eventQueue = new KafkaEventQueue(Time.SYSTEM, logContext, threadNamePrefix);
        shared.registerLogManager(this);
    }

    private void scheduleLogCheck() {
        eventQueue.append(() -> {
            try {
                log.debug("Node {}: running log check.", nodeId);
                int numEntriesFound = 0;
                for (MetaLogListenerData listenerData : listeners) {
                    while (true) {
                        // Load the snapshot if needed
                        Optional<RawSnapshotReader> snapshot = shared.nextSnapshot(listenerData.offset);
                        if (snapshot.isPresent()) {
                            log.trace("Node {}: handling snapshot with id {}.", nodeId, snapshot.get().snapshotId());
                            listenerData.listener.handleSnapshot(
                                SnapshotReader.of(
                                    snapshot.get(),
                                    new  MetadataRecordSerde(),
                                    BufferSupplier.create(),
                                    Integer.MAX_VALUE
                                )
                            );
                            listenerData.offset = snapshot.get().snapshotId().offset - 1;
                        }

                        Entry<Long, LocalBatch> entry = shared.nextBatch(listenerData.offset);
                        if (entry == null) {
                            log.trace("Node {}: reached the end of the log after finding " +
                                "{} entries.", nodeId, numEntriesFound);
                            break;
                        }
                        long entryOffset = entry.getKey();
                        if (entryOffset > maxReadOffset) {
                            log.trace("Node {}: after {} entries, not reading the next " +
                                "entry because its offset is {}, and maxReadOffset is {}.",
                                nodeId, numEntriesFound, entryOffset, maxReadOffset);
                            break;
                        }
                        if (entry.getValue() instanceof LeaderChangeBatch) {
                            LeaderChangeBatch batch = (LeaderChangeBatch) entry.getValue();
                            log.trace("Node {}: handling LeaderChange to {}.",
                                nodeId, batch.newLeader);
                            listenerData.listener.handleLeaderChange(batch.newLeader);
                            if (batch.newLeader.epoch() > leader.epoch()) {
                                leader = batch.newLeader;
                            }
                        } else if (entry.getValue() instanceof LocalRecordBatch) {
                            LocalRecordBatch batch = (LocalRecordBatch) entry.getValue();
                            log.trace("Node {}: handling LocalRecordBatch with offset {}.",
                                nodeId, entryOffset);
                            listenerData.listener.handleCommit(
                                new MemoryBatchReader<>(
                                    Collections.singletonList(
                                        Batch.of(
                                            entryOffset - batch.records.size() + 1,
                                            Math.toIntExact(batch.leaderEpoch),
                                            batch.records
                                        )
                                    ),
                                    reader -> { }
                                )
                            );
                        }
                        numEntriesFound++;
                        listenerData.offset = entryOffset;
                    }
                }
                log.trace("Completed log check for node " + nodeId);
            } catch (Exception e) {
                log.error("Exception while handling log check", e);
            }
        });
    }

    public void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown", () -> {
            try {
                if (initialized && !shutdown) {
                    log.debug("Node {}: beginning shutdown.", nodeId);
                    resign(leader.epoch());
                    for (MetaLogListenerData listenerData : listeners) {
                        listenerData.listener.beginShutdown();
                    }
                    shared.unregisterLogManager(this);
                }
            } catch (Exception e) {
                log.error("Unexpected exception while sending beginShutdown callbacks", e);
            }
            shutdown = true;
        });
    }

    @Override
    public void close() {
        log.debug("Node {}: closing.", nodeId);
        beginShutdown();

        try {
            eventQueue.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * Shutdown the log manager.
     *
     * Even though the API suggests a non-blocking shutdown, this method always returns a completed
     * future. This means that shutdown is a blocking operation.
     */
    @Override
    public CompletableFuture<Void> shutdown(int timeoutMs) {
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        try {
            close();
            shutdownFuture.complete(null);
        } catch (Throwable t) {
            shutdownFuture.completeExceptionally(t);
        }
        return shutdownFuture;
    }

    @Override
    public void initialize() {
        eventQueue.append(() -> {
            log.debug("initialized local log manager for node " + nodeId);
            initialized = true;
        });
    }

    @Override
    public void register(RaftClient.Listener<ApiMessageAndVersion> listener) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.append(() -> {
            if (shutdown) {
                log.info("Node {}: can't register because local log manager has " +
                    "already been shut down.", nodeId);
                future.complete(null);
            } else if (initialized) {
                log.info("Node {}: registered MetaLogListener.", nodeId);
                listeners.add(new MetaLogListenerData(listener));
                shared.electLeaderIfNeeded();
                scheduleLogCheck();
                future.complete(null);
            } else {
                log.info("Node {}: can't register because local log manager has not " +
                    "been initialized.", nodeId);
                future.completeExceptionally(new RuntimeException(
                    "LocalLogManager was not initialized."));
            }
        });
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long scheduleAppend(int epoch, List<ApiMessageAndVersion> batch) {
        return scheduleAtomicAppend(epoch, batch);
    }

    @Override
    public Long scheduleAtomicAppend(int epoch, List<ApiMessageAndVersion> batch) {
        return shared.tryAppend(
            nodeId,
            leader.epoch(),
            new LocalRecordBatch(leader.epoch(), batch)
        );
    }

    @Override
    public void resign(int epoch) {
        LeaderAndEpoch curLeader = leader;
        LeaderAndEpoch nextLeader = new LeaderAndEpoch(OptionalInt.empty(), curLeader.epoch() + 1);
        shared.tryAppend(nodeId, curLeader.epoch(), new LeaderChangeBatch(nextLeader));
    }

    @Override
    public Optional<SnapshotWriter<ApiMessageAndVersion>> createSnapshot(long committedOffset, int committedEpoch) {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(committedOffset + 1, committedEpoch);
        return Optional.of(
            new SnapshotWriter<>(
                new MockRawSnapshotWriter(snapshotId, buffer -> {
                    shared.addSnapshot(new MockRawSnapshotReader(snapshotId, buffer));
                }),
                1024,
                MemoryPool.NONE,
                new MockTime(),
                CompressionType.NONE,
                new MetadataRecordSerde()
            )
        );
    }

    @Override
    public LeaderAndEpoch leaderAndEpoch() {
        return leader;
    }

    @Override
    public OptionalInt nodeId() {
        return OptionalInt.of(nodeId);
    }

    public List<RaftClient.Listener<ApiMessageAndVersion>> listeners() {
        final CompletableFuture<List<RaftClient.Listener<ApiMessageAndVersion>>> future = new CompletableFuture<>();
        eventQueue.append(() -> {
            future.complete(listeners.stream().map(l -> l.listener).collect(Collectors.toList()));
        });
        try {
            return future.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxReadOffset(long maxReadOffset) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        eventQueue.append(() -> {
            log.trace("Node {}: set maxReadOffset to {}.", nodeId, maxReadOffset);
            this.maxReadOffset = maxReadOffset;
            scheduleLogCheck();
            future.complete(null);
        });
        try {
            future.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
