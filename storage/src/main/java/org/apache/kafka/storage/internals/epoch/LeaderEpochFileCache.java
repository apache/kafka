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
package org.apache.kafka.storage.internals.epoch;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;

import org.slf4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * Represents a cache of (LeaderEpoch => Offset) mappings for a particular replica.
 * <p>
 * Leader Epoch = epoch assigned to each leader by the controller.
 * Offset = offset of the first message in each epoch.
 * <p>
 * Note that {@link #truncateFromStartAsyncFlush},{@link #truncateFromEndAsyncFlush} flush the epoch-entry changes to checkpoint asynchronously.
 * Hence, it is instantiater's responsibility to ensure restoring the cache to the correct state after instantiating
 * this class from checkpoint (which might contain stale epoch entries right after instantiation).
 */
public final class LeaderEpochFileCache {
    private final TopicPartition topicPartition;
    private final LeaderEpochCheckpointFile checkpoint;
    private final Scheduler scheduler;
    private final Logger log;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final TreeMap<Integer, EpochEntry> epochs = new TreeMap<>();


    /**
     * @param topicPartition the associated topic partition
     * @param checkpoint     the checkpoint file
     * @param scheduler      the scheduler to use for async I/O operations
     */
    public LeaderEpochFileCache(TopicPartition topicPartition, LeaderEpochCheckpointFile checkpoint, Scheduler scheduler) {
        this.checkpoint = checkpoint;
        this.topicPartition = topicPartition;
        this.scheduler = scheduler;
        LogContext logContext = new LogContext("[LeaderEpochCache " + topicPartition + "] ");
        log = logContext.logger(LeaderEpochFileCache.class);
        checkpoint.read().forEach(this::assign);
    }

    /**
     * Instantiate a new LeaderEpochFileCache with provided epoch entries instead of from the backing checkpoint file.
     * The provided epoch entries are expected to be no less fresh than the checkpoint file.
     * @param epochEntries the current epoch entries
     * @param topicPartition the associated topic partition
     * @param checkpoint the checkpoint file
     * @param scheduler the scheduler to use for async I/O operations
     */
    private LeaderEpochFileCache(List<EpochEntry> epochEntries,
                                 TopicPartition topicPartition,
                                 LeaderEpochCheckpointFile checkpoint,
                                 Scheduler scheduler) {
        this.checkpoint = checkpoint;
        this.topicPartition = topicPartition;
        this.scheduler = scheduler;
        LogContext logContext = new LogContext("[LeaderEpochCache " + topicPartition + "] ");
        log = logContext.logger(LeaderEpochFileCache.class);
        for (EpochEntry entry : epochEntries) {
            epochs.put(entry.epoch, entry);
        }
    }

    /**
     * Assigns the supplied Leader Epoch to the supplied Offset
     * Once the epoch is assigned it cannot be reassigned
     */
    public void assign(int epoch, long startOffset) {
        EpochEntry entry = new EpochEntry(epoch, startOffset);
        if (assign(entry)) {
            log.debug("Appended new epoch entry {}. Cache now contains {} entries.", entry, epochs.size());
            writeToFile();
        }
    }

    public void assign(List<EpochEntry> entries) {
        entries.forEach(entry -> {
            if (assign(entry)) {
                log.debug("Appended new epoch entry {}. Cache now contains {} entries.", entry, epochs.size());
            }
        });
        if (!entries.isEmpty()) writeToFile();
    }

    private boolean isUpdateNeeded(EpochEntry entry) {
        return latestEntry().map(epochEntry -> entry.epoch != epochEntry.epoch || entry.startOffset < epochEntry.startOffset).orElse(true);
    }

    private boolean assign(EpochEntry entry) {
        if (entry.epoch < 0 || entry.startOffset < 0) {
            throw new IllegalArgumentException("Received invalid partition leader epoch entry " + entry);
        }

        // Check whether the append is needed before acquiring the write lock
        // in order to avoid contention with readers in the common case
        if (!isUpdateNeeded(entry)) return false;

        lock.writeLock().lock();
        try {
            if (isUpdateNeeded(entry)) {
                maybeTruncateNonMonotonicEntries(entry);
                epochs.put(entry.epoch, entry);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove any entries which violate monotonicity prior to appending a new entry
     */
    private void maybeTruncateNonMonotonicEntries(EpochEntry newEntry) {
        List<EpochEntry> removedEpochs = removeWhileMatching(
                epochs.descendingMap().entrySet().iterator(),
                entry -> entry.epoch >= newEntry.epoch || entry.startOffset >= newEntry.startOffset);

        if (removedEpochs.size() > 1 || (!removedEpochs.isEmpty() && removedEpochs.get(0).startOffset != newEntry.startOffset)) {

            // Only log a warning if there were non-trivial removals. If the start offset of the new entry
            // matches the start offset of the removed epoch, then no data has been written and the truncation
            // is expected.
            log.warn("New epoch entry {} caused truncation of conflicting entries {}. " + "Cache now contains {} entries.", newEntry, removedEpochs, epochs.size());
        }
    }

    private static List<EpochEntry> removeWhileMatching(Iterator<Map.Entry<Integer, EpochEntry>> iterator, Predicate<EpochEntry> predicate) {
        ArrayList<EpochEntry> removedEpochs = new ArrayList<>();

        while (iterator.hasNext()) {
            EpochEntry entry = iterator.next().getValue();
            if (predicate.test(entry)) {
                removedEpochs.add(entry);
                iterator.remove();
            } else {
                return removedEpochs;
            }
        }

        return removedEpochs;
    }

    public boolean nonEmpty() {
        lock.readLock().lock();
        try {
            return !epochs.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<EpochEntry> latestEntry() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(epochs.lastEntry()).map(Map.Entry::getValue);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the current Leader Epoch if one exists. This is the latest epoch
     * which has messages assigned to it.
     */
    public OptionalInt latestEpoch() {
        return latestEntry().map(epochEntry -> OptionalInt.of(epochEntry.epoch)).orElseGet(OptionalInt::empty);
    }

    public OptionalInt previousEpoch() {
        lock.readLock().lock();
        try {
            return latestEntry().flatMap(entry -> Optional.ofNullable(epochs.lowerEntry(entry.epoch)))
                    .map(integerEpochEntryEntry -> OptionalInt.of(integerEpochEntryEntry.getKey())).orElseGet(OptionalInt::empty);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the earliest cached entry if one exists.
     */
    public Optional<EpochEntry> earliestEntry() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(epochs.firstEntry()).map(Map.Entry::getValue);
        } finally {
            lock.readLock().unlock();
        }
    }

    public OptionalInt previousEpoch(int epoch) {
        lock.readLock().lock();
        try {
            return toOptionalInt(epochs.lowerKey(epoch));
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<EpochEntry> previousEntry(int epoch) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(epochs.lowerEntry(epoch)).map(Map.Entry::getValue);
        } finally {
            lock.readLock().unlock();
        }
    }

    public OptionalInt nextEpoch(int epoch) {
        lock.readLock().lock();
        try {
            return toOptionalInt(epochs.higherKey(epoch));
        } finally {
            lock.readLock().unlock();
        }
    }

    private static OptionalInt toOptionalInt(Integer value) {
        return (value != null) ? OptionalInt.of(value) : OptionalInt.empty();
    }

    public Optional<EpochEntry> epochEntry(int epoch) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(epochs.get(epoch));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the Leader Epoch and the End Offset for a requested Leader Epoch.
     * <p>
     * The Leader Epoch returned is the largest epoch less than or equal to the requested Leader
     * Epoch. The End Offset is the end offset of this epoch, which is defined as the start offset
     * of the first Leader Epoch larger than the Leader Epoch requested, or else the Log End
     * Offset if the latest epoch was requested.
     * <p>
     * During the upgrade phase, where there are existing messages may not have a leader epoch,
     * if requestedEpoch is < the first epoch cached, UNDEFINED_EPOCH_OFFSET will be returned
     * so that the follower falls back to High Water Mark.
     *
     * @param requestedEpoch requested leader epoch
     * @param logEndOffset   the existing Log End Offset
     * @return found leader epoch and end offset
     */
    public Map.Entry<Integer, Long> endOffsetFor(int requestedEpoch, long logEndOffset) {
        lock.readLock().lock();
        try {
            Map.Entry<Integer, Long> epochAndOffset;
            if (requestedEpoch == UNDEFINED_EPOCH) {
                // This may happen if a bootstrapping follower sends a request with undefined epoch or
                // a follower is on the older message format where leader epochs are not recorded
                epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
            } else if (latestEpoch().isPresent() && latestEpoch().getAsInt() == requestedEpoch) {
                // For the leader, the latest epoch is always the current leader epoch that is still being written to.
                // Followers should not have any reason to query for the end offset of the current epoch, but a consumer
                // might if it is verifying its committed offset following a group rebalance. In this case, we return
                // the current log end offset which makes the truncation check work as expected.
                epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(requestedEpoch, logEndOffset);
            } else {
                Map.Entry<Integer, EpochEntry> higherEntry = epochs.higherEntry(requestedEpoch);
                if (higherEntry == null) {
                    // The requested epoch is larger than any known epoch. This case should never be hit because
                    // the latest cached epoch is always the largest.
                    epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
                } else {
                    Map.Entry<Integer, EpochEntry> floorEntry = epochs.floorEntry(requestedEpoch);
                    if (floorEntry == null) {
                        // The requested epoch is smaller than any known epoch, so we return the start offset of the first
                        // known epoch which is larger than it. This may be inaccurate as there could have been
                        // epochs in between, but the point is that the data has already been removed from the log
                        // and we want to ensure that the follower can replicate correctly beginning from the leader's
                        // start offset.
                        epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(requestedEpoch, higherEntry.getValue().startOffset);
                    } else {
                        // We have at least one previous epoch and one subsequent epoch. The result is the first
                        // prior epoch and the starting offset of the first subsequent epoch.
                        epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(floorEntry.getValue().epoch, higherEntry.getValue().startOffset);
                    }
                }
            }

            if (log.isTraceEnabled())
                log.trace("Processed end offset request for epoch {} and returning epoch {} with end offset {} from epoch cache of size {}}", requestedEpoch, epochAndOffset.getKey(), epochAndOffset.getValue(), epochs.size());

            return epochAndOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Removes all epoch entries from the store with start offsets greater than or equal to the passed offset.
     * <p>
     * Checkpoint-flushing is done asynchronously.
     */
    public void truncateFromEndAsyncFlush(long endOffset) {
        lock.writeLock().lock();
        try {
            List<EpochEntry> removedEntries = truncateFromEnd(epochs, endOffset);
            if (!removedEntries.isEmpty()) {
                // We flush the change to the device in the background because:
                // - To avoid fsync latency
                //   * fsync latency could be huge on a disk glitch, which is not rare in spinning drives
                //   * This method is called by ReplicaFetcher threads, which could block replica fetching
                //     then causing ISR shrink or high produce response time degradation in remote scope on high fsync latency.
                // - We still flush the change in #assign synchronously, meaning that it's guaranteed that the checkpoint file always has no missing entries.
                //   * Even when stale epochs are restored from the checkpoint file after the unclean shutdown, it will be handled by
                //     another truncateFromEnd call on log loading procedure, so it won't be a problem
                scheduler.scheduleOnce("leader-epoch-cache-flush-" + topicPartition, this::writeIfDirExists);

                log.debug("Cleared entries {} from epoch cache after truncating to end offset {}, leaving {} entries in the cache.", removedEntries, endOffset, epochs.size());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
     * be offset, then clears any previous epoch entries.
     * <p>
     * This method is exclusive: so truncateFromStart(6) will retain an entry at offset 6.
     * <p>
     * Checkpoint-flushing is done asynchronously.
     *
     * @param startOffset the offset to clear up to
     */
    public void truncateFromStartAsyncFlush(long startOffset) {
        lock.writeLock().lock();
        try {
            List<EpochEntry> removedEntries = truncateFromStart(epochs, startOffset);
            if (!removedEntries.isEmpty()) {
                // We flush the change to the device in the background because:
                // - To avoid fsync latency
                //   * fsync latency could be huge on a disk glitch, which is not rare in spinning drives
                //   * This method is called as part of deleteRecords with holding UnifiedLog#lock.
                //      - Meanwhile all produces against the partition will be blocked, which causes req-handlers to exhaust
                // - We still flush the change in #assign synchronously, meaning that it's guaranteed that the checkpoint file always has no missing entries.
                //   * Even when stale epochs are restored from the checkpoint file after the unclean shutdown, it will be handled by
                //     another truncateFromStart call on log loading procedure, so it won't be a problem
                scheduler.scheduleOnce("leader-epoch-cache-flush-" + topicPartition, this::writeIfDirExists);

                EpochEntry updatedFirstEntry = removedEntries.get(removedEntries.size() - 1);
                log.debug("Cleared entries {} and rewrote first entry {} after truncating to start offset {}, leaving {} in the cache.", removedEntries, updatedFirstEntry, startOffset, epochs.size());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static List<EpochEntry> truncateFromStart(TreeMap<Integer, EpochEntry> epochs, long startOffset) {
        List<EpochEntry> removedEntries = removeWhileMatching(
                epochs.entrySet().iterator(), entry -> entry.startOffset <= startOffset);

        if (!removedEntries.isEmpty()) {
            EpochEntry firstBeforeStartOffset = removedEntries.get(removedEntries.size() - 1);
            EpochEntry updatedFirstEntry = new EpochEntry(firstBeforeStartOffset.epoch, startOffset);
            epochs.put(updatedFirstEntry.epoch, updatedFirstEntry);
        }

        return removedEntries;
    }

    private static List<EpochEntry> truncateFromEnd(TreeMap<Integer, EpochEntry> epochs, long endOffset) {
        Optional<EpochEntry> epochEntry = Optional.ofNullable(epochs.lastEntry()).map(Entry::getValue);
        if (endOffset >= 0 && epochEntry.isPresent() && epochEntry.get().startOffset >= endOffset) {
            return removeWhileMatching(epochs.descendingMap().entrySet().iterator(), x -> x.startOffset >= endOffset);
        }
        return Collections.emptyList();
    }

    public OptionalInt epochForOffset(long offset) {
        lock.readLock().lock();
        try {
            OptionalInt previousEpoch = OptionalInt.empty();
            for (EpochEntry epochEntry : epochs.values()) {
                int epoch = epochEntry.epoch;
                long startOffset = epochEntry.startOffset;

                // Found the exact offset, return the respective epoch.
                if (startOffset == offset) return OptionalInt.of(epoch);

                // Return the previous found epoch as this epoch's start-offset is more than the target offset.
                if (startOffset > offset) return previousEpoch;

                previousEpoch = OptionalInt.of(epoch);
            }

            return previousEpoch;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a new LeaderEpochFileCache which contains same
     * epoch entries with replacing backing checkpoint file.
     * @param leaderEpochCheckpoint the new checkpoint file
     * @return a new LeaderEpochFileCache instance
     */
    public LeaderEpochFileCache withCheckpoint(LeaderEpochCheckpointFile leaderEpochCheckpoint) {
        lock.readLock().lock();
        try {
            return new LeaderEpochFileCache(epochEntries(),
                                            topicPartition,
                                            leaderEpochCheckpoint,
                                            scheduler);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the leader epoch entries within the range of the given start and end offset
     * @param startOffset The start offset of the epoch entries (inclusive).
     * @param endOffset   The end offset of the epoch entries (exclusive)
     * @return the leader epoch entries
     */
    public List<EpochEntry> epochEntriesInRange(long startOffset, long endOffset) {
        lock.readLock().lock();
        try {
            TreeMap<Integer, EpochEntry> epochsCopy = new TreeMap<>(this.epochs);
            if (startOffset >= 0) {
                truncateFromStart(epochsCopy, startOffset);
            }
            truncateFromEnd(epochsCopy, endOffset);
            return new ArrayList<>(epochsCopy.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Delete all entries.
     */
    public void clearAndFlush() {
        lock.writeLock().lock();
        try {
            epochs.clear();
            writeToFile();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            epochs.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<EpochEntry> epochEntries() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(epochs.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    public NavigableMap<Integer, Long> epochWithOffsets() {
        lock.readLock().lock();
        try {
            NavigableMap<Integer, Long> epochWithOffsets = new TreeMap<>();
            for (EpochEntry epochEntry : epochs.values()) {
                epochWithOffsets.put(epochEntry.epoch, epochEntry.startOffset);
            }
            return epochWithOffsets;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeToFile() {
        lock.readLock().lock();
        try {
            checkpoint.write(epochs.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    private void writeIfDirExists() {
        lock.readLock().lock();
        try {
            // If we take a snapshot of the epoch entries here and flush them to disk outside the read lock,
            // by the time of flushing, the leader epoch file may already be updated with newer epoch entries.
            // Those newer entries will then be overridden with the old snapshot.
            checkpoint.writeIfDirExists(epochs.values());
        } finally {
            lock.readLock().unlock();
        }
    }
}
