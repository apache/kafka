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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maintains a mapping from ProducerIds to metadata about the last appended entries (e.g.
 * epoch, sequence number, last offset, etc.)
 * <p>
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 * <p>
 * As long as a producer id is contained in the map, the corresponding producer can continue to write data.
 * However, producer ids can be expired due to lack of recent use or if the last written entry has been deleted from
 * the log (e.g. if the retention policy is "delete"). For compacted topics, the log cleaner will ensure
 * that the most recent entry from a given producer id is retained in the log provided it hasn't expired due to
 * age. This ensures that producer ids will not be expired until either the max expiration time has been reached,
 * or if the topic also is configured for deletion, the segment containing the last written offset has
 * been deleted.
 */
public class ProducerStateManager {

    public static final long LATE_TRANSACTION_BUFFER_MS = 5 * 60 * 1000;

    private static final short PRODUCER_SNAPSHOT_VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String CRC_FIELD = "crc";
    private static final String PRODUCER_ID_FIELD = "producer_id";
    private static final String LAST_SEQUENCE_FIELD = "last_sequence";
    private static final String PRODUCER_EPOCH_FIELD = "epoch";
    private static final String LAST_OFFSET_FIELD = "last_offset";
    private static final String OFFSET_DELTA_FIELD = "offset_delta";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String PRODUCER_ENTRIES_FIELD = "producer_entries";
    private static final String COORDINATOR_EPOCH_FIELD = "coordinator_epoch";
    private static final String CURRENT_TXN_FIRST_OFFSET_FIELD = "current_txn_first_offset";

    private static final int VERSION_OFFSET = 0;
    private static final int CRC_OFFSET = VERSION_OFFSET + 2;
    private static final int PRODUCER_ENTRIES_OFFSET = CRC_OFFSET + 4;

    private static final Schema PRODUCER_SNAPSHOT_ENTRY_SCHEMA =
            new Schema(new Field(PRODUCER_ID_FIELD, Type.INT64, "The producer ID"),
                    new Field(PRODUCER_EPOCH_FIELD, Type.INT16, "Current epoch of the producer"),
                    new Field(LAST_SEQUENCE_FIELD, Type.INT32, "Last written sequence of the producer"),
                    new Field(LAST_OFFSET_FIELD, Type.INT64, "Last written offset of the producer"),
                    new Field(OFFSET_DELTA_FIELD, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
                    new Field(TIMESTAMP_FIELD, Type.INT64, "Max timestamp from the last written entry"),
                    new Field(COORDINATOR_EPOCH_FIELD, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
                    new Field(CURRENT_TXN_FIRST_OFFSET_FIELD, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"));
    private static final Schema PID_SNAPSHOT_MAP_SCHEMA =
            new Schema(new Field(VERSION_FIELD, Type.INT16, "Version of the snapshot file"),
                    new Field(CRC_FIELD, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
                    new Field(PRODUCER_ENTRIES_FIELD, new ArrayOf(PRODUCER_SNAPSHOT_ENTRY_SCHEMA), "The entries in the producer table"));

    private final Logger log;

    private final TopicPartition topicPartition;
    private final int maxTransactionTimeoutMs;
    private final ProducerStateManagerConfig producerStateManagerConfig;
    private final Time time;

    private final Map<Long, ProducerStateEntry> producers = new HashMap<>();

    private final Map<Long, VerificationStateEntry> verificationStates = new HashMap<>();

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();

    // completed transactions whose markers are at offsets above the high watermark
    private final TreeMap<Long, TxnMetadata> unreplicatedTxns = new TreeMap<>();

    private volatile File logDir;

    // The same as producers.size, but for lock-free access.
    private volatile int producerIdCount = 0;

    // Keep track of the last timestamp from the oldest transaction. This is used
    // to detect (approximately) when a transaction has been left hanging on a partition.
    // We make the field volatile so that it can be safely accessed without a lock.
    private volatile long oldestTxnLastTimestamp = -1L;

    private ConcurrentSkipListMap<Long, SnapshotFile> snapshots;
    private long lastMapOffset = 0L;
    private long lastSnapOffset = 0L;

    public ProducerStateManager(TopicPartition topicPartition, File logDir, int maxTransactionTimeoutMs, ProducerStateManagerConfig producerStateManagerConfig, Time time) throws IOException {
        this.topicPartition = topicPartition;
        this.logDir = logDir;
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        this.producerStateManagerConfig = producerStateManagerConfig;
        this.time = time;
        log = new LogContext("[ProducerStateManager partition=" + topicPartition + "] ").logger(ProducerStateManager.class);
        snapshots = loadSnapshots();
    }

    public int maxTransactionTimeoutMs() {
        return maxTransactionTimeoutMs;
    }

    public ProducerStateManagerConfig producerStateManagerConfig() {
        return producerStateManagerConfig;
    }

    /**
     * This method checks whether there is a late transaction in a thread safe manner.
     */
    public boolean hasLateTransaction(long currentTimeMs) {
        long lastTimestamp = oldestTxnLastTimestamp;
        return lastTimestamp > 0 && (currentTimeMs - lastTimestamp) > maxTransactionTimeoutMs + ProducerStateManager.LATE_TRANSACTION_BUFFER_MS;
    }

    public void truncateFullyAndReloadSnapshots() throws IOException {
        log.info("Reloading the producer state snapshots");
        truncateFullyAndStartAt(0L);
        snapshots = loadSnapshots();
    }

    public int producerIdCount() {
        return producerIdCount;
    }

    private void addProducerId(long producerId, ProducerStateEntry entry) {
        producers.put(producerId, entry);
        producerIdCount = producers.size();
    }

    private void removeProducerIds(List<Long> keys) {
        keys.forEach(producers::remove);
        producerIdCount = producers.size();
    }

    private void clearProducerIds() {
        producers.clear();
        producerIdCount = 0;
    }

    /**
     * Maybe create the VerificationStateEntry for a given producer ID and return it.
     * This method also updates the sequence and epoch accordingly.
     */
    public VerificationStateEntry maybeCreateVerificationStateEntry(long producerId, int sequence, short epoch) {
        VerificationStateEntry entry = verificationStates.computeIfAbsent(producerId, pid ->
            new VerificationStateEntry(time.milliseconds(), sequence, epoch)
        );
        entry.maybeUpdateLowestSequenceAndEpoch(sequence, epoch);
        return entry;
    }

    /**
     * Return the VerificationStateEntry for the producer ID if it exists, otherwise return null.
     */
    public VerificationStateEntry verificationStateEntry(long producerId) {
        return verificationStates.get(producerId);
    }

    /**
     * Clear the verificationStateEntry for the given producer ID.
     */
    public void clearVerificationStateEntry(long producerId) {
        verificationStates.remove(producerId);
    }

    /**
     * Load producer state snapshots by scanning the logDir.
     */
    private ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        ConcurrentSkipListMap<Long, SnapshotFile> offsetToSnapshots = new ConcurrentSkipListMap<>();
        List<SnapshotFile> snapshotFiles = listSnapshotFiles(logDir);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            offsetToSnapshots.put(snapshotFile.offset, snapshotFile);
        }
        return offsetToSnapshots;
    }

    /**
     * Scans the log directory, gathering all producer state snapshot files. Snapshot files which do not have an offset
     * corresponding to one of the provided offsets in segmentBaseOffsets will be removed, except in the case that there
     * is a snapshot file at a higher offset than any offset in segmentBaseOffsets.
     * <p>
     * The goal here is to remove any snapshot files which do not have an associated segment file, but not to remove the
     * largest stray snapshot file which was emitted during clean shutdown.
     */
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        OptionalLong maxSegmentBaseOffset = segmentBaseOffsets.isEmpty() ? OptionalLong.empty() : OptionalLong.of(segmentBaseOffsets.stream().max(Long::compare).get());

        HashSet<Long> baseOffsets = new HashSet<>(segmentBaseOffsets);
        Optional<SnapshotFile> latestStraySnapshot = Optional.empty();

        ConcurrentSkipListMap<Long, SnapshotFile> snapshots = loadSnapshots();
        for (SnapshotFile snapshot : snapshots.values()) {
            long key = snapshot.offset;
            if (latestStraySnapshot.isPresent()) {
                SnapshotFile prev = latestStraySnapshot.get();
                if (!baseOffsets.contains(key)) {
                    // this snapshot is now the largest stray snapshot.
                    prev.deleteIfExists();
                    snapshots.remove(prev.offset);
                    latestStraySnapshot = Optional.of(snapshot);
                }
            } else {
                if (!baseOffsets.contains(key)) {
                    latestStraySnapshot = Optional.of(snapshot);
                }
            }
        }

        // Check to see if the latestStraySnapshot is larger than the largest segment base offset, if it is not,
        // delete the largestStraySnapshot.
        if (latestStraySnapshot.isPresent() && maxSegmentBaseOffset.isPresent()) {
            long strayOffset = latestStraySnapshot.get().offset;
            long maxOffset = maxSegmentBaseOffset.getAsLong();
            if (strayOffset < maxOffset) {
                SnapshotFile removedSnapshot = snapshots.remove(strayOffset);
                if (removedSnapshot != null) {
                    removedSnapshot.deleteIfExists();
                }
            }
        }

        this.snapshots = snapshots;
    }

    /**
     * An unstable offset is one which is either undecided (i.e. its ultimate outcome is not yet known),
     * or one that is decided, but may not have been replicated (i.e. any transaction which has a COMMIT/ABORT
     * marker written at a higher offset than the current high watermark).
     */
    public Optional<LogOffsetMetadata> firstUnstableOffset() {
        Optional<LogOffsetMetadata> unreplicatedFirstOffset = Optional.ofNullable(unreplicatedTxns.firstEntry()).map(e -> e.getValue().firstOffset);
        Optional<LogOffsetMetadata> undecidedFirstOffset = Optional.ofNullable(ongoingTxns.firstEntry()).map(e -> e.getValue().firstOffset);

        if (!unreplicatedFirstOffset.isPresent())
            return undecidedFirstOffset;
        else if (!undecidedFirstOffset.isPresent())
            return unreplicatedFirstOffset;
        else if (undecidedFirstOffset.get().messageOffset < unreplicatedFirstOffset.get().messageOffset)
            return undecidedFirstOffset;
        else
            return unreplicatedFirstOffset;
    }

    /**
     * Acknowledge all transactions which have been completed before a given offset. This allows the LSO
     * to advance to the next unstable offset.
     */
    public void onHighWatermarkUpdated(long highWatermark) {
        removeUnreplicatedTransactions(highWatermark);
    }

    /**
     * The first undecided offset is the earliest transactional message which has not yet been committed
     * or aborted. Unlike [[firstUnstableOffset]], this does not reflect the state of replication (i.e.
     * whether a completed transaction marker is beyond the high watermark).
     */
    public OptionalLong firstUndecidedOffset() {
        Map.Entry<Long, TxnMetadata> firstEntry = ongoingTxns.firstEntry();
        return firstEntry != null ? OptionalLong.of(firstEntry.getValue().firstOffset.messageOffset) : OptionalLong.empty();
    }

    /**
     * Returns the last offset of this map
     */
    public long mapEndOffset() {
        return lastMapOffset;
    }

    /**
     * Get an unmodifiable map of active producers.
     */
    public Map<Long, ProducerStateEntry> activeProducers() {
        return Collections.unmodifiableMap(producers);
    }

    public boolean isEmpty() {
        return producers.isEmpty() && unreplicatedTxns.isEmpty();
    }

    private void loadFromSnapshot(long logStartOffset, long currentTime) throws IOException {
        while (true) {
            Optional<SnapshotFile> latestSnapshotFileOptional = latestSnapshotFile();
            if (latestSnapshotFileOptional.isPresent()) {
                SnapshotFile snapshot = latestSnapshotFileOptional.get();
                try {
                    log.info("Loading producer state from snapshot file '{}'", snapshot);
                    Stream<ProducerStateEntry> loadedProducers = readSnapshot(snapshot.file()).stream().filter(producerEntry -> !isProducerExpired(currentTime, producerEntry));
                    loadedProducers.forEach(this::loadProducerEntry);
                    lastSnapOffset = snapshot.offset;
                    lastMapOffset = lastSnapOffset;
                    updateOldestTxnTimestamp();
                    return;
                } catch (CorruptSnapshotException e) {
                    log.warn("Failed to load producer snapshot from '{}': {}", snapshot.file(), e.getMessage());
                    removeAndDeleteSnapshot(snapshot.offset);
                }
            } else {
                lastSnapOffset = logStartOffset;
                lastMapOffset = logStartOffset;
                return;

            }
        }
    }

    // Visible for testing
    public void loadProducerEntry(ProducerStateEntry entry) {
        long producerId = entry.producerId();
        addProducerId(producerId, entry);
        entry.currentTxnFirstOffset().ifPresent(offset -> ongoingTxns.put(offset, new TxnMetadata(producerId, offset)));
    }

    private boolean isProducerExpired(long currentTimeMs, ProducerStateEntry producerState) {
        return !producerState.currentTxnFirstOffset().isPresent() && currentTimeMs - producerState.lastTimestamp() >= producerStateManagerConfig.producerIdExpirationMs();
    }

    /**
     * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
     * Also expire any verification state entries that are lingering as unverified.
     */
    public void removeExpiredProducers(long currentTimeMs) {
        List<Long> keys = producers.entrySet().stream()
                .filter(entry -> isProducerExpired(currentTimeMs, entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        removeProducerIds(keys);

        List<Long> verificationKeys = verificationStates.entrySet().stream()
                .filter(entry -> currentTimeMs - entry.getValue().timestamp() >= producerStateManagerConfig.producerIdExpirationMs())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        verificationKeys.forEach(verificationStates::remove);
    }

    /**
     * Truncate the producer id mapping to the given offset range and reload the entries from the most recent
     * snapshot in range (if there is one). We delete snapshot files prior to the logStartOffset but do not remove
     * producer state from the map. This means that in-memory and on-disk state can diverge, and in the case of
     * broker failover or unclean shutdown, any in-memory state not persisted in the snapshots will be lost, which
     * would lead to UNKNOWN_PRODUCER_ID errors. Note that the log end offset is assumed to be less than or equal
     * to the high watermark.
     */
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs) throws IOException {
        // remove all out of range snapshots
        for (SnapshotFile snapshot : snapshots.values()) {
            if (snapshot.offset > logEndOffset || snapshot.offset <= logStartOffset) {
                removeAndDeleteSnapshot(snapshot.offset);
            }
        }

        if (logEndOffset != mapEndOffset()) {
            clearProducerIds();
            ongoingTxns.clear();
            updateOldestTxnTimestamp();

            // since we assume that the offset is less than or equal to the high watermark, it is
            // safe to clear the unreplicated transactions
            unreplicatedTxns.clear();
            loadFromSnapshot(logStartOffset, currentTimeMs);
        } else {
            onLogStartOffsetIncremented(logStartOffset);
        }
    }

    public ProducerAppendInfo prepareUpdate(long producerId, AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin, verificationStateEntry(producerId));
    }

    /**
     * Update the mapping with the given append information
     */
    public void update(ProducerAppendInfo appendInfo) {
        if (appendInfo.producerId() == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Invalid producer id " + appendInfo.producerId() + " passed to update "
                    + "for partition" + topicPartition);

        log.trace("Updated producer {} state to {}", appendInfo.producerId(), appendInfo);
        ProducerStateEntry updatedEntry = appendInfo.toEntry();
        ProducerStateEntry currentEntry = producers.get(appendInfo.producerId());
        if (currentEntry != null) {
            currentEntry.update(updatedEntry);
        } else {
            addProducerId(appendInfo.producerId(), updatedEntry);
        }

        appendInfo.startedTransactions().forEach(txn -> ongoingTxns.put(txn.firstOffset.messageOffset, txn));

        updateOldestTxnTimestamp();
    }

    private void updateOldestTxnTimestamp() {
        Map.Entry<Long, TxnMetadata> firstEntry = ongoingTxns.firstEntry();
        if (firstEntry == null) {
            oldestTxnLastTimestamp = -1;
        } else {
            TxnMetadata oldestTxnMetadata = firstEntry.getValue();
            ProducerStateEntry entry = producers.get(oldestTxnMetadata.producerId);
            oldestTxnLastTimestamp = entry != null ? entry.lastTimestamp() : -1L;
        }
    }

    public void updateMapEndOffset(long lastOffset) {
        lastMapOffset = lastOffset;
    }

    /**
     * Get the last written entry for the given producer id.
     */
    public Optional<ProducerStateEntry> lastEntry(long producerId) {
        return Optional.ofNullable(producers.get(producerId));
    }

    /**
     * Take a snapshot at the current end offset if one does not already exist with syncing the change to the device
     */
    public void takeSnapshot() throws IOException {
        takeSnapshot(true);
    }

    /**
     * Take a snapshot at the current end offset if one does not already exist, then return the snapshot file if taken.
     */
    public Optional<File> takeSnapshot(boolean sync) throws IOException {
        // If not a new offset, then it is not worth taking another snapshot
        if (lastMapOffset > lastSnapOffset) {
            SnapshotFile snapshotFile = new SnapshotFile(LogFileUtils.producerSnapshotFile(logDir, lastMapOffset));
            long start = time.hiResClockMs();
            writeSnapshot(snapshotFile.file(), producers, sync);
            log.info("Wrote producer snapshot at offset {} with {} producer ids in {} ms.", lastMapOffset,
                    producers.size(), time.hiResClockMs() - start);

            snapshots.put(snapshotFile.offset, snapshotFile);

            // Update the last snap offset according to the serialized map
            lastSnapOffset = lastMapOffset;

            return Optional.of(snapshotFile.file());
        }
        return Optional.empty();
    }

    /**
     * Update the parentDir for this ProducerStateManager and all of the snapshot files which it manages.
     */
    public void updateParentDir(File parentDir) {
        logDir = parentDir;
        snapshots.forEach((k, v) -> v.updateParentDir(parentDir));
    }

    /**
     * Get the last offset (exclusive) of the latest snapshot file.
     */
    public OptionalLong latestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = latestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> OptionalLong.of(snapshotFile.offset)).orElseGet(OptionalLong::empty);
    }

    /**
     * Get the last offset (exclusive) of the oldest snapshot file.
     */
    public OptionalLong oldestSnapshotOffset() {
        Optional<SnapshotFile> snapshotFileOptional = oldestSnapshotFile();
        return snapshotFileOptional.map(snapshotFile -> OptionalLong.of(snapshotFile.offset)).orElseGet(OptionalLong::empty);
    }

    /**
     * Visible for testing
     */
    public Optional<SnapshotFile> snapshotFileForOffset(long offset) {
        return Optional.ofNullable(snapshots.get(offset));
    }

    /**
     * Remove any unreplicated transactions lower than the provided logStartOffset and bring the lastMapOffset forward
     * if necessary.
     */
    public void onLogStartOffsetIncremented(long logStartOffset) {
        removeUnreplicatedTransactions(logStartOffset);

        if (lastMapOffset < logStartOffset) lastMapOffset = logStartOffset;

        lastSnapOffset = latestSnapshotOffset().orElse(logStartOffset);
    }

    private void removeUnreplicatedTransactions(long offset) {
        Iterator<Map.Entry<Long, TxnMetadata>> iterator = unreplicatedTxns.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, TxnMetadata> txnEntry = iterator.next();
            OptionalLong lastOffset = txnEntry.getValue().lastOffset;
            if (lastOffset.isPresent() && lastOffset.getAsLong() < offset) iterator.remove();
        }
    }

    /**
     * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
     */
    public void truncateFullyAndStartAt(long offset) throws IOException {
        clearProducerIds();
        ongoingTxns.clear();
        unreplicatedTxns.clear();
        for (SnapshotFile snapshotFile : snapshots.values()) {
            removeAndDeleteSnapshot(snapshotFile.offset);
        }
        lastSnapOffset = 0L;
        lastMapOffset = offset;
        updateOldestTxnTimestamp();
    }

    /**
     * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
     * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
     * transaction index, but the completion must be done only after successfully appending to the index.
     */
    public long lastStableOffset(CompletedTxn completedTxn) {
        return findNextIncompleteTxn(completedTxn.producerId)
                .map(x -> x.firstOffset.messageOffset)
                .orElse(completedTxn.lastOffset + 1);
    }

    private Optional<TxnMetadata> findNextIncompleteTxn(long producerId) {
        for (TxnMetadata txnMetadata : ongoingTxns.values()) {
            if (txnMetadata.producerId != producerId) {
                return Optional.of(txnMetadata);
            }
        }
        return Optional.empty();
    }

    /**
     * Mark a transaction as completed. We will still await advancement of the high watermark before
     * advancing the first unstable offset.
     */
    public void completeTxn(CompletedTxn completedTxn) {
        TxnMetadata txnMetadata = ongoingTxns.remove(completedTxn.firstOffset);
        if (txnMetadata == null)
            throw new IllegalArgumentException("Attempted to complete transaction " + completedTxn + " on partition "
                    + topicPartition + " which was not started");

        txnMetadata.lastOffset = OptionalLong.of(completedTxn.lastOffset);
        unreplicatedTxns.put(completedTxn.firstOffset, txnMetadata);
        updateOldestTxnTimestamp();
    }

    /**
     * Deletes the producer snapshot files until the given offset (exclusive) in a thread safe manner.
     *
     * @param offset offset number
     * @throws IOException if any IOException occurs while deleting the files.
     */
    public void deleteSnapshotsBefore(long offset) throws IOException {
        for (SnapshotFile snapshot : snapshots.subMap(0L, offset).values()) {
            removeAndDeleteSnapshot(snapshot.offset);
        }
    }

    public Optional<File> fetchSnapshot(long offset) {
        return Optional.ofNullable(snapshots.get(offset)).map(x -> x.file());
    }

    private Optional<SnapshotFile> oldestSnapshotFile() {
        return Optional.ofNullable(snapshots.firstEntry()).map(x -> x.getValue());
    }

    private Optional<SnapshotFile> latestSnapshotFile() {
        return Optional.ofNullable(snapshots.lastEntry()).map(e -> e.getValue());
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and deletes the backing snapshot file.
     */
    private void removeAndDeleteSnapshot(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) snapshotFile.deleteIfExists();
    }

    /**
     * Removes the producer state snapshot file metadata corresponding to the provided offset if it exists from this
     * ProducerStateManager, and renames the backing snapshot file to have the Log.DeletionSuffix.
     * <p>
     * Note: This method is safe to use with async deletes. If a race occurs and the snapshot file
     * is deleted without this ProducerStateManager instance knowing, the resulting exception on
     * SnapshotFile rename will be ignored and {@link Optional#empty()} will be returned.
     */
    public Optional<SnapshotFile> removeAndMarkSnapshotForDeletion(long snapshotOffset) throws IOException {
        SnapshotFile snapshotFile = snapshots.remove(snapshotOffset);
        if (snapshotFile != null) {
            // If the file cannot be renamed, it likely means that the file was deleted already.
            // This can happen due to the way we construct an intermediate producer state manager
            // during log recovery, and use it to issue deletions prior to creating the "real"
            // producer state manager.
            //
            // In any case, removeAndMarkSnapshotForDeletion is intended to be used for snapshot file
            // deletion, so ignoring the exception here just means that the intended operation was
            // already completed.
            try {
                snapshotFile.renameToDelete();
                return Optional.of(snapshotFile);
            } catch (NoSuchFileException ex) {
                log.info("Failed to rename producer state snapshot {} with deletion suffix because it was already deleted", snapshotFile.file().getAbsoluteFile());
            }
        }
        return Optional.empty();
    }

    public static List<ProducerStateEntry> readSnapshot(File file) throws IOException {
        try {
            byte[] buffer = Files.readAllBytes(file.toPath());
            Struct struct = PID_SNAPSHOT_MAP_SCHEMA.read(ByteBuffer.wrap(buffer));

            Short version = struct.getShort(VERSION_FIELD);
            if (version != PRODUCER_SNAPSHOT_VERSION)
                throw new CorruptSnapshotException("Snapshot contained an unknown file version " + version);

            long crc = struct.getUnsignedInt(CRC_FIELD);
            long computedCrc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.length - PRODUCER_ENTRIES_OFFSET);
            if (crc != computedCrc)
                throw new CorruptSnapshotException("Snapshot is corrupt (CRC is no longer valid). Stored crc: " + crc
                        + ". Computed crc: " + computedCrc);

            Object[] producerEntryFields = struct.getArray(PRODUCER_ENTRIES_FIELD);
            List<ProducerStateEntry> entries = new ArrayList<>(producerEntryFields.length);
            for (Object producerEntryObj : producerEntryFields) {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                long producerId = producerEntryStruct.getLong(PRODUCER_ID_FIELD);
                short producerEpoch = producerEntryStruct.getShort(PRODUCER_EPOCH_FIELD);
                int seq = producerEntryStruct.getInt(LAST_SEQUENCE_FIELD);
                long offset = producerEntryStruct.getLong(LAST_OFFSET_FIELD);
                long timestamp = producerEntryStruct.getLong(TIMESTAMP_FIELD);
                int offsetDelta = producerEntryStruct.getInt(OFFSET_DELTA_FIELD);
                int coordinatorEpoch = producerEntryStruct.getInt(COORDINATOR_EPOCH_FIELD);
                long currentTxnFirstOffset = producerEntryStruct.getLong(CURRENT_TXN_FIRST_OFFSET_FIELD);

                OptionalLong currentTxnFirstOffsetVal = currentTxnFirstOffset >= 0 ? OptionalLong.of(currentTxnFirstOffset) : OptionalLong.empty();
                Optional<BatchMetadata> batchMetadata =
                        (offset >= 0) ? Optional.of(new BatchMetadata(seq, offset, offsetDelta, timestamp)) : Optional.empty();
                entries.add(new ProducerStateEntry(producerId, producerEpoch, coordinatorEpoch, timestamp, currentTxnFirstOffsetVal, batchMetadata));
            }

            return entries;
        } catch (SchemaException e) {
            throw new CorruptSnapshotException("Snapshot failed schema validation: " + e.getMessage());
        }
    }

    private static void writeSnapshot(File file, Map<Long, ProducerStateEntry> entries, boolean sync) throws IOException {
        Struct struct = new Struct(PID_SNAPSHOT_MAP_SCHEMA);
        struct.set(VERSION_FIELD, PRODUCER_SNAPSHOT_VERSION);
        struct.set(CRC_FIELD, 0L); // we'll fill this after writing the entries
        Struct[] structEntries = new Struct[entries.size()];
        int i = 0;
        for (Map.Entry<Long, ProducerStateEntry> producerIdEntry : entries.entrySet()) {
            Long producerId = producerIdEntry.getKey();
            ProducerStateEntry entry = producerIdEntry.getValue();
            Struct producerEntryStruct = struct.instance(PRODUCER_ENTRIES_FIELD);
            producerEntryStruct.set(PRODUCER_ID_FIELD, producerId)
                    .set(PRODUCER_EPOCH_FIELD, entry.producerEpoch())
                    .set(LAST_SEQUENCE_FIELD, entry.lastSeq())
                    .set(LAST_OFFSET_FIELD, entry.lastDataOffset())
                    .set(OFFSET_DELTA_FIELD, entry.lastOffsetDelta())
                    .set(TIMESTAMP_FIELD, entry.lastTimestamp())
                    .set(COORDINATOR_EPOCH_FIELD, entry.coordinatorEpoch())
                    .set(CURRENT_TXN_FIRST_OFFSET_FIELD, entry.currentTxnFirstOffset().orElse(-1L));
            structEntries[i++] = producerEntryStruct;
        }
        struct.set(PRODUCER_ENTRIES_FIELD, structEntries);

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        // now fill in the CRC
        long crc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.limit() - PRODUCER_ENTRIES_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            if (sync) {
                fileChannel.force(true);
            }
        }
    }

    private static boolean isSnapshotFile(Path path) {
        return Files.isRegularFile(path) && path.getFileName().toString().endsWith(LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX);
    }

    // visible for testing
    public static List<SnapshotFile> listSnapshotFiles(File dir) throws IOException {
        if (dir.exists() && dir.isDirectory()) {
            try (Stream<Path> paths = Files.list(dir.toPath())) {
                return paths.filter(ProducerStateManager::isSnapshotFile)
                        .map(path -> new SnapshotFile(path.toFile())).collect(Collectors.toList());
            }
        } else {
            return Collections.emptyList();
        }
    }

}
