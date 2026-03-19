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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.state.internals.OffsetCheckpoint.OFFSET_UNKNOWN;

public class LegacyCheckpointingStateStore<S extends StateStore, K, V> extends WrappedStateStore<S, K, V> {
    public static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    private static final Logger log = LoggerFactory.getLogger(LegacyCheckpointingStateStore.class);

    static final long OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT = 10_000L;

    private final boolean eosEnabled;
    private final Set<TopicPartition> changelogPartitions;
    private final TaskId taskId;
    private final OffsetCheckpoint checkpointFile;
    private final String logPrefix;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Map<TopicPartition, Long> checkpointedOffsets;
    private boolean corrupted = false;

    /**
     * Wraps the given {@link StateStore} as a {@code LegacyCheckpointingStateStore}, only if it is both
     * {@link StateStore#persistent() persistent}, and it does not {@link StateStore#managesOffsets() manage its own offsets}.
     */
    @SuppressWarnings("deprecation")
    public static <S extends StateStore, K, V> StateStore maybeWrapStore(final S wrapped,
                                                                         final boolean eosEnabled,
                                                                         final Set<TopicPartition> changelogPartitions,
                                                                         final StateDirectory stateDirectory,
                                                                         final TaskId taskId,
                                                                         final String logPrefix) {
        return wrapped.persistent() && !wrapped.managesOffsets()
                ? new LegacyCheckpointingStateStore<>(wrapped, eosEnabled, changelogPartitions, stateDirectory, taskId, logPrefix)
                : wrapped;
    }

    /**
     * Unwraps the given store, only if it is a {@code LegacyCheckpointingStateStore}.
     */
    public static StateStore maybeUnwrapStore(final StateStore store) {
        return (store instanceof LegacyCheckpointingStateStore<?, ?, ?>)
                ? ((LegacyCheckpointingStateStore<?, ?, ?>) store).wrapped()
                : store;
    }

    /**
     * Writes a consolidated per-task {@code .checkpoint} file for downgrade support.
     *
     * When {@code upgradeFrom} is set to a version older than 4.3, this method writes the offsets into the legacy
     * per-task checkpoint file so that an older Kafka Streams version can find its offsets after a downgrade.
     *
     * This is a no-op if {@code upgradeFrom} is {@code null} or refers to version 4.3 or later.
     *
     * @param logPrefix Log prefix to use for log messages.
     * @param upgradeFrom The configured {@code upgrade.from} value, or {@code null} if not set.
     * @param stateDirectory The singleton {@link StateDirectory} used for looking up state directories.
     * @param taskId Either the task ID for regular stores, or {@code null} for global stores.
     * @param offsets The offsets to write to the checkpoint file. Entries with {@code null} values are written as
     *                {@link OffsetCheckpoint#OFFSET_UNKNOWN}.
     */
    public static void maybeDowngradeOffsets(final String logPrefix,
                                             final UpgradeFromValues upgradeFrom,
                                             final StateDirectory stateDirectory,
                                             final TaskId taskId,
                                             final Map<TopicPartition, Long> offsets) {
        if (upgradeFrom == null || upgradeFrom.ordinal() > UpgradeFromValues.UPGRADE_FROM_42.ordinal()) {
            return;
        }

        final Map<TopicPartition, Long> checkpointableOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            checkpointableOffsets.put(entry.getKey(), checkpointableOffsetFromChangelogOffset(entry.getValue()));
        }

        final File legacyCheckpointFile = checkpointFileFor(stateDirectory, taskId, null);
        final OffsetCheckpoint checkpoint = new OffsetCheckpoint(legacyCheckpointFile);
        try {
            log.debug("{}Writing downgrade checkpoint file for task {} with offsets {}", logPrefix, taskId, checkpointableOffsets);
            checkpoint.write(checkpointableOffsets);
        } catch (final IOException e) {
            log.warn("{}Failed to write downgrade checkpoint file for task {}", logPrefix, taskId, e);
        }
    }

    public static void maybeMarkCorrupted(final StateStore store) {
        if (store instanceof LegacyCheckpointingStateStore<?, ?, ?>) {
            ((LegacyCheckpointingStateStore<?, ?, ?>) store).markAsCorrupted();
        }
    }

    /**
     * Migrates offsets stored in a legacy, global/per-task .checkpoint file into the {@code stores}.
     *
     * The {@code stores} <em>MUST</em> manage their own offsets (i.e. {@link #managesOffsets()} must be {@code true}.
     * They can either do this themselves, or be wrapped in a {@link LegacyCheckpointingStateStore} implementation.
     *
     * Once this method successfully returns, the legacy {@code .checkpoint} file for the given {@link TaskId} (or the
     * global checkpoint, when {@code taskId} is {@code null}), will have been migrated and deleted from the filesystem.
     *
     * @param logPrefix Log prefix to use for log messages.
     * @param stateDirectory The singleton {@link StateDirectory} used for looking up existing checkpoint files.
     * @param taskId Either the task ID for regular stores, or {@code null} to migrate global stores.
     * @param stores A {@link Map} of {@link TopicPartition changelog partitions} to their {@link StateStore}. For global
     *               stores, which may have multiple {@link TopicPartition changelog partitions}, stores may appear
     *               multiple times, once for each of its {@link TopicPartition changelog partitions}.
     */
    @SuppressWarnings("deprecation")
    public static void migrateLegacyOffsets(final String logPrefix,
                                            final StateDirectory stateDirectory,
                                            final TaskId taskId,
                                            final Map<TopicPartition, StateStore> stores) {
        // load legacy per-task checkpoint
        final File legacyCheckpointFile = checkpointFileFor(stateDirectory, taskId, null);

        if (legacyCheckpointFile.exists()) {
            log.info("Migrating legacy checkpoint file for task {}", taskId);
            final OffsetCheckpoint legacyCheckpoint = new OffsetCheckpoint(legacyCheckpointFile);

            try {
                // build offsets for each store
                final Map<StateStore, Map<TopicPartition, Long>> storesToMigrate = new HashMap<>();
                for (final Map.Entry<TopicPartition, Long> entry : legacyCheckpoint.read().entrySet()) {
                    final StateStore store = stores.get(entry.getKey());
                    if (store != null) {
                        final Long offset = changelogOffsetFromCheckpointedOffset(entry.getValue());
                        if (offset != null) {
                            storesToMigrate
                                    .computeIfAbsent(store, k -> new HashMap<>())
                                    .put(entry.getKey(), offset);
                        }
                    }
                }

                // commit checkpointed offsets to each store
                for (final Map.Entry<StateStore, Map<TopicPartition, Long>> entry : storesToMigrate.entrySet()) {
                    final StateStore store = entry.getKey();
                    if (!store.managesOffsets()) {
                        log.warn("{}Error migrating legacy checkpoint offsets: StateStore '{}' does not manage its own offsets. " +
                                "The checkpointed offsets for this store will not be migrated, and will be lost. " +
                                "This store will need to fully restore its state on application restart. " +
                                "This is a bug in Kafka Streams, and should never be possible.", logPrefix, store.name());
                    }

                    // attempt to commit the offsets, even if the store doesn't manage them itself
                    store.commit(entry.getValue());
                }

                // delete legacy checkpoint file
                legacyCheckpoint.delete();

                log.info("Migrated legacy checkpoint file for task {} with offsets migrated for {} stores", taskId, storesToMigrate.size());
            } catch (final IOException | RuntimeException e) {
                throw new ProcessorStateException(String.format("%sError migrating checkpoint file for task '%s'", logPrefix, taskId), e);
            }
        } else {
            log.debug("No legacy checkpoint file found for task {}", taskId);
        }
    }

    LegacyCheckpointingStateStore(final S wrapped,
                                  final boolean eosEnabled,
                                  final Set<TopicPartition> changelogPartitions,
                                  final StateDirectory stateDirectory,
                                  final TaskId taskId,
                                  final String logPrefix) {
        super(wrapped);
        this.eosEnabled = eosEnabled;
        this.changelogPartitions = changelogPartitions;
        this.taskId = taskId;
        this.checkpointFile = new OffsetCheckpoint(checkpointFileFor(stateDirectory, taskId, this));
        this.logPrefix = logPrefix;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        // load store offsets from checkpoint file
        try {
            final Map<TopicPartition, Long> allOffsets = checkpointFile.read();
            for (final Map.Entry<TopicPartition, Long> entry : allOffsets.entrySet()) {
                if (changelogPartitions.contains(entry.getKey())) {
                    offsets.put(entry.getKey(), entry.getValue());
                }
            }
            checkpointedOffsets = new HashMap<>(offsets);
        } catch (final IOException | RuntimeException e) {
            throw new ProcessorStateException(String.format("%sError loading checkpoint file when creating StateStore '%s'", logPrefix, name()), e);
        }

        // initialize the actual store
        super.init(stateStoreContext, root);

        // under EOS, we delete the checkpoint file after everything has been loaded to ensure state is wiped after a crash
        try {
            if (eosEnabled) {
                checkpointFile.delete();
            }
        } catch (final IOException e) {
            throw new ProcessorStateException(String.format("%sError deleting checkpoint file when creating StateStore '%s'", logPrefix, name()), e);
        }
    }

    @Override
    @Deprecated
    public boolean managesOffsets() {
        return true;
    }

    @Override
    public Long committedOffset(final TopicPartition partition) {
        return offsets.get(partition);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        super.commit(changelogOffsets);

        // update in-memory offsets
        if (changelogOffsets.isEmpty()) {
            offsets.clear();
        } else {
            for (final Map.Entry<TopicPartition, Long> entry : changelogOffsets.entrySet()) {
                if (entry.getValue() != null) {
                    offsets.put(entry.getKey(), entry.getValue());
                } else {
                    offsets.remove(entry.getKey());
                }
            }
        }

        // only write the checkpoint file if both:
        // 1. in ALOS mode (under EOS, the checkpoint file is only written when closing the store)
        // 2. we have written enough new data to the store to warrant updating the checkpoint (prevents disk thrashing)
        if (!eosEnabled && checkpointNeeded(checkpointedOffsets, offsets)) {
            checkpoint();
        }
    }

    @Override
    public void close() {
        super.close();

        if (!corrupted) {
            checkpoint();
        }
    }

    public void markAsCorrupted() {
        corrupted = true;
    }

    /**
     * "checkpoint" committed offsets to disk.
     */
    void checkpoint() {
        // only checkpoint persistent and logged stores
        if (persistent() && !changelogPartitions.isEmpty()) {
            try {
                log.debug("Writing checkpoint: {} for task {}", offsets, taskId);
                checkpointFile.write(offsets);
                checkpointedOffsets = new HashMap<>(offsets);
            } catch (final IOException e) {
                log.warn("{}Failed to write offset checkpoint file to [{}]." +
                                " This may occur if OS cleaned the state.dir in case when it located in ${java.io.tmpdir} directory." +
                                " This may also occur due to running multiple instances on the same machine using the same state dir." +
                                " Changing the location of state.dir may resolve the problem.",
                        logPrefix, checkpointFile, e);
            }
        }
    }

    static File checkpointFileFor(final StateDirectory stateDirectory,
                                  final TaskId taskId,
                                  final StateStore store) {
        return taskId == null ?
                // global store
                (store == null ?
                        // legacy, global file
                        new File(stateDirectory.globalStateDir(), CHECKPOINT_FILE_NAME) :
                        // per-store file
                        new File(stateDirectory.globalStateDir(), CHECKPOINT_FILE_NAME + "_" + store.name())
                ) :
                (store == null ?
                        // legacy, per-task file
                        new File(stateDirectory.getOrCreateDirectoryForTask(taskId), CHECKPOINT_FILE_NAME) :
                        // per-store file
                        new File(stateDirectory.getOrCreateDirectoryForTask(taskId), CHECKPOINT_FILE_NAME + "_" + store.name())
                );
    }

    static boolean checkpointNeeded(final Map<TopicPartition, Long> oldOffsetSnapshot,
                                    final Map<TopicPartition, Long> newOffsetSnapshot) {
        // we should always have the old snapshot post completing the register state stores;
        // if it is null it means the registration is not done and hence we should not overwrite the checkpoint
        if (oldOffsetSnapshot == null) {
            return false;
        }

        // we can checkpoint if the difference between the current and the previous snapshot is large enough
        long totalOffsetDelta = 0L;
        for (final Map.Entry<TopicPartition, Long> entry : newOffsetSnapshot.entrySet()) {
            final Long newOffset = entry.getValue();
            if (newOffset != null) {
                final Long oldOffset = oldOffsetSnapshot.get(entry.getKey());
                totalOffsetDelta += newOffset - (oldOffset == null ? 0L : oldOffset);
            }
        }

        return totalOffsetDelta > OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT;
    }

    // Convert a changelog offset to the value written in the checkpoint file
    private static long checkpointableOffsetFromChangelogOffset(final Long offset) {
        return offset != null ? offset : OFFSET_UNKNOWN;
    }

    // Convert the written offsets in the checkpoint file back to the changelog offset
    private static Long changelogOffsetFromCheckpointedOffset(final long offset) {
        return offset != OFFSET_UNKNOWN ? offset : null;
    }
}
