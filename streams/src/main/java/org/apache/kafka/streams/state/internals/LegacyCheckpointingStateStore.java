package org.apache.kafka.streams.state.internals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.state.internals.OffsetCheckpoint.OFFSET_UNKNOWN;

public class LegacyCheckpointingStateStore<S extends StateStore, K, V> extends WrappedStateStore<S, K, V> {

    private static final Logger log = LoggerFactory.getLogger(LegacyCheckpointingStateStore.class);

    static final String CHECKPOINT_FILE_NAME = ".checkpoint";
    static final long OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT = 10_000L;

    private final boolean eosEnabled;
    private final Set<TopicPartition> changelogPartitions;
    private final StateDirectory stateDirectory;
    private final TaskId taskId;
    private final OffsetCheckpoint checkpointFile;
    private final String logPrefix;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Map<TopicPartition, Long> checkpointedOffsets;

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
     * Runs post-initialization for {@code LegacyCheckpointingStore}, only if the {@code store} is one.
     *
     * This must be run after <em>ALL</em> stores have been initialized, as it's possible it may delete a shared
     * checkpoint file, which is needed during initialization.
     */
    public static void maybeCleanupCheckpointFile(final Iterable<StateStore> stores) {
        for (final StateStore store : stores) {
            if (store instanceof LegacyCheckpointingStateStore) {
                final LegacyCheckpointingStateStore<?, ?, ?> wrappedStore = ((LegacyCheckpointingStateStore<?, ?, ?>) store);
                try {
                    if (wrappedStore.eosEnabled) {
                        wrappedStore.checkpointFile.delete();
                    }
                } catch (final IOException e) {
                    throw new ProcessorStateException(String.format("%sError deleting checkpoint file when creating StateStore '%s'", wrappedStore.logPrefix, store.name()), e);
                }
            }
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
        this.stateDirectory = stateDirectory;
        this.taskId = taskId;
        this.checkpointFile = new OffsetCheckpoint(checkpointFileFor(taskId));
        this.logPrefix = logPrefix;

        // fail-crash; in this case we would not need to immediately close the state store before throwing
        if (CHECKPOINT_FILE_NAME.equals(wrapped.name())) {
            wrapped.close();
            throw new IllegalArgumentException(String.format("%sIllegal store name: %s, which collides with the pre-defined " +
                    "checkpoint file name", logPrefix, wrapped.name()));
        }
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        // load store offsets from checkpoint file
        try {
            final Map<TopicPartition, Long> allOffsets = checkpointFile.read();
            for (final Map.Entry<TopicPartition, Long> entry : allOffsets.entrySet()) {
                if (changelogPartitions.contains(entry.getKey())) {
                    offsets.put(entry.getKey(), changelogOffsetFromCheckpointedOffset(entry.getValue()));
                }
            }
            checkpointedOffsets = new HashMap<>(offsets);
        } catch (final IOException e) {
            throw new ProcessorStateException(String.format("%sError loading checkpoint file when creating StateStore '%s'", logPrefix, name()), e);
        }

        // initialize the actual store
        super.init(stateStoreContext, root);
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
        for (final TopicPartition partition : offsets.keySet()) {
            offsets.put(partition, changelogOffsets.get(partition));
        }
        // todo: error handling: what if changelogOffsets and checkpointedOffsets contain different sets of partitions?

        // only write the checkpoint file if both:
        // 1. in ALOS mode (under EOS, the checkpoint file is only written when closing the store)
        // 2. we have written enough new data to the store to warrant updating the checkpoint (prevents disk thrashing)
        if (!eosEnabled && checkpointNeeded(checkpointedOffsets, offsets)) {
            checkpoint();
            checkpointedOffsets = new HashMap<>(offsets);
        }
    }

    @Override
    public void close() {
        super.close();
        checkpoint();
    }

    /**
     * "checkpoint" committed offsets to disk.
     */
    void checkpoint() {
        // only checkpoint persistent and logged stores
        if (persistent() && !changelogPartitions.isEmpty()) {
            try {
                // merge new checkpoint offsets into checkpoint file
                final Map<TopicPartition, Long> checkpointingOffsets = checkpointFile.read();
                for (final Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                    checkpointingOffsets.put(entry.getKey(), checkpointableOffsetFromChangelogOffset(entry.getValue()));
                }

                log.debug("Writing checkpoint: {} for task {}", checkpointingOffsets, taskId);
                checkpointFile.write(checkpointingOffsets);
            } catch (final IOException e) {
                log.warn("Failed to write offset checkpoint file to [{}]." +
                                " This may occur if OS cleaned the state.dir in case when it located in ${java.io.tmpdir} directory." +
                                " This may also occur due to running multiple instances on the same machine using the same state dir." +
                                " Changing the location of state.dir may resolve the problem.",
                        checkpointFile, e);
            }
        }
    }

    File checkpointFileFor(final TaskId taskId) {
        return taskId == null ? new File(stateDirectory.globalStateDir(), CHECKPOINT_FILE_NAME) // global store
                : new File(stateDirectory.getOrCreateDirectoryForTask(taskId), CHECKPOINT_FILE_NAME); // non-global store
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
            totalOffsetDelta += entry.getValue() - oldOffsetSnapshot.getOrDefault(entry.getKey(), 0L);
        }

        // when enforcing checkpoint is required, we should overwrite the checkpoint if it is different from the old one;
        // otherwise, we only overwrite the checkpoint if it is largely different from the old one
        return totalOffsetDelta > OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT;
    }

    // Pass in a sentinel value to checkpoint when the changelog offset is not yet initialized/known
    private long checkpointableOffsetFromChangelogOffset(final Long offset) {
        return offset != null ? offset : OFFSET_UNKNOWN;
    }

    // Convert the written offsets in the checkpoint file back to the changelog offset
    private Long changelogOffsetFromCheckpointedOffset(final long offset) {
        return offset != OFFSET_UNKNOWN ? offset : null;
    }
}
