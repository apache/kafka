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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.internals.CachedStateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;
import static org.apache.kafka.streams.state.internals.OffsetCheckpoint.OFFSET_UNKNOWN;

/**
 * ProcessorStateManager is the source of truth for the current offset for each state store,
 * which is either the read offset during restoring, or the written offset during normal processing.
 *
 * The offset is initialized as null when the state store is registered, and then it can be updated by
 * loading checkpoint file, restore state stores, or passing from the record collector's written offsets.
 *
 * When checkpointing, if the offset is not null it would be written to the file.
 *
 * The manager is also responsible for restoring state stores via their registered restore callback,
 * which is used for both updating standby tasks as well as restoring active tasks.
 */
public class ProcessorStateManager implements StateManager {

    public static class StateStoreMetadata {
        private final StateStore stateStore;

        // corresponding changelog partition of the store, this and the following two fields
        // will only be not-null if the state store is logged (i.e. changelog partition and restorer provided)
        private final TopicPartition changelogPartition;

        // could be used for both active restoration and standby
        private final StateRestoreCallback restoreCallback;

        private final CommitCallback commitCallback;

        // record converters used for restoration and standby
        private final RecordConverter recordConverter;

        // indicating the current snapshot of the store as the offset of last changelog record that has been
        // applied to the store used for both restoration (active and standby tasks restored offset) and
        // normal processing that update stores (written offset); could be null (when initialized)
        //
        // the offset is updated in three ways:
        //   1. when loading from the checkpoint file, when the corresponding task has acquired the state
        //      directory lock and have registered all the state store; it is only one-time
        //   2. when updating with restore records (by both restoring active and standby),
        //      update to the last restore record's offset
        //   3. when checkpointing with the given written offsets from record collector,
        //      update blindly with the given offset
        private Long offset;

        // Will be updated on batch restored
        private Long endOffset;
        // corrupted state store should not be included in checkpointing
        private boolean corrupted;


        private StateStoreMetadata(final StateStore stateStore,
                                   final CommitCallback commitCallback) {
            this.stateStore = stateStore;
            this.commitCallback = commitCallback;
            this.restoreCallback = null;
            this.recordConverter = null;
            this.changelogPartition = null;
            this.corrupted = false;
            this.offset = null;
        }

        private StateStoreMetadata(final StateStore stateStore,
                                   final TopicPartition changelogPartition,
                                   final StateRestoreCallback restoreCallback,
                                   final CommitCallback commitCallback,
                                   final RecordConverter recordConverter) {
            if (restoreCallback == null) {
                throw new IllegalStateException("Log enabled store should always provide a restore callback upon registration");
            }

            this.stateStore = stateStore;
            this.changelogPartition = changelogPartition;
            this.restoreCallback = restoreCallback;
            this.commitCallback = commitCallback;
            this.recordConverter = recordConverter;
            this.offset = null;
        }

        private void setOffset(final Long offset) {
            this.offset = offset;
        }

        // the offset is exposed to the changelog reader to determine if restoration is completed
        Long offset() {
            return this.offset;
        }

        Long endOffset() {
            return this.endOffset;
        }

        public void setEndOffset(final Long endOffset) {
            this.endOffset = endOffset;
        }

        TopicPartition changelogPartition() {
            return this.changelogPartition;
        }

        StateStore store() {
            return this.stateStore;
        }

        @Override
        public String toString() {
            return "StateStoreMetadata (" + stateStore.name() + " : " + changelogPartition + " @ " + offset;
        }
    }

    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private final String logPrefix;

    private final TaskId taskId;
    private final boolean eosEnabled;
    private final boolean readUncommittedIsolation;
    private final ChangelogRegister changelogReader;
    private final Collection<TopicPartition> sourcePartitions;
    private final Map<String, String> storeToChangelogTopic;

    // must be maintained in topological order
    private final FixedOrderMap<String, StateStoreMetadata> stores = new FixedOrderMap<>();
    private final FixedOrderMap<String, StateStore> globalStores = new FixedOrderMap<>();

    private final File baseDir;
    private final OffsetCheckpoint checkpointFile;
    private final boolean stateUpdaterEnabled;

    private TaskType taskType;
    private Logger log;
    private Task.State taskState;

    public static String storeChangelogTopic(final String prefix, final String storeName, final String namedTopology) {
        if (namedTopology == null) {
            return prefix + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
        } else {
            return prefix + "-" + namedTopology + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
        }
    }

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final TaskType taskType,
                                 final boolean eosEnabled,
                                 final boolean readUncommittedIsolation,
                                 final LogContext logContext,
                                 final StateDirectory stateDirectory,
                                 final ChangelogRegister changelogReader,
                                 final Map<String, String> storeToChangelogTopic,
                                 final Collection<TopicPartition> sourcePartitions,
                                 final boolean stateUpdaterEnabled) throws ProcessorStateException {
        this.storeToChangelogTopic = storeToChangelogTopic;
        this.log = logContext.logger(ProcessorStateManager.class);
        this.logPrefix = logContext.logPrefix();
        this.taskId = taskId;
        this.taskType = taskType;
        this.eosEnabled = eosEnabled;
        this.readUncommittedIsolation = readUncommittedIsolation;
        this.changelogReader = changelogReader;
        this.sourcePartitions = sourcePartitions;
        this.stateUpdaterEnabled = stateUpdaterEnabled;

        this.baseDir = stateDirectory.getOrCreateDirectoryForTask(taskId);
        this.checkpointFile = new OffsetCheckpoint(stateDirectory.checkpointFileFor(taskId));

        log.debug("Created state store manager for task {}", taskId);
    }

    void registerStateStores(final List<StateStore> allStores, final InternalProcessorContext processorContext) {
        processorContext.uninitialize();
        for (final StateStore store : allStores) {
            if (stores.containsKey(store.name())) {
                if (!stateUpdaterEnabled) {
                    maybeRegisterStoreWithChangelogReader(store.name());
                }
            } else {
                store.init((StateStoreContext) processorContext, store);
            }
            log.trace("Registered state store {}", store.name());
        }
    }

    void registerGlobalStateStores(final List<StateStore> stateStores) {
        log.debug("Register global stores {}", stateStores);
        for (final StateStore stateStore : stateStores) {
            globalStores.put(stateStore.name(), stateStore);
        }
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return globalStores.get(name);
    }

    // package-private for test only
    void initializeStoreOffsetsFromCheckpoint(final boolean storeDirIsEmpty) {
        try {
            final Map<TopicPartition, Long> loadedCheckpoints = checkpointFile.read();

            log.trace("Loaded offsets from the checkpoint file: {}", loadedCheckpoints);

            for (final StateStoreMetadata store : stores.values()) {
                if (store.corrupted) {
                    log.error("Tried to initialize store offsets for corrupted store {}", store);
                    throw new IllegalStateException("Should not initialize offsets for a corrupted task");
                }

                if (store.changelogPartition == null) {
                    log.info("State store {} is not logged and hence would not be restored", store.stateStore.name());
                } else if (!store.stateStore.persistent()) {
                    log.info("Initializing to the starting offset for changelog {} of in-memory state store {}",
                             store.changelogPartition, store.stateStore.name());
                } else if (store.offset() == null) {

                    // load managed offsets from store
                    Long offset = loadOffsetFromStore(store);

                    // load offsets from .checkpoint file
                    offset = loadOffsetFromCheckpointFile(offset, loadedCheckpoints, store);

                    // no offsets found for store, store is corrupt if not empty
                    throwCorruptIfNoOffsetAndNotEmpty(offset, storeDirIsEmpty);

                    updateOffsetInMemory(offset, store);

                    syncManagedOffsetInStore(offset, store);

                }  else {
                    loadedCheckpoints.remove(store.changelogPartition);
                    log.debug("Skipping re-initialization of offset from checkpoint for recycled store {}",
                              store.stateStore.name());
                }
            }

            if (!loadedCheckpoints.isEmpty()) {
                log.warn("Some loaded checkpoint offsets cannot find their corresponding state stores: {}", loadedCheckpoints);
            }

            if (eosEnabled && readUncommittedIsolation) {
                // delete checkpoint file to ensure store is wiped on-crash
                checkpointFile.delete();
            } else {
                writeCheckpointFile(true);
            }

        } catch (final TaskCorruptedException e) {
            throw e;
        } catch (final IOException | RuntimeException e) {
            // both IOException or runtime exception like number parsing can throw
            throw new ProcessorStateException(format("%sError loading and deleting checkpoint file when creating the state manager",
                logPrefix), e);
        }
    }

    private Long loadOffsetFromStore(final StateStoreMetadata store) {
        if (store.stateStore.managesOffsets()) {
            return store.stateStore.getCommittedOffset(store.changelogPartition);
        }
        return null;
    }

    private Long loadOffsetFromCheckpointFile(final Long existingOffset,
                                              final Map<TopicPartition, Long> loadedCheckpoints,
                                              final StateStoreMetadata store) {
        // load offsets from .checkpoint file
        if (existingOffset == null && loadedCheckpoints.containsKey(store.changelogPartition)) {
            return loadedCheckpoints.remove(store.changelogPartition);
        } else {
            // offset found in store, disregard offset from .checkpoint file
            loadedCheckpoints.remove(store.changelogPartition);
        }
        return existingOffset;
    }

    private void throwCorruptIfNoOffsetAndNotEmpty(final Long offset, final boolean storeDirIsEmpty) {
        if (offset == null && eosEnabled && !storeDirIsEmpty) {
            throw new TaskCorruptedException(Collections.singleton(taskId));
        }
    }

    private void updateOffsetInMemory(final Long offset, final StateStoreMetadata store) {
        if (offset != null) {
            log.info("State store {} initialized with offset {} at changelog {}",
                    store.stateStore.name(), store.offset, store.changelogPartition);
            store.setOffset(changelogOffsetFromCheckpointedOffset(offset));
        }
    }

    private void syncManagedOffsetInStore(final Long offset, final StateStoreMetadata store) {
        if (store.stateStore.managesOffsets()) {
            if (eosEnabled && readUncommittedIsolation) {
                // clear changelog offset to ensure store is wiped on-crash
                store.stateStore.commit(Collections.singletonMap(store.changelogPartition, null));
            } else {
                // ensure current checkpoint is persisted to disk before we begin processing
                store.stateStore.commit(Collections.singletonMap(store.changelogPartition, checkpointableOffsetFromChangelogOffset(offset)));
            }
        }
    }

    private void maybeRegisterStoreWithChangelogReader(final String storeName) {
        if (isLoggingEnabled(storeName)) {
            changelogReader.register(getStorePartition(storeName), this);
        }
    }

    private List<TopicPartition> getAllChangelogTopicPartitions() {
        final List<TopicPartition> allChangelogPartitions = new ArrayList<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (storeMetadata.changelogPartition != null) {
                allChangelogPartitions.add(storeMetadata.changelogPartition);
            }
        }
        return allChangelogPartitions;
    }

    @Override
    public File baseDir() {
        return baseDir;
    }

    @Override
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback,
                              final CommitCallback commitCallback) {
        final String storeName = store.name();

        // TODO (KAFKA-12887): we should not trigger user's exception handler for illegal-argument but always
        // fail-crash; in this case we would not need to immediately close the state store before throwing
        if (CHECKPOINT_FILE_NAME.equals(storeName)) {
            store.close();
            throw new IllegalArgumentException(format("%sIllegal store name: %s, which collides with the pre-defined " +
                "checkpoint file name", logPrefix, storeName));
        }

        if (stores.containsKey(storeName)) {
            store.close();
            throw new IllegalArgumentException(format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        if (stateRestoreCallback instanceof StateRestoreListener) {
            log.warn("The registered state restore callback is also implementing the state restore listener interface, " +
                    "which is not expected and would be ignored");
        }

        final StateStoreMetadata storeMetadata = isLoggingEnabled(storeName) ?
            new StateStoreMetadata(
                store,
                getStorePartition(storeName),
                stateRestoreCallback,
                commitCallback,
                converterForStore(store)) :
            new StateStoreMetadata(store, commitCallback);

        // register the store first, so that if later an exception is thrown then eventually while we call `close`
        // on the state manager this state store would be closed as well
        stores.put(storeName, storeMetadata);

        if (!stateUpdaterEnabled) {
            maybeRegisterStoreWithChangelogReader(storeName);
        }

        log.debug("Registered state store {} to its state manager", storeName);
    }

    @Override
    public StateStore getStore(final String name) {
        if (stores.containsKey(name)) {
            return stores.get(name).stateStore;
        } else {
            return null;
        }
    }

    Set<TopicPartition> changelogPartitions() {
        return Collections.unmodifiableSet(changelogOffsets().keySet());
    }

    void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
        final Collection<TopicPartition> partitionsToMarkAsCorrupted = new LinkedList<>(partitions);
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (partitionsToMarkAsCorrupted.contains(storeMetadata.changelogPartition)) {
                storeMetadata.corrupted = true;
                partitionsToMarkAsCorrupted.remove(storeMetadata.changelogPartition);
            }
        }

        if (!partitionsToMarkAsCorrupted.isEmpty()) {
            throw new IllegalStateException("Some partitions " + partitionsToMarkAsCorrupted + " are not contained in " +
                "the store list of task " + taskId + " marking as corrupted, this is not expected");
        }
    }

    /**
     * This is used to determine if the state managed by this StateManager should be wiped and rebuilt.
     * @return {@code true} iff this StateManager has at least one "corrupt" store.
     */
    boolean hasCorruptedStores() {
        return stores.values().stream().anyMatch(store -> store.corrupted);
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        // return the current offsets for those logged stores
        final Map<TopicPartition, Long> changelogOffsets = new HashMap<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (storeMetadata.changelogPartition != null) {
                // for changelog whose offset is unknown, use 0L indicating earliest offset
                // otherwise return the current offset + 1 as the next offset to fetch
                changelogOffsets.put(
                    storeMetadata.changelogPartition,
                    storeMetadata.offset == null ? 0L : storeMetadata.offset + 1L);
            }
        }
        return changelogOffsets;
    }

    TaskId taskId() {
        return taskId;
    }

    void transitionTaskState(final Task.State taskState) {
        this.taskState = taskState;
    }

    Task.State taskState() {
        return taskState;
    }

    // used by the changelog reader only
    boolean changelogAsSource(final TopicPartition partition) {
        return sourcePartitions.contains(partition);
    }

    @Override
    public TaskType taskType() {
        return taskType;
    }

    // used by the changelog reader only
    StateStoreMetadata storeMetadata(final TopicPartition partition) {
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (partition.equals(storeMetadata.changelogPartition)) {
                return storeMetadata;
            }
        }
        return null;
    }

    // used by the changelog reader only
    void restore(final StateStoreMetadata storeMetadata, final List<ConsumerRecord<byte[], byte[]>> restoreRecords, final OptionalLong optionalLag) {
        if (!stores.containsValue(storeMetadata)) {
            throw new IllegalStateException("Restoring " + storeMetadata + " which is not registered in this state manager, " +
                "this should not happen.");
        }

        if (!restoreRecords.isEmpty()) {
            // restore states from changelog records and update the snapshot offset as the batch end record's offset
            final Long batchEndOffset = restoreRecords.get(restoreRecords.size() - 1).offset();
            final RecordBatchingStateRestoreCallback restoreCallback = adapt(storeMetadata.restoreCallback);
            final List<ConsumerRecord<byte[], byte[]>> convertedRecords = restoreRecords.stream()
                .map(storeMetadata.recordConverter::convert)
                .collect(Collectors.toList());

            try {
                restoreCallback.restoreBatch(convertedRecords);
            } catch (final RuntimeException e) {
                throw new ProcessorStateException(
                    format("%sException caught while trying to restore state from %s", logPrefix, storeMetadata.changelogPartition),
                    e
                );
            }

            storeMetadata.setOffset(batchEndOffset);
            // If null means the lag for this partition is not known yet
            if (optionalLag.isPresent()) {
                storeMetadata.setEndOffset(optionalLag.getAsLong() + batchEndOffset);
            }
        }
    }

    /**
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     * @throws StreamsException fatal error when flushing the state store, for example sending changelog records failed
     *                          or flushing state store get IO errors; such error should cause the thread to die
     */
    @Override
    public void commit() {
        RuntimeException firstException = null;
        // attempting to commit the stores
        if (!stores.isEmpty()) {
            log.debug("Committing all stores registered in the state manager: {}", stores);
            for (final StateStoreMetadata metadata : stores.values()) {
                final StateStore store = metadata.stateStore;
                log.trace("Committing store {}", store.name());
                try {
                    // if the changelog is corrupt, we need to ensure we delete any currently stored offset
                    // also, under EOS+READ_UNCOMMITTED, delete any offset to ensure store is wiped on-crash
                    final Long checkpointOffset = !eosEnabled || !readUncommittedIsolation || metadata.corrupted ?
                            null : checkpointableOffsetFromChangelogOffset(metadata.offset);
                    store.commit(metadata.changelogPartition != null && store.persistent() ?
                            Collections.singletonMap(metadata.changelogPartition, checkpointOffset) :
                            Collections.emptyMap()
                    );

                    if (metadata.commitCallback != null) {
                        metadata.commitCallback.onCommit();
                    }
                } catch (final IOException e) {
                    throw new ProcessorStateException(
                            format("%sException caught while trying to commit store, " +
                                    "changelog partition %s", logPrefix, metadata.changelogPartition),
                            e
                    );
                } catch (final RuntimeException exception) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (exception instanceof StreamsException)
                            firstException = exception;
                        else
                            firstException = new ProcessorStateException(
                                format("%sFailed to commit state store %s", logPrefix, store.name()), exception);
                    }
                    log.error("Failed to commit state store {}: ", store.name(), exception);
                }
            }

            // update checkpoints for only stores that don't manage their own offsets
            // do not write checkpoints under EOS+READ_UNCOMMITTED
            // this ensures that stores are still wiped on-crash in this case
            if (!eosEnabled || !readUncommittedIsolation) {
                writeCheckpointFile(false);
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    public void flushCache() {
        RuntimeException firstException = null;
        // attempting to flush the stores
        if (!stores.isEmpty()) {
            log.debug("Flushing all store caches registered in the state manager: {}", stores);
            for (final StateStoreMetadata metadata : stores.values()) {
                final StateStore store = metadata.stateStore;

                try {
                    // buffer should be flushed to send all records to changelog
                    if (store instanceof TimeOrderedKeyValueBuffer) {
                        final long checkpointOffset = checkpointableOffsetFromChangelogOffset(metadata.offset);
                        store.commit(metadata.changelogPartition != null ?
                                Collections.singletonMap(metadata.changelogPartition, checkpointOffset) :
                                Collections.emptyMap()
                        );
                    } else if (store instanceof CachedStateStore) {
                        ((CachedStateStore) store).flushCache();
                    }
                    log.trace("Flushed cache or buffer {}", store.name());
                } catch (final RuntimeException exception) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (exception instanceof StreamsException) {
                            firstException = exception;
                        } else {
                            firstException = new ProcessorStateException(
                                format("%sFailed to flush cache of store %s", logPrefix, store.name()),
                                exception
                            );
                        }
                    }
                    log.error("Failed to flush cache of store {}: ", store.name(), exception);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * {@link StateStore#close() Close} all stores (even in case of failure).
     * Log all exceptions and re-throw the first exception that occurred at the end.
     *
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    @Override
    public void close() throws ProcessorStateException {
        log.debug("Closing its state manager and all the registered state stores: {}", stores);

        if (!stateUpdaterEnabled) {
            changelogReader.unregister(getAllChangelogTopicPartitions());
        }

        RuntimeException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!stores.isEmpty()) {
            for (final Map.Entry<String, StateStoreMetadata> entry : stores.entrySet()) {
                final StateStore store = entry.getValue().stateStore;
                log.trace("Closing store {}", store.name());
                try {
                    store.close();
                } catch (final RuntimeException exception) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (exception instanceof StreamsException)
                            firstException = exception;
                        else
                            firstException = new ProcessorStateException(
                                format("%sFailed to close state store %s", logPrefix, store.name()), exception);
                    }
                    log.error("Failed to close state store {}: ", store.name(), exception);
                }
            }

            // write out .checkpoint file for *all* stores, including stores that manage their own checkpoints
            // this ensures store offsets are available for partition assignment, for all stores, without having to open
            // the store to call #getCommittedOffset()
            writeCheckpointFile(true);

            stores.clear();
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Alternative to {@link #close()} that just resets the changelogs without closing any of the underlying state
     * or unregistering the stores themselves
     */
    void recycle() {
        log.debug("Recycling state for {} task {}.", taskType, taskId);

        if (!stateUpdaterEnabled) {
            final List<TopicPartition> allChangelogs = getAllChangelogTopicPartitions();
            changelogReader.unregister(allChangelogs);
        }

        // when the state manager is recycled to be used, future writes may bypass its store's caching
        // layer if they are from restoration, hence we need to clear the state store's caches just in case
        // See KAFKA-14172 for details
        if (!stores.isEmpty()) {
            log.debug("Clearing all store caches registered in the state manager: {}", stores);
            for (final StateStoreMetadata metadata : stores.values()) {
                final StateStore store = metadata.stateStore;

                if (store instanceof CachedStateStore) {
                    ((CachedStateStore) store).clearCache();
                }
                log.trace("Cleared cache {}", store.name());
            }
        }
    }

    void transitionTaskType(final TaskType newType, final LogContext logContext) {
        if (taskType.equals(newType)) {
            throw new IllegalStateException("Tried to recycle state for task type conversion but new type was the same.");
        }

        taskType = newType;
        log = logContext.logger(ProcessorStateManager.class);
    }

    @Override
    public void updateChangelogOffsets(final Map<TopicPartition, Long> writtenOffsets) {
        for (final Map.Entry<TopicPartition, Long> entry : writtenOffsets.entrySet()) {
            final StateStoreMetadata store = findStore(entry.getKey());

            if (store != null) {
                store.setOffset(entry.getValue());

                log.debug("State store {} updated to written offset {} at changelog {}",
                        store.stateStore.name(), store.offset, store.changelogPartition);
            }
        }
    }

    void writeCheckpointFile(final boolean includeStoreManagedOffsets) {
        // checkpoint those stores that are only unmanaged, logged and persistent to the checkpoint file
        try {
            final Map<TopicPartition, Long> existingOffsets = checkpointFile.read();
            final Map<TopicPartition, Long> checkpointingOffsets = new HashMap<>();
            boolean needToWriteOffsets = false;
            for (final StateStoreMetadata storeMetadata : stores.values()) {
                // store is logged, persistent, not corrupted, and has a valid current offset
                if (storeMetadata.changelogPartition != null &&
                        storeMetadata.stateStore.persistent() &&
                        !storeMetadata.corrupted) {
                    final long checkpointableOffset;
                    if (storeMetadata.stateStore.managesOffsets() && !includeStoreManagedOffsets) {
                        // keep existing entry, don't update it because we don't know when the currently committed offset
                        // will be persisted to disk, so the existing entry is the latest one we can guarantee
                        checkpointableOffset = checkpointableOffsetFromChangelogOffset(existingOffsets.get(storeMetadata.changelogPartition));
                    } else {
                        // use current offset, either because this store doesn't manage its own offsets, or because it
                        // does, and we're deliberately including managed offsets (e.g. during #close())
                        checkpointableOffset = checkpointableOffsetFromChangelogOffset(storeMetadata.offset);
                        needToWriteOffsets = true;
                    }

                    checkpointingOffsets.put(storeMetadata.changelogPartition, checkpointableOffset);
                }
            }

            // we only need to actually write the offsets if they have changed, which will only be the case iff:
            // 1. there's at least one store that doesn't manage its offsets OR
            // 2. this StoreManager is being closed (hence, includeStoreManagedOffsets == true) OR
            // 3. at least one store is corrupt and needs to reset its checkpoint offsets by deleting the file
            // we avoid writing the .checkpoint file when we can, because this method is called on every commit(), which
            // can be very frequent, and writing this file is fairly expensive
            if (needToWriteOffsets || existingOffsets.size() != checkpointingOffsets.size()) {
                log.debug("Writing checkpoint: {} for task {}", checkpointingOffsets, taskId);
                checkpointFile.write(checkpointingOffsets);
            }
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to [{}]." +
                            " This may occur if OS cleaned the state.dir in case when it located in ${java.io.tmpdir} directory." +
                            " This may also occur due to running multiple instances on the same machine using the same state dir." +
                            " Changing the location of state.dir may resolve the problem.",
                    checkpointFile, e);
        }
    }

    private  TopicPartition getStorePartition(final String storeName) {
        // NOTE we assume the partition of the topic can always be inferred from the task id;
        // if user ever use a custom partition grouper (deprecated in KIP-528) this would break and
        // it is not a regression (it would always break anyways)
        return new TopicPartition(changelogFor(storeName), taskId.partition());
    }

    private boolean isLoggingEnabled(final String storeName) {
        // if the store name does not exist in the changelog map, it means the underlying store
        // is not log enabled (including global stores)
        return changelogFor(storeName) != null;
    }

    private StateStoreMetadata findStore(final TopicPartition changelogPartition) {
        final List<StateStoreMetadata> found = stores.values().stream()
            .filter(metadata -> changelogPartition.equals(metadata.changelogPartition))
            .collect(Collectors.toList());

        if (found.size() > 1) {
            throw new IllegalStateException("Multiple state stores are found for changelog partition " + changelogPartition +
                ", this should never happen: " + found);
        }

        return found.isEmpty() ? null : found.get(0);
    }

    // Pass in a sentinel value to checkpoint when the changelog offset is not yet initialized/known
    private long checkpointableOffsetFromChangelogOffset(final Long offset) {
        return offset != null ? offset : OFFSET_UNKNOWN;
    }

    // Convert the written offsets in the checkpoint file back to the changelog offset
    private Long changelogOffsetFromCheckpointedOffset(final long offset) {
        return offset != OFFSET_UNKNOWN ? offset : null;
    }

    public TopicPartition registeredChangelogPartitionFor(final String storeName) {
        final StateStoreMetadata storeMetadata = stores.get(storeName);
        if (storeMetadata == null) {
            throw new IllegalStateException("State store " + storeName
                + " for which the registered changelog partition should be"
                + " retrieved has not been registered"
            );
        }
        if (storeMetadata.changelogPartition == null) {
            throw new IllegalStateException("Registered state store " + storeName
                + " does not have a registered changelog partition."
                + " This may happen if logging is disabled for the state store."
            );
        }
        return storeMetadata.changelogPartition;
    }

    public String changelogFor(final String storeName) {
        return storeToChangelogTopic.get(storeName);
    }

    @Override
    public long approximateNumUncommittedBytes() {
        return stores.values().stream()
                .map(metadata -> metadata.store().approximateNumUncommittedBytes())
                .reduce(0L, Long::sum);
    }
}
