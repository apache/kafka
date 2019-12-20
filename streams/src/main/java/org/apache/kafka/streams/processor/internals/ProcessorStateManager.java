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
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;


public class ProcessorStateManager implements StateManager {

    public class StateStoreMetadata {
        public StateStore stateStore;

        // indicating the current snapshot of the store, used for both
        // restoration (active and standby tasks restored offset) and
        // running (written offset); could be null (when initialized)
        //
        // the offset is updated in two ways:
        //   1. when updating with restore records (by both restoring active and standby),
        //      update to the last restore record's offset
        //   2. when checkpointing with the given written offsets from record collector,
        //      update blindly with the given offset
        public Long offset;

        // could be used for both active restoration and standby
        public StateRestoreCallback restoreCallback;

        // record converters used for restoration and standby
        public RecordConverter recordConverter;

        // corresponding changelog partition of the store
        public TopicPartition changelogPartition;

        // TODO: temp
        public StateRestorer stateRestorer;

        private StateStoreMetadata(final StateStore stateStore) {
            this.stateStore = stateStore;
            this.offset = null;
            this.stateRestorer = null;
            this.restoreCallback = null;
            this.recordConverter = null;
            this.changelogPartition = null;
        }
    }

    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private final Logger log;
    private final TaskId taskId;
    private final String logPrefix;
    private final ChangelogReader changelogReader;
    private final Map<String, String> storeToChangelogTopic;

    // must be maintained in topological order
    private final FixedOrderMap<String, StateStoreMetadata> stores = new FixedOrderMap<>();

    // NOTE we assume the partition of the topic can always be inferred from the task id;
    // if user ever use a default partition grouper (deprecated in KIP-528) this would break and
    // it is not a regression (it would always break anyways)

    private final File baseDir;
    private final OffsetCheckpoint checkpointFile;

    public static String storeChangelogTopic(final String applicationId,
                                             final String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final boolean isStandby,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic,
                                 final ChangelogReader changelogReader,
                                 final LogContext logContext) throws ProcessorStateException {
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        this.logPrefix = format("task [%s] ", taskId);
        this.log = logContext.logger(ProcessorStateManager.class);

        this.storeToChangelogTopic = new HashMap<>(storeToChangelogTopic);

        this.baseDir = stateDirectory.directoryForTask(taskId);
        this.checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));

        log.debug("Created state store manager for task {}", taskId);
    }

    public void initialize() {
        try {
            // load the checkpointed offsets, initialize the registered stores and then delete the checkpoint file
            final Map<TopicPartition, Long> loadedCheckpoints = checkpointFile.read();

            log.trace("Loaded offsets from the checkpoint filed: {}", loadedCheckpoints);

            for (final StateStoreMetadata storeMetadata : stores.values()) {
                if (storeMetadata.changelogPartition == null) {
                    log.info("State store {} is not logged and hence would not be restored", storeMetadata.stateStore.name());
                }

                if (loadedCheckpoints.containsKey(storeMetadata.changelogPartition)) {
                    storeMetadata.offset = loadedCheckpoints.remove(storeMetadata.changelogPartition);

                    log.debug("State store {} initialized from checkpoint with offset {} at changelog {}",
                        storeMetadata.stateStore.name(), storeMetadata.offset, storeMetadata.changelogPartition);
                } else {
                    log.info("State store {} did not find checkpoint offset, hence would " +
                            "default to the starting offset at changelog {}",
                        storeMetadata.stateStore.name(), storeMetadata.changelogPartition);
                }
            }

            if (!loadedCheckpoints.isEmpty()) {
                log.warn("Some loaded checkpoint offsets cannot find their corresponding state stores: {}", loadedCheckpoints);
            }

            checkpointFile.delete();
        } catch (final IOException e) {
            throw new ProcessorStateException(format("%sError loading and deleting checkpoint file when creating the state manager",
                logPrefix), e);
        }
    }

    @Override
    public File baseDir() {
        return baseDir;
    }

    @Override
    public void registerStore(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        final String storeName = store.name();

        if (CHECKPOINT_FILE_NAME.equals(storeName)) {
            throw new IllegalArgumentException(format("%sIllegal store name: %s", logPrefix, storeName));
        }

        if (stores.containsKey(storeName)) {
            throw new IllegalArgumentException(format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        final StateStoreMetadata storeMetadata = new StateStoreMetadata(store);

        // if the store name does not exist in the changelog map, it means the underlying store
        // is not log enabled OR the store is global (i.e. not logged either), and hence it does not need to be restored
        final String topic = storeToChangelogTopic.get(storeName);
        if (topic != null && stateRestoreCallback != null) {
            final CompositeRestoreListener restoreListener = new CompositeRestoreListener(stateRestoreCallback);
            final TopicPartition storePartition = new TopicPartition(topic, taskId.partition);
            final RecordConverter recordConverter = converterForStore(store);

            storeMetadata.changelogPartition = storePartition;
            storeMetadata.restoreCallback = stateRestoreCallback;
            storeMetadata.recordConverter = converterForStore(store);
            storeMetadata.stateRestorer = new StateRestorer(
                storePartition,
                restoreListener,
                -1L,        // TODO
                Long.MAX_VALUE,
                store.persistent(),
                storeName,
                recordConverter
            );

            changelogReader.register(storePartition, this);
        }

        stores.put(storeName, storeMetadata);

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

    Collection<TopicPartition> changelogPartitions() {
        return changelogOffsets().keySet();
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        // return the current offsets for those logged stores
        Map<TopicPartition, Long> changelogOffsets = new HashMap<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (storeMetadata.changelogPartition != null && storeMetadata.offset != null) {
                changelogOffsets.put(storeMetadata.changelogPartition, storeMetadata.offset);
            }
        }
        return changelogOffsets;
    }

    // used by the changelog reader only
    StateStoreMetadata storeMetadata(final TopicPartition partition) {
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            if (storeMetadata.changelogPartition == partition) {
                return storeMetadata;
            }
        }
        return null;
    }

    private StateStoreMetadata findStore(final TopicPartition changelogPartition) {
        final List<StateStoreMetadata> found = stores.values().stream()
            .filter(metadata -> metadata.changelogPartition == changelogPartition).collect(Collectors.toList());

        if (found.size() > 1) {
            throw new IllegalStateException("Multiple state stores are found for changelog partition " + changelogPartition +
                ", this should never happen: " + found);
        }

        return found.isEmpty() ? null : found.get(0);
    }

    private StateStoreMetadata mustFindStore(final TopicPartition changelogPartition) {
        final StateStoreMetadata storeMetadata = findStore(changelogPartition);

        if (storeMetadata == null) {
            throw new IllegalStateException("No state stores found for changelog partition " + changelogPartition);
        }

        return storeMetadata;
    }

    void restore(final TopicPartition changelogPartition, final List<ConsumerRecord<byte[], byte[]>> restoreRecords) {
        final StateStoreMetadata store = mustFindStore(changelogPartition);

        if (!restoreRecords.isEmpty()) {
            // restore states from changelog records and update the snapshot offset as the batch end record's offset
            final Long batchEndOffset = restoreRecords.get(restoreRecords.size() - 1).offset();
            final RecordBatchingStateRestoreCallback restoreCallback = adapt(store.restoreCallback);
            final List<ConsumerRecord<byte[], byte[]>> convertedRecords = new ArrayList<>(restoreRecords.size());
            for (final ConsumerRecord<byte[], byte[]> record : restoreRecords) {
                convertedRecords.add(store.recordConverter.convert(record));
            }

            try {
                restoreCallback.restoreBatch(convertedRecords);
            } catch (final RuntimeException e) {
                throw new ProcessorStateException(format("%sException caught while trying to restore state from %s",
                    logPrefix, changelogPartition), e);
            }

            store.offset = batchEndOffset;
        }
    }

    /**
     * @throws ProcessorStateException error when flushing the state store; for example IO exception
     * @throws StreamsException fatal error sending changelog records that should cause the thread to die
     * @throws TaskMigratedException recoverable error sending changelog records that would cause the task to be removed
     */
    @Override
    public void flush() {
        RuntimeException firstException = null;
        // attempting to flush the stores
        if (!stores.isEmpty()) {
            log.debug("Flushing all stores registered in the state manager");
            for (final Map.Entry<String, StateStoreMetadata> entry : stores.entrySet()) {
                final StateStore store = entry.getValue().stateStore;
                log.trace("Flushing store {}", store.name());
                try {
                    store.flush();
                } catch (final RuntimeException e) {
                    if (firstException == null) {
                        // do NOT wrap the error if it is actually caused by Streams itself
                        if (e instanceof StreamsException)
                            firstException = e;
                        else
                            firstException = new ProcessorStateException(
                                format("%sFailed to flush state store %s", logPrefix, store.name()), e);
                    }
                    log.error("Failed to flush state store {}: ", store.name(), e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * {@link StateStore#close() Close} all stores (even in case of failure).
     * Log all exception and re-throw the first exception that did occur at the end.
     *
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    @Override
    public void close(final boolean clean) throws ProcessorStateException {
        ProcessorStateException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!stores.isEmpty()) {
            log.debug("Closing its state manager and all the registered state stores");
            for (final Map.Entry<String, StateStoreMetadata> entry : stores.entrySet()) {
                final StateStore store = entry.getValue().stateStore;
                log.debug("Closing storage engine {}", store.name());
                try {
                    store.close();
                } catch (final RuntimeException e) {
                    if (firstException == null) {
                        firstException = new ProcessorStateException(format("%sFailed to close state store %s", logPrefix, store.name()), e);
                    }
                    log.error("Failed to close state store {}: ", store.name(), e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public void checkpoint(final Map<TopicPartition, Long> writtenOffsets) {
        // first update each state store's current offset, then checkpoint
        // those stores that are only logged and persistent to the checkpoint file
        for (final Map.Entry<TopicPartition, Long> entry : writtenOffsets.entrySet()) {
            final StateStoreMetadata store = findStore(entry.getKey());

            if (store != null) {
                store.offset = entry.getValue();

                log.debug("State store {} updated to written offset {} at changelog {}",
                    store.stateStore.name(), store.offset, store.changelogPartition);
            }
        }

        final Map<TopicPartition, Long> checkpointingOffsets = new HashMap<>();
        for (final StateStoreMetadata storeMetadata : stores.values()) {
            // store is logged, persistent, and has a valid current offset
            if (storeMetadata.changelogPartition != null &&
                storeMetadata.stateStore.persistent() &&
                storeMetadata.offset != null
            ) {
                checkpointingOffsets.put(storeMetadata.changelogPartition, storeMetadata.offset);
            }
        }

        log.debug("Writing checkpoint: {}", checkpointingOffsets);
        try {
            checkpointFile.write(checkpointingOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to [{}]", checkpointFile, e);
        }
    }
}
