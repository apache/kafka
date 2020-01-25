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

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.StateRestoreCallbackAdapter.adapt;


public class ProcessorStateManager implements StateManager {
    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private final Logger log;
    private final TaskId taskId;
    private final String logPrefix;
    private final boolean isStandby;
    private final ChangelogReader changelogReader;
    private final Map<TopicPartition, Long> offsetLimits;
    private final Map<TopicPartition, Long> standbyRestoredOffsets;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private final Map<String, RecordConverter> recordConverters; // used for standby tasks, keyed by state topic name
    private final Map<String, String> storeToChangelogTopic;

    // must be maintained in topological order
    private final FixedOrderMap<String, Optional<StateStore>> registeredStores = new FixedOrderMap<>();
    private final FixedOrderMap<String, Optional<StateStore>> globalStores = new FixedOrderMap<>();

    private final List<TopicPartition> changelogPartitions = new ArrayList<>();

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same task.
    private final Map<String, TopicPartition> partitionForTopic;

    private final boolean eosEnabled;
    private final File baseDir;
    private OffsetCheckpoint checkpointFile;
    private final Map<TopicPartition, Long> checkpointFileCache = new HashMap<>();
    private final Map<TopicPartition, Long> initialLoadedCheckpoints;

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     * @throws IOException             if any severe error happens while creating or locking the state directory
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final boolean isStandby,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic,
                                 final ChangelogReader changelogReader,
                                 final boolean eosEnabled,
                                 final LogContext logContext) throws IOException {
        this.eosEnabled = eosEnabled;

        log = logContext.logger(ProcessorStateManager.class);
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        logPrefix = String.format("task [%s] ", taskId);

        partitionForTopic = new HashMap<>();
        for (final TopicPartition source : sources) {
            partitionForTopic.put(source.topic(), source);
        }
        offsetLimits = new HashMap<>();
        standbyRestoredOffsets = new ConcurrentHashMap<>();
        this.isStandby = isStandby;
        restoreCallbacks = isStandby ? new ConcurrentHashMap<>() : null;
        recordConverters = isStandby ? new HashMap<>() : null;
        this.storeToChangelogTopic = new HashMap<>(storeToChangelogTopic);

        baseDir = stateDirectory.directoryForTask(taskId);
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        initialLoadedCheckpoints = checkpointFile.read();

        log.trace("Checkpointable offsets read from checkpoint: {}", initialLoadedCheckpoints);

        if (eosEnabled) {
            // with EOS enabled, there should never be a checkpoint file _during_ processing.
            // delete the checkpoint file after loading its stored offsets.
            checkpointFile.delete();
            checkpointFile = null;
        }

        log.debug("Created state store manager for task {}", taskId);
    }


    public static String storeChangelogTopic(final String applicationId,
                                             final String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    @Override
    public File baseDir() {
        return baseDir;
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        final String storeName = store.name();
        log.debug("Registering state store {} to its state manager", storeName);

        if (CHECKPOINT_FILE_NAME.equals(storeName)) {
            throw new IllegalArgumentException(String.format("%sIllegal store name: %s", logPrefix, storeName));
        }

        if (registeredStores.containsKey(storeName) && registeredStores.get(storeName).isPresent()) {
            throw new IllegalArgumentException(String.format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        // check that the underlying change log topic exist or not
        final String topic = storeToChangelogTopic.get(storeName);
        if (topic != null) {
            final TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

            final RecordConverter recordConverter = converterForStore(store);

            if (isStandby) {
                log.trace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);

                restoreCallbacks.put(topic, stateRestoreCallback);
                recordConverters.put(topic, recordConverter);
            } else {
                final Long restoreCheckpoint = store.persistent() ? initialLoadedCheckpoints.get(storePartition) : null;
                if (restoreCheckpoint != null) {
                    checkpointFileCache.put(storePartition, restoreCheckpoint);
                }
                log.trace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

                final StateRestorer restorer = new StateRestorer(
                    storePartition,
                    new CompositeRestoreListener(stateRestoreCallback),
                    restoreCheckpoint,
                    offsetLimit(storePartition),
                    store.persistent(),
                    storeName,
                    recordConverter
                );

                changelogReader.register(restorer);
            }
            changelogPartitions.add(storePartition);
        }

        registeredStores.put(storeName, Optional.of(store));
    }

    @Override
    public void reinitializeStateStoresForPartitions(final Collection<TopicPartition> partitions,
                                                     final InternalProcessorContext processorContext) {
        StateManagerUtil.reinitializeStateStoresForPartitions(log,
                                                              eosEnabled,
                                                              baseDir,
                                                              registeredStores,
                                                              storeToChangelogTopic,
                                                              partitions,
                                                              processorContext,
                                                              checkpointFile,
                                                              checkpointFileCache
        );
    }

    void clearCheckpoints() throws IOException {
        if (checkpointFile != null) {
            checkpointFile.delete();
            checkpointFile = null;

            checkpointFileCache.clear();
        }
    }

    @Override
    public Map<TopicPartition, Long> checkpointed() {
        updateCheckpointFileCache(emptyMap());
        final Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (final Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            final String topicName = entry.getKey();
            final int partition = getPartition(topicName);
            final TopicPartition storePartition = new TopicPartition(topicName, partition);

            partitionsAndOffsets.put(storePartition, checkpointFileCache.getOrDefault(storePartition, -1L));
        }
        return partitionsAndOffsets;
    }

    void updateStandbyStates(final TopicPartition storePartition,
                             final List<ConsumerRecord<byte[], byte[]>> restoreRecords,
                             final long lastOffset) {
        // restore states from changelog records
        final RecordBatchingStateRestoreCallback restoreCallback = adapt(restoreCallbacks.get(storePartition.topic()));

        if (!restoreRecords.isEmpty()) {
            final RecordConverter converter = recordConverters.get(storePartition.topic());
            final List<ConsumerRecord<byte[], byte[]>> convertedRecords = new ArrayList<>(restoreRecords.size());
            for (final ConsumerRecord<byte[], byte[]> record : restoreRecords) {
                convertedRecords.add(converter.convert(record));
            }

            try {
                restoreCallback.restoreBatch(convertedRecords);
            } catch (final RuntimeException e) {
                throw new ProcessorStateException(String.format("%sException caught while trying to restore state from %s", logPrefix, storePartition), e);
            }
        }

        // record the restored offset for its change log partition
        standbyRestoredOffsets.put(storePartition, lastOffset + 1);
    }

    void putOffsetLimits(final Map<TopicPartition, Long> offsets) {
        log.trace("Updating store offset limit with {}", offsets);
        offsetLimits.putAll(offsets);
    }

    private long offsetLimit(final TopicPartition partition) {
        final Long limit = offsetLimits.get(partition);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    ChangelogReader changelogReader() {
        return changelogReader;
    }

    @Override
    public StateStore getStore(final String name) {
        return registeredStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    @Override
    public void flush() {
        ProcessorStateException firstException = null;
        // attempting to flush the stores
        if (!registeredStores.isEmpty()) {
            log.debug("Flushing all stores registered in the state manager");
            for (final Map.Entry<String, Optional<StateStore>> entry : registeredStores.entrySet()) {
                if (entry.getValue().isPresent()) {
                    final StateStore store = entry.getValue().get();
                    log.trace("Flushing store {}", store.name());
                    try {
                        store.flush();
                    } catch (final RuntimeException e) {
                        if (firstException == null) {
                            firstException = new ProcessorStateException(String.format("%sFailed to flush state store %s", logPrefix, store.name()), e);
                        }
                        log.error("Failed to flush state store {}: ", store.name(), e);
                    }
                } else {
                    throw new IllegalStateException("Expected " + entry.getKey() + " to have been initialized");
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
        if (!registeredStores.isEmpty()) {
            log.debug("Closing its state manager and all the registered state stores");
            for (final Map.Entry<String, Optional<StateStore>> entry : registeredStores.entrySet()) {
                if (entry.getValue().isPresent()) {
                    final StateStore store = entry.getValue().get();
                    log.debug("Closing storage engine {}", store.name());
                    try {
                        store.close();
                        registeredStores.put(store.name(), Optional.empty());
                    } catch (final RuntimeException e) {
                        if (firstException == null) {
                            firstException = new ProcessorStateException(String.format("%sFailed to close state store %s", logPrefix, store.name()), e);
                        }
                        log.error("Failed to close state store {}: ", store.name(), e);
                    }
                } else {
                    log.info("Skipping to close non-initialized store {}", entry.getKey());
                }
            }
        }

        if (!clean && eosEnabled) {
            // delete the checkpoint file if this is an unclean close
            try {
                clearCheckpoints();
            } catch (final IOException e) {
                throw new ProcessorStateException(String.format("%sError while deleting the checkpoint file", logPrefix), e);
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public void checkpoint(final Map<TopicPartition, Long> checkpointableOffsetsFromProcessing) {
        ensureStoresRegistered();

        // write the checkpoint file before closing
        if (checkpointFile == null) {
            checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        }

        updateCheckpointFileCache(checkpointableOffsetsFromProcessing);

        log.debug("Writing checkpoint: {}", checkpointFileCache);
        try {
            checkpointFile.write(checkpointFileCache);
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to [{}]", checkpointFile, e);
        }
    }

    private void updateCheckpointFileCache(final Map<TopicPartition, Long> checkpointableOffsetsFromProcessing) {
        final Set<TopicPartition> validCheckpointableTopics = validCheckpointableTopics();
        final Map<TopicPartition, Long> restoredOffsets = validCheckpointableOffsets(
            changelogReader.restoredOffsets(),
            validCheckpointableTopics
        );
        log.trace("Checkpointable offsets updated with restored offsets: {}", checkpointFileCache);
        for (final TopicPartition topicPartition : validCheckpointableTopics) {
            if (checkpointableOffsetsFromProcessing.containsKey(topicPartition)) {
                // if we have just recently processed some offsets,
                // store the last offset + 1 (the log position after restoration)
                checkpointFileCache.put(topicPartition, checkpointableOffsetsFromProcessing.get(topicPartition) + 1);
            } else if (standbyRestoredOffsets.containsKey(topicPartition)) {
                // or if we restored some offset as a standby task, use it
                checkpointFileCache.put(topicPartition, standbyRestoredOffsets.get(topicPartition));
            } else if (restoredOffsets.containsKey(topicPartition)) {
                // or if we restored some offset as an active task, use it
                checkpointFileCache.put(topicPartition, restoredOffsets.get(topicPartition));
            } else if (checkpointFileCache.containsKey(topicPartition)) {
                // or if we have a prior value we've cached (and written to the checkpoint file), then keep it
            } else {
                // As a last resort, fall back to the offset we loaded from the checkpoint file at startup, but
                // only if the offset is actually valid for our current state stores.
                final Long loadedOffset =
                    validCheckpointableOffsets(initialLoadedCheckpoints, validCheckpointableTopics).get(topicPartition);
                if (loadedOffset != null) {
                    checkpointFileCache.put(topicPartition, loadedOffset);
                }
            }
        }
    }

    private int getPartition(final String topic) {
        final TopicPartition partition = partitionForTopic.get(topic);
        return partition == null ? taskId.partition : partition.partition();
    }

    void registerGlobalStateStores(final List<StateStore> stateStores) {
        log.debug("Register global stores {}", stateStores);
        for (final StateStore stateStore : stateStores) {
            globalStores.put(stateStore.name(), Optional.of(stateStore));
        }
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return globalStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    Collection<TopicPartition> changelogPartitions() {
        return unmodifiableList(changelogPartitions);
    }

    void ensureStoresRegistered() {
        for (final Map.Entry<String, Optional<StateStore>> entry : registeredStores.entrySet()) {
            if (!entry.getValue().isPresent()) {
                throw new IllegalStateException(
                    "store [" + entry.getKey() + "] has not been correctly registered. This is a bug in Kafka Streams."
                );
            }
        }
    }

    private Set<TopicPartition> validCheckpointableTopics() {
        // it's only valid to record checkpoints for registered stores that are both persistent and change-logged

        final Set<TopicPartition> result = new HashSet<>(storeToChangelogTopic.size());
        for (final Map.Entry<String, String> storeToChangelog : storeToChangelogTopic.entrySet()) {
            final String storeName = storeToChangelog.getKey();
            if (registeredStores.containsKey(storeName)
                && registeredStores.get(storeName).isPresent()
                && registeredStores.get(storeName).get().persistent()) {

                final String changelogTopic = storeToChangelog.getValue();
                result.add(new TopicPartition(changelogTopic, getPartition(changelogTopic)));
            }
        }
        return result;
    }

    private static Map<TopicPartition, Long> validCheckpointableOffsets(
        final Map<TopicPartition, Long> checkpointableOffsets,
        final Set<TopicPartition> validCheckpointableTopics) {

        final Map<TopicPartition, Long> result = new HashMap<>(checkpointableOffsets.size());

        for (final Map.Entry<TopicPartition, Long> topicToCheckpointableOffset : checkpointableOffsets.entrySet()) {
            final TopicPartition topic = topicToCheckpointableOffset.getKey();
            if (validCheckpointableTopics.contains(topic)) {
                final Long checkpointableOffset = topicToCheckpointableOffset.getValue();
                result.put(topic, checkpointableOffset);
            }
        }

        return result;
    }

    Map<TopicPartition, Long> standbyRestoredOffsets() {
        return Collections.unmodifiableMap(standbyRestoredOffsets);
    }
}
