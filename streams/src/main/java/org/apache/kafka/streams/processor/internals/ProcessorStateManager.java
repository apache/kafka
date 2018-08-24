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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessorStateManager extends AbstractStateManager {
    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

    private final Logger log;
    private final TaskId taskId;
    private final String logPrefix;
    private final boolean isStandby;
    private final ChangelogReader changelogReader;
    private final Map<TopicPartition, Long> offsetLimits;
    private final Map<TopicPartition, Long> standbyRestoredOffsets;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private final Map<String, String> storeToChangelogTopic;
    private final List<TopicPartition> changelogPartitions = new ArrayList<>();

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same topic.
    private final Map<String, TopicPartition> partitionForTopic;

    /**
     * @throws ProcessorStateException if the task directory does not exist and could not be created
     * @throws IOException if any severe error happens while creating or locking the state directory
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final boolean isStandby,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic,
                                 final ChangelogReader changelogReader,
                                 final boolean eosEnabled,
                                 final LogContext logContext) throws IOException {
        super(stateDirectory.directoryForTask(taskId), eosEnabled);

        this.log = logContext.logger(ProcessorStateManager.class);
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        logPrefix = String.format("task [%s] ", taskId);

        partitionForTopic = new HashMap<>();
        for (final TopicPartition source : sources) {
            partitionForTopic.put(source.topic(), source);
        }
        offsetLimits = new HashMap<>();
        standbyRestoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        restoreCallbacks = isStandby ? new HashMap<>() : null;
        this.storeToChangelogTopic = storeToChangelogTopic;

        // load the checkpoint information
        checkpointableOffsets.putAll(checkpoint.read());
        if (eosEnabled) {
            // delete the checkpoint file after finish loading its stored offsets
            checkpoint.delete();
            checkpoint = null;
        }

        log.debug("Created state store manager for task {} with the acquired state dir lock", taskId);
    }


    public static String storeChangelogTopic(final String applicationId, final String storeName) {
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
            throw new IllegalArgumentException(String.format("%sIllegal store name: %s", logPrefix, CHECKPOINT_FILE_NAME));
        }

        if (stores.containsKey(storeName)) {
            throw new IllegalArgumentException(String.format("%sStore %s has already been registered.", logPrefix, storeName));
        }

        // check that the underlying change log topic exist or not
        final String topic = storeToChangelogTopic.get(storeName);
        if (topic == null) {
            stores.put(storeName, store);
            return;
        }

        final TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

        if (isStandby) {
            log.trace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);
            restoreCallbacks.put(topic, stateRestoreCallback);

        } else {
            log.trace("Restoring state store {} from changelog topic {}", storeName, topic);
            final StateRestorer restorer = new StateRestorer(storePartition,
                                                             new CompositeRestoreListener(stateRestoreCallback),
                                                             checkpointableOffsets.get(storePartition),
                                                             offsetLimit(storePartition),
                                                             store.persistent(),
                storeName);

            changelogReader.register(restorer);
        }
        changelogPartitions.add(storePartition);

        stores.put(storeName, store);
    }

    @Override
    public void reinitializeStateStoresForPartitions(final Collection<TopicPartition> partitions,
                                                     final InternalProcessorContext processorContext) {
        super.reinitializeStateStoresForPartitions(
            log,
            stores,
            storeToChangelogTopic,
            partitions,
            processorContext);
    }

    @Override
    public Map<TopicPartition, Long> checkpointed() {
        final Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (final Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            final String topicName = entry.getKey();
            final int partition = getPartition(topicName);
            final TopicPartition storePartition = new TopicPartition(topicName, partition);

            partitionsAndOffsets.put(storePartition, checkpointableOffsets.getOrDefault(storePartition, -1L));
        }
        return partitionsAndOffsets;
    }

    void updateStandbyStates(final TopicPartition storePartition,
                             final List<KeyValue<byte[], byte[]>> restoreRecords,
                             final long lastOffset) {
        // restore states from changelog records
        final BatchingStateRestoreCallback restoreCallback = getBatchingRestoreCallback(restoreCallbacks.get(storePartition.topic()));

        if (!restoreRecords.isEmpty()) {
            try {
                restoreCallback.restoreAll(restoreRecords);
            } catch (final Exception e) {
                throw new ProcessorStateException(String.format("%sException caught while trying to restore state from %s", logPrefix, storePartition), e);
            }
        }

        // record the restored offset for its change log partition
        standbyRestoredOffsets.put(storePartition, lastOffset + 1);
    }

    void putOffsetLimit(final TopicPartition partition, final long limit) {
        log.trace("Updating store offset limit for partition {} to {}", partition, limit);
        offsetLimits.put(partition, limit);
    }

    long offsetLimit(final TopicPartition partition) {
        final Long limit = offsetLimits.get(partition);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    @Override
    public StateStore getStore(final String name) {
        return stores.get(name);
    }

    @Override
    public void flush() {
        ProcessorStateException firstException = null;
        // attempting to flush the stores
        if (!stores.isEmpty()) {
            log.debug("Flushing all stores registered in the state manager");
            for (final StateStore store : stores.values()) {
                log.trace("Flushing store {}", store.name());
                try {
                    store.flush();
                } catch (final Exception e) {
                    if (firstException == null) {
                        firstException = new ProcessorStateException(String.format("%sFailed to flush state store %s", logPrefix, store.name()), e);
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
     * @throws ProcessorStateException if any error happens when closing the state stores
     */
    @Override
    public void close(final Map<TopicPartition, Long> ackedOffsets) throws ProcessorStateException {
        ProcessorStateException firstException = null;
        // attempting to close the stores, just in case they
        // are not closed by a ProcessorNode yet
        if (!stores.isEmpty()) {
            log.debug("Closing its state manager and all the registered state stores");
            for (final StateStore store : stores.values()) {
                log.debug("Closing storage engine {}", store.name());
                try {
                    store.close();
                } catch (final Exception e) {
                    if (firstException == null) {
                        firstException = new ProcessorStateException(String.format("%sFailed to close state store %s", logPrefix, store.name()), e);
                    }
                    log.error("Failed to close state store {}: ", store.name(), e);
                }
            }

            if (ackedOffsets != null) {
                checkpoint(ackedOffsets);
            }
            stores.clear();
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    // write the checkpoint
    @Override
    public void checkpoint(final Map<TopicPartition, Long> checkpointableOffsets) {
        this.checkpointableOffsets.putAll(changelogReader.restoredOffsets());
        for (final StateStore store : stores.values()) {
            final String storeName = store.name();
            // only checkpoint the offset to the offsets file if
            // it is persistent AND changelog enabled
            if (store.persistent() && storeToChangelogTopic.containsKey(storeName)) {
                final String changelogTopic = storeToChangelogTopic.get(storeName);
                final TopicPartition topicPartition = new TopicPartition(changelogTopic, getPartition(storeName));
                if (checkpointableOffsets.containsKey(topicPartition)) {
                    // store the last offset + 1 (the log position after restoration)
                    this.checkpointableOffsets.put(topicPartition, checkpointableOffsets.get(topicPartition) + 1);
                } else if (standbyRestoredOffsets.containsKey(topicPartition)) {
                    this.checkpointableOffsets.put(topicPartition, standbyRestoredOffsets.get(topicPartition));
                }
            }
        }
        // write the checkpoint file before closing
        if (checkpoint == null) {
            checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        }

        log.trace("Writing checkpoint: {}", this.checkpointableOffsets);
        try {
            checkpoint.write(this.checkpointableOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to {}: {}", checkpoint, e);
        }
    }

    private int getPartition(final String topic) {
        final TopicPartition partition = partitionForTopic.get(topic);
        return partition == null ? taskId.partition : partition.partition();
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

    private BatchingStateRestoreCallback getBatchingRestoreCallback(final StateRestoreCallback callback) {
        if (callback instanceof BatchingStateRestoreCallback) {
            return (BatchingStateRestoreCallback) callback;
        }

        return new WrappedBatchingStateRestoreCallback(callback);
    }

    Collection<TopicPartition> changelogPartitions() {
        return changelogPartitions;
    }
}
