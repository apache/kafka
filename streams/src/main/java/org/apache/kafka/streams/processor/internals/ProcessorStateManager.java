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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class ProcessorStateManager implements StateManager {

    private static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
    static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    private final Logger log;
    private final File baseDir;
    private final TaskId taskId;
    private final String logPrefix;
    private final boolean isStandby;
    private final ChangelogReader changelogReader;
    private final Map<String, StateStore> stores;
    private final Map<String, StateStore> globalStores;
    private final Map<TopicPartition, Long> offsetLimits;
    private final Map<TopicPartition, Long> restoredOffsets;
    private final Map<TopicPartition, Long> checkpointedOffsets;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private final Map<String, String> storeToChangelogTopic;
    private final List<TopicPartition> changelogPartitions = new ArrayList<>();

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same topic.
    private final Map<String, TopicPartition> partitionForTopic;
    private OffsetCheckpoint checkpoint;

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
        this.taskId = taskId;
        this.changelogReader = changelogReader;
        logPrefix = String.format("task [%s] ", taskId);
        this.log = logContext.logger(getClass());

        partitionForTopic = new HashMap<>();
        for (final TopicPartition source : sources) {
            partitionForTopic.put(source.topic(), source);
        }
        stores = new LinkedHashMap<>();
        globalStores = new HashMap<>();
        offsetLimits = new HashMap<>();
        restoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        restoreCallbacks = isStandby ? new HashMap<String, StateRestoreCallback>() : null;
        this.storeToChangelogTopic = storeToChangelogTopic;

        baseDir = stateDirectory.directoryForTask(taskId);

        // load the checkpoint information
        checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointedOffsets = new HashMap<>(checkpoint.read());

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
        log.debug("Registering state store {} to its state manager", store.name());

        if (store.name().equals(CHECKPOINT_FILE_NAME)) {
            throw new IllegalArgumentException(String.format("%sIllegal store name: %s", logPrefix, CHECKPOINT_FILE_NAME));
        }

        if (stores.containsKey(store.name())) {
            throw new IllegalArgumentException(String.format("%sStore %s has already been registered.", logPrefix, store.name()));
        }

        // check that the underlying change log topic exist or not
        final String topic = storeToChangelogTopic.get(store.name());
        if (topic == null) {
            stores.put(store.name(), store);
            return;
        }

        final TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

        if (isStandby) {
            if (store.persistent()) {
                log.trace("Preparing standby replica of persistent state store {} with changelog topic {}", store.name(), topic);

                restoreCallbacks.put(topic, stateRestoreCallback);
            }
        } else {
            log.trace("Restoring state store {} from changelog topic {}", store.name(), topic);
            final StateRestorer restorer = new StateRestorer(storePartition,
                                                             new CompositeRestoreListener(stateRestoreCallback),
                                                             checkpointedOffsets.get(storePartition),
                                                             offsetLimit(storePartition),
                                                             store.persistent(),
                                                             store.name());

            changelogReader.register(restorer);
        }
        changelogPartitions.add(storePartition);

        stores.put(store.name(), store);
    }

    @Override
    public Map<TopicPartition, Long> checkpointed() {
        final Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (final Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            final String topicName = entry.getKey();
            final int partition = getPartition(topicName);
            final TopicPartition storePartition = new TopicPartition(topicName, partition);

            if (checkpointedOffsets.containsKey(storePartition)) {
                partitionsAndOffsets.put(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                partitionsAndOffsets.put(storePartition, -1L);
            }
        }
        return partitionsAndOffsets;
    }

    List<ConsumerRecord<byte[], byte[]>> updateStandbyStates(final TopicPartition storePartition,
                                                             final List<ConsumerRecord<byte[], byte[]>> records) {
        final long limit = offsetLimit(storePartition);
        List<ConsumerRecord<byte[], byte[]>> remainingRecords = null;
        final List<KeyValue<byte[], byte[]>> restoreRecords = new ArrayList<>();

        // restore states from changelog records
        final BatchingStateRestoreCallback restoreCallback = getBatchingRestoreCallback(restoreCallbacks.get(storePartition.topic()));

        long lastOffset = -1L;
        int count = 0;
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            if (record.offset() < limit) {
                restoreRecords.add(KeyValue.pair(record.key(), record.value()));
                lastOffset = record.offset();
            } else {
                if (remainingRecords == null) {
                    remainingRecords = new ArrayList<>(records.size() - count);
                }

                remainingRecords.add(record);
            }
            count++;
        }

        if (!restoreRecords.isEmpty()) {
            try {
                restoreCallback.restoreAll(restoreRecords);
            } catch (final Exception e) {
                throw new ProcessorStateException(String.format("%sException caught while trying to restore state from %s", logPrefix, storePartition), e);
            }
        }

        // record the restored offset for its change log partition
        restoredOffsets.put(storePartition, lastOffset + 1);

        return remainingRecords;
    }

    void putOffsetLimit(final TopicPartition partition, final long limit) {
        log.trace("Updating store offset limit for partition {} to {}", partition, limit);
        offsetLimits.put(partition, limit);
    }

    private long offsetLimit(final TopicPartition partition) {
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

        }

        if (firstException != null) {
            throw firstException;
        }
    }

    // write the checkpoint
    @Override
    public void checkpoint(final Map<TopicPartition, Long> ackedOffsets) {
        log.trace("Writing checkpoint: {}", ackedOffsets);
        checkpointedOffsets.putAll(changelogReader.restoredOffsets());
        for (final StateStore store : stores.values()) {
            final String storeName = store.name();
            // only checkpoint the offset to the offsets file if
            // it is persistent AND changelog enabled
            if (store.persistent() && storeToChangelogTopic.containsKey(storeName)) {
                final String changelogTopic = storeToChangelogTopic.get(storeName);
                final TopicPartition topicPartition = new TopicPartition(changelogTopic, getPartition(storeName));
                if (ackedOffsets.containsKey(topicPartition)) {
                    // store the last offset + 1 (the log position after restoration)
                    checkpointedOffsets.put(topicPartition, ackedOffsets.get(topicPartition) + 1);
                } else if (restoredOffsets.containsKey(topicPartition)) {
                    checkpointedOffsets.put(topicPartition, restoredOffsets.get(topicPartition));
                }
            }
        }
        // write the checkpoint file before closing, to indicate clean shutdown
        try {
            if (checkpoint == null) {
                checkpoint = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
            }
            checkpoint.write(checkpointedOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write checkpoint file to {}:", new File(baseDir, CHECKPOINT_FILE_NAME), e);
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

    private BatchingStateRestoreCallback getBatchingRestoreCallback(StateRestoreCallback callback) {
        if (callback instanceof BatchingStateRestoreCallback) {
            return (BatchingStateRestoreCallback) callback;
        }

        return new WrappedBatchingStateRestoreCallback(callback);
    }

    Collection<TopicPartition> changelogPartitions() {
        return changelogPartitions;
    }
}
