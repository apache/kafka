/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;

public class ProcessorStateManager implements StateManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorStateManager.class);

    public static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
    public static final String CHECKPOINT_FILE_NAME = ".checkpoint";

    private final File baseDir;
    private final TaskId taskId;
    private final String logPrefix;
    private final boolean isStandby;
    private final StateDirectory stateDirectory;
    private final Map<String, StateStore> stores;
    private final Map<String, StateStore> globalStores;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final Map<TopicPartition, Long> offsetLimits;
    private final Map<TopicPartition, Long> restoredOffsets;
    private final Map<TopicPartition, Long> checkpointedOffsets;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name
    private final Map<String, String> storeToChangelogTopic;

    // TODO: this map does not work with customized grouper where multiple partitions
    // of the same topic can be assigned to the same topic.
    private final Map<String, TopicPartition> partitionForTopic;

    /**
     * @throws LockException if the state directory cannot be locked because another thread holds the lock
     *                       (this might be recoverable by retrying)
     * @throws IOException if any severe error happens while creating or locking the state directory
     */
    public ProcessorStateManager(final TaskId taskId,
                                 final Collection<TopicPartition> sources,
                                 final Consumer<byte[], byte[]> restoreConsumer,
                                 final boolean isStandby,
                                 final StateDirectory stateDirectory,
                                 final Map<String, String> storeToChangelogTopic) throws LockException, IOException {
        this.taskId = taskId;
        this.stateDirectory = stateDirectory;
        this.baseDir  = stateDirectory.directoryForTask(taskId);
        this.partitionForTopic = new HashMap<>();
        for (TopicPartition source : sources) {
            this.partitionForTopic.put(source.topic(), source);
        }
        this.stores = new LinkedHashMap<>();
        this.globalStores = new HashMap<>();
        this.restoreConsumer = restoreConsumer;
        this.offsetLimits = new HashMap<>();
        this.restoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        this.restoreCallbacks = isStandby ? new HashMap<String, StateRestoreCallback>() : null;
        this.storeToChangelogTopic = storeToChangelogTopic;

        this.logPrefix = String.format("task [%s]", taskId);

        if (!stateDirectory.lock(taskId, 5)) {
            throw new LockException(String.format("%s Failed to lock the state directory: %s", logPrefix, baseDir.getCanonicalPath()));
        }

        // load the checkpoint information
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
        this.checkpointedOffsets = new HashMap<>(checkpoint.read());

        // delete the checkpoint file after finish loading its stored offsets
        checkpoint.delete();
    }


    public static String storeChangelogTopic(String applicationId, String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    public File baseDir() {
        return this.baseDir;
    }

    /**
     * @throws IllegalArgumentException if the store name has already been registered or if it is not a valid name
     * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
     *
     * // TODO: parameter loggingEnabled can be removed now
     *
     * @throws StreamsException if the store's change log does not contain the partition
     */
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        log.debug("{} Registering state store {} to its state manager", logPrefix, store.name());

        if (store.name().equals(CHECKPOINT_FILE_NAME)) {
            throw new IllegalArgumentException(String.format("%s Illegal store name: %s", logPrefix, CHECKPOINT_FILE_NAME));
        }

        if (this.stores.containsKey(store.name())) {
            throw new IllegalArgumentException(String.format("%s Store %s has already been registered.", logPrefix, store.name()));
        }

        // check that the underlying change log topic exist or not
        String topic = storeToChangelogTopic.get(store.name());
        if (topic == null) {
            this.stores.put(store.name(), store);
            return;
        }

        // block until the partition is ready for this state changelog topic or time has elapsed
        int partition = getPartition(topic);
        boolean partitionNotFound = true;
        long startTime = System.currentTimeMillis();
        long waitTime = 5000L;      // hard-code the value since we should not block after KIP-4

        do {
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                // ignore
            }

            List<PartitionInfo> partitions;
            try {
                partitions = restoreConsumer.partitionsFor(topic);
            } catch (TimeoutException e) {
                throw new StreamsException(String.format("%s Could not fetch partition info for topic: %s before expiration of the configured request timeout", logPrefix, topic));
            }
            if (partitions == null) {
                throw new StreamsException(String.format("%s Could not find partition info for topic: %s", logPrefix, topic));
            }
            for (PartitionInfo partitionInfo : partitions) {
                if (partitionInfo.partition() == partition) {
                    partitionNotFound = false;
                    break;
                }
            }
        } while (partitionNotFound && System.currentTimeMillis() < startTime + waitTime);

        if (partitionNotFound) {
            throw new StreamsException(String.format("%s Store %s's change log (%s) does not contain partition %s",
                    logPrefix, store.name(), topic, partition));
        }

        if (isStandby) {
            if (store.persistent()) {
                log.trace("{} Preparing standby replica of persistent state store {} with changelog topic {}", logPrefix, store.name(), topic);

                restoreCallbacks.put(topic, stateRestoreCallback);
            }
        } else {
            log.trace("{} Restoring state store {} from changelog topic {}", logPrefix, store.name(), topic);

            restoreActiveState(topic, stateRestoreCallback);
        }

        this.stores.put(store.name(), store);
    }

    private void restoreActiveState(String topicName, StateRestoreCallback stateRestoreCallback) {
        // ---- try to restore the state from change-log ---- //

        // subscribe to the store's partition
        if (!restoreConsumer.subscription().isEmpty()) {
            throw new IllegalStateException(String.format("%s Restore consumer should have not subscribed to any partitions (%s) beforehand", logPrefix, restoreConsumer.subscription()));
        }
        TopicPartition storePartition = new TopicPartition(topicName, getPartition(topicName));
        restoreConsumer.assign(Collections.singletonList(storePartition));

        try {
            // calculate the end offset of the partition
            // TODO: this is a bit hacky to first seek then position to get the end offset
            restoreConsumer.seekToEnd(singleton(storePartition));
            long endOffset = restoreConsumer.position(storePartition);

            // restore from the checkpointed offset of the change log if it is persistent and the offset exists;
            // restore the state from the beginning of the change log otherwise
            if (checkpointedOffsets.containsKey(storePartition)) {
                restoreConsumer.seek(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                restoreConsumer.seekToBeginning(singleton(storePartition));
            }

            // restore its state from changelog records
            long limit = offsetLimit(storePartition);
            while (true) {
                long offset = 0L;
                for (ConsumerRecord<byte[], byte[]> record : restoreConsumer.poll(100).records(storePartition)) {
                    offset = record.offset();
                    if (offset >= limit) break;
                    stateRestoreCallback.restore(record.key(), record.value());
                }

                if (offset >= limit) {
                    break;
                } else if (restoreConsumer.position(storePartition) == endOffset) {
                    break;
                } else if (restoreConsumer.position(storePartition) > endOffset) {
                    // For a logging enabled changelog (no offset limit),
                    // the log end offset should not change while restoring since it is only written by this thread.
                    throw new IllegalStateException(String.format("%s Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
                            logPrefix, storePartition, endOffset, restoreConsumer.position(storePartition)));
                }
            }

            // record the restored offset for its change log partition
            long newOffset = Math.min(limit, restoreConsumer.position(storePartition));
            restoredOffsets.put(storePartition, newOffset);
        } finally {
            // un-assign the change log partition
            restoreConsumer.assign(Collections.<TopicPartition>emptyList());
        }
    }

    public Map<TopicPartition, Long> checkpointedOffsets() {
        Map<TopicPartition, Long> partitionsAndOffsets = new HashMap<>();

        for (Map.Entry<String, StateRestoreCallback> entry : restoreCallbacks.entrySet()) {
            String topicName = entry.getKey();
            int partition = getPartition(topicName);
            TopicPartition storePartition = new TopicPartition(topicName, partition);

            if (checkpointedOffsets.containsKey(storePartition)) {
                partitionsAndOffsets.put(storePartition, checkpointedOffsets.get(storePartition));
            } else {
                partitionsAndOffsets.put(storePartition, -1L);
            }
        }
        return partitionsAndOffsets;
    }

    public List<ConsumerRecord<byte[], byte[]>> updateStandbyStates(TopicPartition storePartition, List<ConsumerRecord<byte[], byte[]>> records) {
        long limit = offsetLimit(storePartition);
        List<ConsumerRecord<byte[], byte[]>> remainingRecords = null;

        // restore states from changelog records
        StateRestoreCallback restoreCallback = restoreCallbacks.get(storePartition.topic());

        long lastOffset = -1L;
        int count = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.offset() < limit) {
                try {
                    restoreCallback.restore(record.key(), record.value());
                } catch (Exception e) {
                    throw new ProcessorStateException(String.format("%s exception caught while trying to restore state from %s", logPrefix, storePartition), e);
                }
                lastOffset = record.offset();
            } else {
                if (remainingRecords == null)
                    remainingRecords = new ArrayList<>(records.size() - count);

                remainingRecords.add(record);
            }
            count++;
        }

        // record the restored offset for its change log partition
        restoredOffsets.put(storePartition, lastOffset + 1);

        return remainingRecords;
    }

    public void putOffsetLimit(TopicPartition partition, long limit) {
        offsetLimits.put(partition, limit);
    }

    private long offsetLimit(TopicPartition partition) {
        Long limit = offsetLimits.get(partition);
        return limit != null ? limit : Long.MAX_VALUE;
    }

    public StateStore getStore(String name) {
        return stores.get(name);
    }

    @Override
    public void flush(final InternalProcessorContext context) {
        if (!this.stores.isEmpty()) {
            log.debug("{} Flushing all stores registered in the state manager", logPrefix);
            for (StateStore store : this.stores.values()) {
                try {
                    log.trace("{} Flushing store={}", logPrefix, store.name());
                    store.flush();
                } catch (Exception e) {
                    throw new ProcessorStateException(String.format("%s Failed to flush state store %s", logPrefix, store.name()), e);
                }
            }
        }
    }

    /**
     * @throws IOException if any error happens when closing the state stores
     */
    @Override
    public void close(Map<TopicPartition, Long> ackedOffsets) throws IOException {
        try {
            // attempting to close the stores, just in case they
            // are not closed by a ProcessorNode yet
            if (!stores.isEmpty()) {
                log.debug("{} Closing its state manager and all the registered state stores", logPrefix);
                for (Map.Entry<String, StateStore> entry : stores.entrySet()) {
                    log.debug("{} Closing storage engine {}", logPrefix, entry.getKey());
                    try {
                        entry.getValue().close();
                    } catch (Exception e) {
                        throw new ProcessorStateException(String.format("%s Failed to close state store %s", logPrefix, entry.getKey()), e);
                    }
                }

                if (ackedOffsets != null) {
                    Map<TopicPartition, Long> checkpointOffsets = new HashMap<>();
                    for (String storeName : stores.keySet()) {
                        // only checkpoint the offset to the offsets file if
                        // it is persistent AND changelog enabled
                        if (stores.get(storeName).persistent() && storeToChangelogTopic.containsKey(storeName)) {
                            String changelogTopic = storeToChangelogTopic.get(storeName);
                            TopicPartition topicPartition = new TopicPartition(changelogTopic, getPartition(storeName));
                            Long offset = ackedOffsets.get(topicPartition);

                            if (offset != null) {
                                // store the last offset + 1 (the log position after restoration)
                                checkpointOffsets.put(topicPartition, offset + 1);
                            } else {
                                // if no record was produced. we need to check the restored offset.
                                offset = restoredOffsets.get(topicPartition);
                                if (offset != null)
                                    checkpointOffsets.put(topicPartition, offset);
                            }
                        }
                    }
                    // write the checkpoint file before closing, to indicate clean shutdown
                    OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
                    checkpoint.write(checkpointOffsets);
                }

            }
        } finally {
            // release the state directory directoryLock
            stateDirectory.unlock(taskId);
        }
    }

    private int getPartition(String topic) {
        TopicPartition partition = partitionForTopic.get(topic);

        return partition == null ? taskId.partition : partition.partition();
    }

    void registerGlobalStateStores(final List<StateStore> stateStores) {
        for (StateStore stateStore : stateStores) {
            globalStores.put(stateStore.name(), stateStore);
        }
    }

    public StateStore getGlobalStore(final String name) {
        return globalStores.get(name);
    }
}
