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
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;

public class ProcessorStateManager {

    private static final Logger log = LoggerFactory.getLogger(ProcessorStateManager.class);

    public static final String STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";
    public static final String CHECKPOINT_FILE_NAME = ".checkpoint";
    public static final String LOCK_FILE_NAME = ".lock";

    private final String applicationId;
    private final int defaultPartition;
    private final Map<String, TopicPartition> partitionForTopic;
    private final File baseDir;
    private final FileLock directoryLock;
    private final Map<String, StateStore> stores;
    private final Set<String> loggingEnabled;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final Map<TopicPartition, Long> restoredOffsets;
    private final Map<TopicPartition, Long> checkpointedOffsets;
    private final Map<TopicPartition, Long> offsetLimits;
    private final boolean isStandby;
    private final Map<String, StateRestoreCallback> restoreCallbacks; // used for standby tasks, keyed by state topic name

    /**
     * @throws IOException if any error happens while creating or locking the state directory
     */
    public ProcessorStateManager(String applicationId, int defaultPartition, Collection<TopicPartition> sources, File baseDir, Consumer<byte[], byte[]> restoreConsumer, boolean isStandby) throws IOException {
        this.applicationId = applicationId;
        this.defaultPartition = defaultPartition;
        this.partitionForTopic = new HashMap<>();
        for (TopicPartition source : sources) {
            this.partitionForTopic.put(source.topic(), source);
        }
        this.baseDir = baseDir;
        this.stores = new HashMap<>();
        this.loggingEnabled = new HashSet<>();
        this.restoreConsumer = restoreConsumer;
        this.restoredOffsets = new HashMap<>();
        this.isStandby = isStandby;
        this.restoreCallbacks = isStandby ? new HashMap<String, StateRestoreCallback>() : null;
        this.offsetLimits = new HashMap<>();

        // create the state directory for this task if missing (we won't create the parent directory)
        createStateDirectory(baseDir);

        // try to acquire the exclusive lock on the state directory
        directoryLock = lockStateDirectory(baseDir, 5);
        if (directoryLock == null) {
            throw new IOException("Failed to lock the state directory: " + baseDir.getCanonicalPath());
        }

        // load the checkpoint information
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
        this.checkpointedOffsets = new HashMap<>(checkpoint.read());

        // delete the checkpoint file after finish loading its stored offsets
        checkpoint.delete();
    }

    private static void createStateDirectory(File stateDir) throws IOException {
        if (!stateDir.exists()) {
            stateDir.mkdir();
        }
    }

    public static String storeChangelogTopic(String applicationId, String storeName) {
        return applicationId + "-" + storeName + STATE_CHANGELOG_TOPIC_SUFFIX;
    }

    /**
     * @throws IOException if any error happens when locking the state directory
     */
    public static FileLock lockStateDirectory(File stateDir) throws IOException {
        return lockStateDirectory(stateDir, 0);
    }

    private static FileLock lockStateDirectory(File stateDir, int retry) throws IOException {
        File lockFile = new File(stateDir, ProcessorStateManager.LOCK_FILE_NAME);
        FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();

        FileLock lock = lockStateDirectory(channel);
        while (lock == null && retry > 0) {
            try {
                Thread.sleep(200);
            } catch (Exception ex) {
                // do nothing
            }
            retry--;
            lock = lockStateDirectory(channel);
        }
        if (lock == null) {
            channel.close();
        }
        return lock;
    }

    private static FileLock lockStateDirectory(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

    public File baseDir() {
        return this.baseDir;
    }

    /**
     * @throws IllegalArgumentException if the store name has already been registered or if it is not a valid name
     * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
     * @throws StreamsException if the store's change log does not contain the partition
     */
    public void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback) {
        if (store.name().equals(CHECKPOINT_FILE_NAME))
            throw new IllegalArgumentException("Illegal store name: " + CHECKPOINT_FILE_NAME);

        if (this.stores.containsKey(store.name()))
            throw new IllegalArgumentException("Store " + store.name() + " has already been registered.");

        if (loggingEnabled)
            this.loggingEnabled.add(store.name());

        // check that the underlying change log topic exist or not
        String topic;
        if (loggingEnabled)
            topic = storeChangelogTopic(this.applicationId, store.name());
        else topic = store.name();

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

            for (PartitionInfo partitionInfo : restoreConsumer.partitionsFor(topic)) {
                if (partitionInfo.partition() == partition) {
                    partitionNotFound = false;
                    break;
                }
            }
        } while (partitionNotFound && System.currentTimeMillis() < startTime + waitTime);

        if (partitionNotFound)
            throw new StreamsException("Store " + store.name() + "'s change log (" + topic + ") does not contain partition " + partition);

        this.stores.put(store.name(), store);

        if (isStandby) {
            if (store.persistent())
                restoreCallbacks.put(topic, stateRestoreCallback);
        } else {
            restoreActiveState(topic, stateRestoreCallback);
        }
    }

    private void restoreActiveState(String topicName, StateRestoreCallback stateRestoreCallback) {
        // ---- try to restore the state from change-log ---- //

        // subscribe to the store's partition
        if (!restoreConsumer.subscription().isEmpty()) {
            throw new IllegalStateException("Restore consumer should have not subscribed to any partitions beforehand");
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
                    throw new IllegalStateException("Log end offset should not change while restoring");
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
                restoreCallback.restore(record.key(), record.value());
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

    public void flush() {
        if (!this.stores.isEmpty()) {
            log.debug("Flushing stores.");
            for (StateStore store : this.stores.values())
                store.flush();
        }
    }

    /**
     * @throws IOException if any error happens when flushing or closing the state stores
     */
    public void close(Map<TopicPartition, Long> ackedOffsets) throws IOException {
        try {
            if (!stores.isEmpty()) {
                log.debug("Closing stores.");
                for (Map.Entry<String, StateStore> entry : stores.entrySet()) {
                    log.debug("Closing storage engine {}", entry.getKey());
                    entry.getValue().flush();
                    entry.getValue().close();
                }

                Map<TopicPartition, Long> checkpointOffsets = new HashMap<>();
                for (String storeName : stores.keySet()) {
                    TopicPartition part;
                    if (loggingEnabled.contains(storeName))
                        part = new TopicPartition(storeChangelogTopic(applicationId, storeName), getPartition(storeName));
                    else
                        part = new TopicPartition(storeName, getPartition(storeName));

                    // only checkpoint the offset to the offsets file if it is persistent;
                    if (stores.get(storeName).persistent()) {
                        Long offset = ackedOffsets.get(part);

                        if (offset != null) {
                            // store the last offset + 1 (the log position after restoration)
                            checkpointOffsets.put(part, offset + 1);
                        } else {
                            // if no record was produced. we need to check the restored offset.
                            offset = restoredOffsets.get(part);
                            if (offset != null)
                                checkpointOffsets.put(part, offset);
                        }
                    }
                }

                // write the checkpoint file before closing, to indicate clean shutdown
                OffsetCheckpoint checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
                checkpoint.write(checkpointOffsets);
            }
        } finally {
            // release the state directory directoryLock
            directoryLock.release();
            directoryLock.channel().close();
        }
    }

    private int getPartition(String topic) {
        TopicPartition partition = partitionForTopic.get(topic);

        return partition == null ? defaultPartition : partition.partition();
    }
}
