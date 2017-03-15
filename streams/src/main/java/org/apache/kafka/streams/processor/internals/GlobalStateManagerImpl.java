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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.ProcessorStateManager.CHECKPOINT_FILE_NAME;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl implements GlobalStateManager {
    private static final int MAX_LOCK_ATTEMPTS = 5;
    private static final Logger log = LoggerFactory.getLogger(GlobalStateManagerImpl.class);

    private final ProcessorTopology topology;
    private final Consumer<byte[], byte[]> consumer;
    private final StateDirectory stateDirectory;
    private final Map<String, StateStore> stores = new LinkedHashMap<>();
    private final File baseDir;
    private final OffsetCheckpoint checkpoint;
    private final Set<String> globalStoreNames = new HashSet<>();
    private HashMap<TopicPartition, Long> checkpointableOffsets;

    public GlobalStateManagerImpl(final ProcessorTopology topology,
                           final Consumer<byte[], byte[]> consumer,
                           final StateDirectory stateDirectory) {
        this.topology = topology;
        this.consumer = consumer;
        this.stateDirectory = stateDirectory;
        this.baseDir = stateDirectory.globalStateDir();
        this.checkpoint = new OffsetCheckpoint(new File(this.baseDir, CHECKPOINT_FILE_NAME));
    }

    @Override
    public Set<String> initialize(final InternalProcessorContext processorContext) {
        try {
            if (!stateDirectory.lockGlobalState(MAX_LOCK_ATTEMPTS)) {
                throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir));
            }
        } catch (IOException e) {
            throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir));
        }

        try {
            this.checkpointableOffsets = new HashMap<>(checkpoint.read());
            checkpoint.delete();
        } catch (IOException e) {
            try {
                stateDirectory.unlockGlobalState();
            } catch (IOException e1) {
                log.error("failed to unlock the global state directory", e);
            }
            throw new StreamsException("Failed to read checkpoints for global state stores", e);
        }

        final List<StateStore> stateStores = topology.globalStateStores();
        for (final StateStore stateStore : stateStores) {
            globalStoreNames.add(stateStore.name());
            stateStore.init(processorContext, stateStore);
        }
        return Collections.unmodifiableSet(globalStoreNames);
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return stores.get(name);
    }

    @Override
    public StateStore getStore(final String name) {
        return getGlobalStore(name);
    }

    public File baseDir() {
        return baseDir;
    }

    public void register(final StateStore store,
                         final boolean ignored,
                         final StateRestoreCallback stateRestoreCallback) {

        if (stores.containsKey(store.name())) {
            throw new IllegalArgumentException(String.format("Global Store %s has already been registered", store.name()));
        }

        if (!globalStoreNames.contains(store.name())) {
            throw new IllegalArgumentException(String.format("Trying to register store %s that is not a known global store", store.name()));
        }

        if (stateRestoreCallback == null) {
            throw new IllegalArgumentException(String.format("The stateRestoreCallback provided for store %s was null", store.name()));
        }

        log.info("restoring state for global store {}", store.name());
        final List<TopicPartition> topicPartitions = topicPartitionsForStore(store);
        consumer.assign(topicPartitions);
        final Map<TopicPartition, Long> highWatermarks = consumer.endOffsets(topicPartitions);
        try {
            restoreState(stateRestoreCallback, topicPartitions, highWatermarks);
            stores.put(store.name(), store);
        } finally {
            consumer.assign(Collections.<TopicPartition>emptyList());
        }

    }

    private List<TopicPartition> topicPartitionsForStore(final StateStore store) {
        final String sourceTopic = topology.storeToChangelogTopic().get(store.name());
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(sourceTopic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new StreamsException(String.format("There are no partitions available for topic %s when initializing global store %s", sourceTopic, store.name()));
        }

        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partition : partitionInfos) {
            topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private void restoreState(final StateRestoreCallback stateRestoreCallback,
                              final List<TopicPartition> topicPartitions,
                              final Map<TopicPartition, Long> highWatermarks) {
        for (final TopicPartition topicPartition : topicPartitions) {
            consumer.assign(Collections.singletonList(topicPartition));
            final Long checkpoint = checkpointableOffsets.get(topicPartition);
            if (checkpoint != null) {
                consumer.seek(topicPartition, checkpoint);
            } else {
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            long offset = consumer.position(topicPartition);
            final Long highWatermark = highWatermarks.get(topicPartition);

            while (offset < highWatermark) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    offset = record.offset() + 1;
                    stateRestoreCallback.restore(record.key(), record.value());
                }
            }
            checkpointableOffsets.put(topicPartition, offset);
        }
    }

    public void flush(final InternalProcessorContext context) {
        log.debug("Flushing all global stores registered in the state manager");
        for (StateStore store : this.stores.values()) {
            try {
                log.trace("Flushing global store={}", store.name());
                store.flush();
            } catch (Exception e) {
                throw new ProcessorStateException(String.format("Failed to flush global state store %s", store.name()), e);
            }
        }
    }


    @Override
    public void close(final Map<TopicPartition, Long> offsets) throws IOException {
        try {
            if (stores.isEmpty()) {
                return;
            }
            final StringBuilder closeFailed = new StringBuilder();
            for (final Map.Entry<String, StateStore> entry : stores.entrySet()) {
                log.debug("Closing global storage engine {}", entry.getKey());
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    log.error("Failed to close global state store {}", entry.getKey(), e);
                    closeFailed.append("Failed to close global state store:")
                            .append(entry.getKey())
                            .append(". Reason: ")
                            .append(e.getMessage())
                            .append("\n");
                }
            }
            stores.clear();
            if (closeFailed.length() > 0) {
                throw new ProcessorStateException("Exceptions caught during close of 1 or more global state stores\n" + closeFailed);
            }
            writeCheckpoints(offsets);
        } finally {
            stateDirectory.unlockGlobalState();
        }
    }

    private void writeCheckpoints(final Map<TopicPartition, Long> offsets) {
        if (!offsets.isEmpty()) {
            checkpointableOffsets.putAll(offsets);
            try {
                checkpoint.write(checkpointableOffsets);
            } catch (IOException e) {
                log.warn("failed to write offsets checkpoint for global stores", e);
            }
        }
    }

    @Override
    public Map<TopicPartition, Long> checkpointedOffsets() {
        return Collections.unmodifiableMap(checkpointableOffsets);
    }


}
