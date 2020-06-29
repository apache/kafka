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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.streams.state.internals.RecordConverter;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl implements GlobalStateManager {
    private final Logger log;
    private final ProcessorTopology topology;
    private final Consumer<byte[], byte[]> globalConsumer;
    private final File baseDir;
    private final StateDirectory stateDirectory;
    private final Set<String> globalStoreNames = new HashSet<>();
    private final FixedOrderMap<String, Optional<StateStore>> globalStores = new FixedOrderMap<>();
    private final StateRestoreListener stateRestoreListener;
    private InternalProcessorContext globalProcessorContext;
    private final int retries;
    private final long retryBackoffMs;
    private final Duration pollTime;
    private final Set<String> globalNonPersistentStoresTopics = new HashSet<>();
    private final OffsetCheckpoint checkpointFile;
    private final Map<TopicPartition, Long> checkpointFileCache;
    private final Map<String, RecordDeserializer> deserializers = new HashMap<>();
    private final LogContext logContext;
    private final DeserializationExceptionHandler deserializationExceptionHandler;

    public GlobalStateManagerImpl(final LogContext logContext,
                                  final ProcessorTopology topology,
                                  final Consumer<byte[], byte[]> globalConsumer,
                                  final StateDirectory stateDirectory,
                                  final StateRestoreListener stateRestoreListener,
                                  final StreamsConfig config) {
        baseDir = stateDirectory.globalStateDir();
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointFileCache = new HashMap<>();

        // Find non persistent store's topics
        final Map<String, String> storeToChangelogTopic = topology.storeToChangelogTopic();
        for (final StateStore store : topology.globalStateStores()) {
            if (!store.persistent()) {
                globalNonPersistentStoresTopics.add(storeToChangelogTopic.get(store.name()));
            }
        }

        log = logContext.logger(GlobalStateManagerImpl.class);
        this.topology = topology;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.stateRestoreListener = stateRestoreListener;
        retries = config.getInt(StreamsConfig.RETRIES_CONFIG);
        retryBackoffMs = config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG);
        pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        this.deserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        this.logContext = logContext;
    }

    @Override
    public void setGlobalProcessorContext(final InternalProcessorContext globalProcessorContext) {
        this.globalProcessorContext = globalProcessorContext;
    }

    @Override
    public Set<String> initialize() {
        try {
            if (!stateDirectory.lockGlobalState()) {
                throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir));
            }
        } catch (final IOException e) {
            throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir), e);
        }

        try {
            checkpointFileCache.putAll(checkpointFile.read());
        } catch (final IOException e) {
            try {
                stateDirectory.unlockGlobalState();
            } catch (final IOException e1) {
                log.error("Failed to unlock the global state directory", e);
            }
            throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
        }

        final List<StateStore> stateStores = topology.globalStateStores();
        final Map<String, String> storeNameToChangelog = topology.storeToChangelogTopic();
        final Set<String> changelogTopics = new HashSet<>();
        for (final StateStore stateStore : stateStores) {
            globalStoreNames.add(stateStore.name());
            final String sourceTopic = storeNameToChangelog.get(stateStore.name());
            changelogTopics.add(sourceTopic);

            final SourceNode source = topology.source(sourceTopic);
            if (source != null && !ProcessorStateManager
                .storeChangelogTopic(globalProcessorContext.applicationId(), stateStore.name()).equals(sourceTopic)) {
                log.info("Found sourceNode {} for topic {}", source.toString(), sourceTopic);
                deserializers.put(
                    sourceTopic,
                    new RecordDeserializer(
                        source,
                        deserializationExceptionHandler,
                        logContext,
                        droppedRecordsSensorOrSkippedRecordsSensor(
                            Thread.currentThread().getName(),
                            globalProcessorContext.taskId().toString(),
                            globalProcessorContext.metrics()
                        )
                    )
                );
            }

            stateStore.init(globalProcessorContext, stateStore);
        }

        // make sure each topic-partition from checkpointFileCache is associated with a global state store
        checkpointFileCache.keySet().forEach(tp -> {
            if (!changelogTopics.contains(tp.topic())) {
                log.error("Encountered a topic-partition in the global checkpoint file not associated with any global" +
                    " state store, topic-partition: {}, checkpoint file: {}. If this topic-partition is no longer valid," +
                    " an application reset and state store directory cleanup will be required.",
                    tp.topic(), checkpointFile.toString());
                try {
                    stateDirectory.unlockGlobalState();
                } catch (final IOException e) {
                    log.error("Failed to unlock the global state directory", e);
                }
                throw new StreamsException("Encountered a topic-partition not associated with any global state store");
            }
        });
        return Collections.unmodifiableSet(globalStoreNames);
    }

    public StateStore getGlobalStore(final String name) {
        return globalStores.getOrDefault(name, Optional.empty()).orElse(null);
    }

    @Override
    public StateStore getStore(final String name) {
        return getGlobalStore(name);
    }

    public File baseDir() {
        return baseDir;
    }

    @Override
    public void registerStore(final StateStore store, final StateRestoreCallback stateRestoreCallback) {
        if (globalStores.containsKey(store.name())) {
            throw new IllegalArgumentException(String.format("Global Store %s has already been registered", store.name()));
        }

        if (!globalStoreNames.contains(store.name())) {
            throw new IllegalArgumentException(String.format("Trying to register store %s that is not a known global store", store.name()));
        }

        if (stateRestoreCallback == null) {
            throw new IllegalArgumentException(String.format("The stateRestoreCallback provided for store %s was null", store.name()));
        }

        log.info("Restoring state for global store {}", store.name());
        final List<TopicPartition> topicPartitions = topicPartitionsForStore(store);
        Map<TopicPartition, Long> highWatermarks = null;

        int attempts = 0;
        while (highWatermarks == null) {
            try {
                highWatermarks = globalConsumer.endOffsets(topicPartitions);
            } catch (final TimeoutException retryableException) {
                if (++attempts > retries) {
                    log.error("Failed to get end offsets for topic partitions of global store {} after {} retry attempts. " +
                        "You can increase the number of retries via configuration parameter `retries`.",
                        store.name(),
                        retries,
                        retryableException);
                    throw new StreamsException(String.format("Failed to get end offsets for topic partitions of global store %s after %d retry attempts. " +
                            "You can increase the number of retries via configuration parameter `retries`.", store.name(), retries),
                        retryableException);
                }
                log.debug("Failed to get end offsets for partitions {}, backing off for {} ms to retry (attempt {} of {})",
                    topicPartitions,
                    retryBackoffMs,
                    attempts,
                    retries,
                    retryableException);
                Utils.sleep(retryBackoffMs);
            }
        }
        try {
            restoreState(
                stateRestoreCallback,
                topicPartitions,
                highWatermarks,
                store.name(),
                converterForStore(store)
            );
            globalStores.put(store.name(), Optional.of(store));
        } finally {
            globalConsumer.unsubscribe();
        }
    }

    private List<TopicPartition> topicPartitionsForStore(final StateStore store) {
        final String sourceTopic = topology.storeToChangelogTopic().get(store.name());
        List<PartitionInfo> partitionInfos;
        int attempts = 0;
        while (true) {
            try {
                partitionInfos = globalConsumer.partitionsFor(sourceTopic);
                break;
            } catch (final TimeoutException retryableException) {
                if (++attempts > retries) {
                    log.error("Failed to get partitions for topic {} after {} retry attempts due to timeout. " +
                            "The broker may be transiently unavailable at the moment. " +
                            "You can increase the number of retries via configuration parameter `retries`.",
                        sourceTopic,
                        retries,
                        retryableException);
                    throw new StreamsException(String.format("Failed to get partitions for topic %s after %d retry attempts due to timeout. " +
                        "The broker may be transiently unavailable at the moment. " +
                        "You can increase the number of retries via configuration parameter `retries`.", sourceTopic, retries),
                        retryableException);
                }
                log.debug("Failed to get partitions for topic {} due to timeout. The broker may be transiently unavailable at the moment. " +
                        "Backing off for {} ms to retry (attempt {} of {})",
                    sourceTopic,
                    retryBackoffMs,
                    attempts,
                    retries,
                    retryableException);
                Utils.sleep(retryBackoffMs);
            }
        }

        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new StreamsException(String.format("There are no partitions available for topic %s when initializing global store %s", sourceTopic, store.name()));
        }

        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (final PartitionInfo partition : partitionInfos) {
            topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private void restoreState(final StateRestoreCallback stateRestoreCallback,
                              final List<TopicPartition> topicPartitions,
                              final Map<TopicPartition, Long> highWatermarks,
                              final String storeName,
                              final RecordConverter recordConverter) {
        for (final TopicPartition topicPartition : topicPartitions) {
            globalConsumer.assign(Collections.singletonList(topicPartition));
            final Long checkpoint = checkpointFileCache.get(topicPartition);
            if (checkpoint != null) {
                globalConsumer.seek(topicPartition, checkpoint);
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            long offset = globalConsumer.position(topicPartition);
            final Long highWatermark = highWatermarks.get(topicPartition);
            final RecordBatchingStateRestoreCallback stateRestoreAdapter =
                StateRestoreCallbackAdapter.adapt(stateRestoreCallback);

            stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
            long restoreCount = 0L;

            final RecordDeserializer recordDeserializer = deserializers.get(topicPartition.topic());

            while (offset < highWatermark) {
                try {
                    final ConsumerRecords<byte[], byte[]> records = globalConsumer.poll(pollTime);
                    final List<ConsumerRecord<byte[], byte[]>> restoreRecords = new ArrayList<>();
                    for (final ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                        if (record.key() != null) {
                            if (recordDeserializer != null && recordDeserializer.deserialize(globalProcessorContext, record) == null) {
                                continue;
                            }

                            restoreRecords.add(recordConverter.convert(record));
                        }
                    }
                    offset = globalConsumer.position(topicPartition);
                    stateRestoreAdapter.restoreBatch(restoreRecords);
                    stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.size());
                    restoreCount += restoreRecords.size();
                } catch (final InvalidOffsetException recoverableException) {
                    log.warn("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
                        storeName,
                        recoverableException.toString());

                    // TODO K9113: we remove the re-init logic and push it to be handled by the thread directly

                    restoreCount = 0L;
                }
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeName, restoreCount);
            checkpointFileCache.put(topicPartition, offset);
        }
    }

    @Override
    public void flush() {
        log.debug("Flushing all global globalStores registered in the state manager");
        for (final Map.Entry<String, Optional<StateStore>> entry : globalStores.entrySet()) {
            if (entry.getValue().isPresent()) {
                final StateStore store = entry.getValue().get();
                try {
                    log.trace("Flushing global store={}", store.name());
                    store.flush();
                } catch (final RuntimeException e) {
                    throw new ProcessorStateException(
                        String.format("Failed to flush global state store %s", store.name()),
                        e
                    );
                }
            } else {
                throw new IllegalStateException("Expected " + entry.getKey() + " to have been initialized");
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (globalStores.isEmpty()) {
                return;
            }
            final StringBuilder closeFailed = new StringBuilder();
            for (final Map.Entry<String, Optional<StateStore>> entry : globalStores.entrySet()) {
                if (entry.getValue().isPresent()) {
                    log.debug("Closing global storage engine {}", entry.getKey());
                    try {
                        entry.getValue().get().close();
                    } catch (final RuntimeException e) {
                        log.error("Failed to close global state store {}", entry.getKey(), e);
                        closeFailed.append("Failed to close global state store:")
                                   .append(entry.getKey())
                                   .append(". Reason: ")
                                   .append(e)
                                   .append("\n");
                    }
                    globalStores.put(entry.getKey(), Optional.empty());
                } else {
                    log.info("Skipping to close non-initialized store {}", entry.getKey());
                }
            }
            if (closeFailed.length() > 0) {
                throw new ProcessorStateException("Exceptions caught during close of 1 or more global state globalStores\n" + closeFailed);
            }
        } finally {
            stateDirectory.unlockGlobalState();
        }
    }

    @Override
    public void checkpoint(final Map<TopicPartition, Long> offsets) {
        checkpointFileCache.putAll(offsets);

        final Map<TopicPartition, Long> filteredOffsets = new HashMap<>();

        // Skip non persistent store
        for (final Map.Entry<TopicPartition, Long> topicPartitionOffset : checkpointFileCache.entrySet()) {
            final String topic = topicPartitionOffset.getKey().topic();
            if (!globalNonPersistentStoresTopics.contains(topic)) {
                filteredOffsets.put(topicPartitionOffset.getKey(), topicPartitionOffset.getValue());
            }
        }

        try {
            checkpointFile.write(filteredOffsets);
        } catch (final IOException e) {
            log.warn("Failed to write offset checkpoint file to {} for global stores: {}", checkpointFile, e);
        }
    }

    @Override
    public TaskType taskType() {
        return TaskType.GLOBAL;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return Collections.unmodifiableMap(checkpointFileCache);
    }
}
