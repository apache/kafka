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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.ShutdownException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.ConsumerUtils.poll;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl extends AbstractStateManager implements GlobalStateManager {
    private final Logger log;
    private final ProcessorTopology topology;
    private final Consumer<byte[], byte[]> globalConsumer;
    private final StateDirectory stateDirectory;
    private final Set<String> globalStoreNames = new HashSet<>();
    private final StateRestoreListener stateRestoreListener;
    private InternalProcessorContext processorContext;
    private final int retries;
    private final long retryBackoffMs;
    private final IsRunning isRunning;

    public GlobalStateManagerImpl(final LogContext logContext,
                                  final ProcessorTopology topology,
                                  final Consumer<byte[], byte[]> globalConsumer,
                                  final StateDirectory stateDirectory,
                                  final StateRestoreListener stateRestoreListener,
                                  final StreamsConfig config,
                                  final IsRunning isRunning) {
        super(stateDirectory.globalStateDir());

        this.log = logContext.logger(GlobalStateManagerImpl.class);
        this.topology = topology;
        this.globalConsumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.stateRestoreListener = stateRestoreListener;
        this.retries = config.getInt(StreamsConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG);
        this.isRunning = isRunning;
    }

    public interface IsRunning {
        boolean check();
    }

    @Override
    public void setGlobalProcessorContext(final InternalProcessorContext processorContext) {
        this.processorContext = processorContext;
    }

    @Override
    public Set<String> initialize() {
        try {
            if (!stateDirectory.lockGlobalState()) {
                throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir));
            }
        } catch (IOException e) {
            throw new LockException(String.format("Failed to lock the global state directory: %s", baseDir));
        }

        try {
            this.checkpointableOffsets.putAll(checkpoint.read());
        } catch (IOException e) {
            try {
                stateDirectory.unlockGlobalState();
            } catch (IOException e1) {
                log.error("Failed to unlock the global state directory", e);
            }
            throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
        }

        final List<StateStore> stateStores = topology.globalStateStores();
        for (final StateStore stateStore : stateStores) {
            globalStoreNames.add(stateStore.name());
            stateStore.init(processorContext, stateStore);
        }
        return Collections.unmodifiableSet(globalStoreNames);
    }

    @Override
    public void reinitializeStateStoresForPartitions(final Collection<TopicPartition> partitions,
                                                     final InternalProcessorContext processorContext) {
        super.reinitializeStateStoresForPartitions(
            log,
            globalStores,
            topology.storeToChangelogTopic(),
            partitions,
            processorContext);

        globalConsumer.assign(partitions);
        globalConsumer.seekToBeginning(partitions);
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return globalStores.get(name);
    }

    @Override
    public StateStore getStore(final String name) {
        return getGlobalStore(name);
    }

    public File baseDir() {
        return baseDir;
    }

    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {

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
            restoreState(stateRestoreCallback, topicPartitions, highWatermarks, store.name());
            globalStores.put(store.name(), store);
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
            } catch (final WakeupException wakeupException) {
                if (isRunning.check()) {
                    // note we may decide later that this condition is ok and just let the retry loop continue
                    throw new IllegalStateException("Got unexpected WakeupException during initialization.", wakeupException);
                } else {
                    throw new ShutdownException("Shutting down from fetching partitions");
                }
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
        for (PartitionInfo partition : partitionInfos) {
            topicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        return topicPartitions;
    }

    private void restoreState(final StateRestoreCallback stateRestoreCallback,
                              final List<TopicPartition> topicPartitions,
                              final Map<TopicPartition, Long> highWatermarks,
                              final String storeName) {
        for (final TopicPartition topicPartition : topicPartitions) {
            globalConsumer.assign(Collections.singletonList(topicPartition));
            final Long checkpoint = checkpointableOffsets.get(topicPartition);
            if (checkpoint != null) {
                globalConsumer.seek(topicPartition, checkpoint);
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            long offset = globalConsumer.position(topicPartition);
            final Long highWatermark = highWatermarks.get(topicPartition);
            final BatchingStateRestoreCallback stateRestoreAdapter =
                (BatchingStateRestoreCallback) ((stateRestoreCallback instanceof BatchingStateRestoreCallback)
                    ? stateRestoreCallback
                    : new WrappedBatchingStateRestoreCallback(stateRestoreCallback));

            stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
            long restoreCount = 0L;

            while (offset < highWatermark) {
                if (!isRunning.check()) {
                    throw new ShutdownException("Streams is not running (any more)");
                }
                try {
                    final ConsumerRecords<byte[], byte[]> records = poll(globalConsumer, 100);
                    final List<KeyValue<byte[], byte[]>> restoreRecords = new ArrayList<>();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (record.key() != null) {
                            restoreRecords.add(KeyValue.pair(record.key(), record.value()));
                        }
                        offset = globalConsumer.position(topicPartition);
                    }
                    stateRestoreAdapter.restoreAll(restoreRecords);
                    stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.size());
                    restoreCount += restoreRecords.size();
                } catch (final InvalidOffsetException recoverableException) {
                    log.warn("Restoring GlobalStore {} failed due to: {}. Deleting global store to recreate from scratch.",
                        storeName,
                        recoverableException.toString());
                    reinitializeStateStoresForPartitions(recoverableException.partitions(), processorContext);

                    stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
                    restoreCount = 0L;
                }
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeName, restoreCount);
            checkpointableOffsets.put(topicPartition, offset);
        }
    }

    @Override
    public void flush() {
        log.debug("Flushing all global globalStores registered in the state manager");
        for (StateStore store : this.globalStores.values()) {
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
            if (globalStores.isEmpty()) {
                return;
            }
            final StringBuilder closeFailed = new StringBuilder();
            for (final Map.Entry<String, StateStore> entry : globalStores.entrySet()) {
                log.debug("Closing global storage engine {}", entry.getKey());
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    log.error("Failed to close global state store {}", entry.getKey(), e);
                    closeFailed.append("Failed to close global state store:")
                            .append(entry.getKey())
                            .append(". Reason: ")
                            .append(e.toString())
                            .append("\n");
                }
            }
            globalStores.clear();
            if (closeFailed.length() > 0) {
                throw new ProcessorStateException("Exceptions caught during close of 1 or more global state globalStores\n" + closeFailed);
            }
            checkpoint(offsets);
        } finally {
            stateDirectory.unlockGlobalState();
        }
    }

    @Override
    public void checkpoint(final Map<TopicPartition, Long> offsets) {
        checkpointableOffsets.putAll(offsets);
        if (!checkpointableOffsets.isEmpty()) {
            try {
                checkpoint.write(checkpointableOffsets);
            } catch (IOException e) {
                log.warn("Failed to write offset checkpoint file to {} for global stores: {}", checkpoint, e);
            }
        }
    }

    @Override
    public Map<TopicPartition, Long> checkpointed() {
        return Collections.unmodifiableMap(checkpointableOffsets);
    }


}
