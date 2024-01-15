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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
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
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl implements GlobalStateManager {
    private final static long NO_DEADLINE = -1L;

    private final Time time;
    private final Logger log;
    private final File baseDir;
    private final long taskTimeoutMs;
    private final ProcessorTopology topology;
    private final OffsetCheckpoint checkpointFile;
    private final Duration pollMsPlusRequestTimeout;
    private final Consumer<byte[], byte[]> globalConsumer;
    private final StateRestoreListener stateRestoreListener;
    private final Map<TopicPartition, Long> checkpointFileCache;
    private final Map<String, String> storeToChangelogTopic;
    private final Set<String> globalStoreNames = new HashSet<>();
    private final Set<String> globalNonPersistentStoresTopics = new HashSet<>();
    private final FixedOrderMap<String, Optional<StateStore>> globalStores = new FixedOrderMap<>();

    private InternalProcessorContext globalProcessorContext;

    public GlobalStateManagerImpl(final LogContext logContext,
                                  final Time time,
                                  final ProcessorTopology topology,
                                  final Consumer<byte[], byte[]> globalConsumer,
                                  final StateDirectory stateDirectory,
                                  final StateRestoreListener stateRestoreListener,
                                  final StreamsConfig config) {
        this.time = time;
        this.topology = topology;
        baseDir = stateDirectory.globalStateDir();
        storeToChangelogTopic = topology.storeToChangelogTopic();
        checkpointFile = new OffsetCheckpoint(new File(baseDir, CHECKPOINT_FILE_NAME));
        checkpointFileCache = new HashMap<>();

        // Find non persistent store's topics
        for (final StateStore store : topology.globalStateStores()) {
            globalStoreNames.add(store.name());
            if (!store.persistent()) {
                globalNonPersistentStoresTopics.add(changelogFor(store.name()));
            }
        }

        log = logContext.logger(GlobalStateManagerImpl.class);
        this.globalConsumer = globalConsumer;
        this.stateRestoreListener = stateRestoreListener;

        final Map<String, Object> consumerProps = config.getGlobalConsumerConfigs("dummy");
        // need to add mandatory configs; otherwise `QuietConsumerConfig` throws
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        final int requestTimeoutMs = new ClientUtils.QuietConsumerConfig(consumerProps)
            .getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        pollMsPlusRequestTimeout = Duration.ofMillis(
            config.getLong(StreamsConfig.POLL_MS_CONFIG) + requestTimeoutMs
        );
        taskTimeoutMs = config.getLong(StreamsConfig.TASK_TIMEOUT_MS_CONFIG);
    }

    @Override
    public void setGlobalProcessorContext(final InternalProcessorContext globalProcessorContext) {
        this.globalProcessorContext = globalProcessorContext;
    }

    @Override
    public Set<String> initialize() {
        try {
            checkpointFileCache.putAll(checkpointFile.read());
        } catch (final IOException e) {
            throw new StreamsException("Failed to read checkpoints for global state globalStores", e);
        }

        final Set<String> changelogTopics = new HashSet<>();
        for (final StateStore stateStore : topology.globalStateStores()) {
            final String sourceTopic = storeToChangelogTopic.get(stateStore.name());
            changelogTopics.add(sourceTopic);
            stateStore.init((StateStoreContext) globalProcessorContext, stateStore);
        }

        // make sure each topic-partition from checkpointFileCache is associated with a global state store
        checkpointFileCache.keySet().forEach(tp -> {
            if (!changelogTopics.contains(tp.topic())) {
                log.error(
                    "Encountered a topic-partition in the global checkpoint file not associated with any global" +
                        " state store, topic-partition: {}, checkpoint file: {}. If this topic-partition is no longer valid," +
                        " an application reset and state store directory cleanup will be required.",
                    tp.topic(),
                    checkpointFile
                );
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
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback,
                              final CommitCallback ignored) {
        log.info("Restoring state for global store {}", store.name());

        // TODO (KAFKA-12887): we should not trigger user's exception handler for illegal-argument but always
        // fail-crash; in this case we would not need to immediately close the state store before throwing
        if (globalStores.containsKey(store.name())) {
            store.close();
            throw new IllegalArgumentException(String.format("Global Store %s has already been registered", store.name()));
        }

        if (!globalStoreNames.contains(store.name())) {
            store.close();
            throw new IllegalArgumentException(String.format("Trying to register store %s that is not a known global store", store.name()));
        }

        // register the store first, so that if later an exception is thrown then eventually while we call `close`
        // on the state manager this state store would be closed as well
        globalStores.put(store.name(), Optional.of(store));

        if (stateRestoreCallback == null) {
            throw new IllegalArgumentException(String.format("The stateRestoreCallback provided for store %s was null", store.name()));
        }

        final List<TopicPartition> topicPartitions = topicPartitionsForStore(store);
        final Map<TopicPartition, Long> highWatermarks = retryUntilSuccessOrThrowOnTaskTimeout(
            () -> globalConsumer.endOffsets(topicPartitions),
            String.format(
                "Failed to get offsets for partitions %s. The broker may be transiently unavailable at the moment.",
                topicPartitions
            )
        );

        try {
            restoreState(
                stateRestoreCallback,
                topicPartitions,
                highWatermarks,
                store.name(),
                converterForStore(store)
            );
        } finally {
            globalConsumer.unsubscribe();
        }
    }

    private List<TopicPartition> topicPartitionsForStore(final StateStore store) {
        final String sourceTopic = storeToChangelogTopic.get(store.name());

        final List<PartitionInfo> partitionInfos = retryUntilSuccessOrThrowOnTaskTimeout(
            () -> globalConsumer.partitionsFor(sourceTopic),
            String.format(
                "Failed to get partitions for topic %s. The broker may be transiently unavailable at the moment.",
                sourceTopic
            )
        );

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
            long currentDeadline = NO_DEADLINE;

            globalConsumer.assign(Collections.singletonList(topicPartition));
            long offset;
            final Long checkpoint = checkpointFileCache.get(topicPartition);
            if (checkpoint != null) {
                globalConsumer.seek(topicPartition, checkpoint);
                offset = checkpoint;
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
                offset = getGlobalConsumerOffset(topicPartition);
            }

            final Long highWatermark = highWatermarks.get(topicPartition);
            final RecordBatchingStateRestoreCallback stateRestoreAdapter =
                StateRestoreCallbackAdapter.adapt(stateRestoreCallback);

            stateRestoreListener.onRestoreStart(topicPartition, storeName, offset, highWatermark);
            long restoreCount = 0L;

            while (offset < highWatermark) {
                // we add `request.timeout.ms` to `poll.ms` because `poll.ms` might be too short
                // to give a fetch request a fair chance to actually complete and we don't want to
                // start `task.timeout.ms` too early
                //
                // TODO with https://issues.apache.org/jira/browse/KAFKA-10315 we can just call
                //      `poll(pollMS)` without adding the request timeout and do a more precise
                //      timeout handling
                final ConsumerRecords<byte[], byte[]> records = globalConsumer.poll(pollMsPlusRequestTimeout);
                if (records.isEmpty()) {
                    currentDeadline = maybeUpdateDeadlineOrThrow(currentDeadline);
                } else {
                    currentDeadline = NO_DEADLINE;
                }

                final List<ConsumerRecord<byte[], byte[]>> restoreRecords = new ArrayList<>();
                for (final ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                    if (record.key() != null) {
                        restoreRecords.add(recordConverter.convert(record));
                    }
                }

                offset = getGlobalConsumerOffset(topicPartition);

                stateRestoreAdapter.restoreBatch(restoreRecords);
                stateRestoreListener.onBatchRestored(topicPartition, storeName, offset, restoreRecords.size());
                restoreCount += restoreRecords.size();
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeName, restoreCount);
            checkpointFileCache.put(topicPartition, offset);
        }
    }

    private long getGlobalConsumerOffset(final TopicPartition topicPartition) {
        return retryUntilSuccessOrThrowOnTaskTimeout(
            () -> globalConsumer.position(topicPartition),
            String.format(
                "Failed to get position for partition %s. The broker may be transiently unavailable at the moment.",
                topicPartition
            )
        );
    }

    private <R> R retryUntilSuccessOrThrowOnTaskTimeout(final Supplier<R> supplier,
                                                        final String errorMessage) {
        long deadlineMs = NO_DEADLINE;

        do {
            try {
                return supplier.get();
            } catch (final TimeoutException retriableException) {
                if (taskTimeoutMs == 0L) {
                    throw new StreamsException(
                        String.format(
                            "Retrying is disabled. You can enable it by setting `%s` to a value larger than zero.",
                            StreamsConfig.TASK_TIMEOUT_MS_CONFIG
                        ),
                        retriableException
                    );
                }

                deadlineMs = maybeUpdateDeadlineOrThrow(deadlineMs);

                log.warn(errorMessage, retriableException);
            }
        } while (true);
    }

    private long maybeUpdateDeadlineOrThrow(final long currentDeadlineMs) {
        final long currentWallClockMs = time.milliseconds();

        if (currentDeadlineMs == NO_DEADLINE) {
            final long newDeadlineMs = currentWallClockMs + taskTimeoutMs;
            return newDeadlineMs < 0L ? Long.MAX_VALUE : newDeadlineMs;
        } else if (currentWallClockMs >= currentDeadlineMs) {
            throw new TimeoutException(String.format(
                "Global task did not make progress to restore state within %d ms. Adjust `%s` if needed.",
                currentWallClockMs - currentDeadlineMs + taskTimeoutMs,
                StreamsConfig.TASK_TIMEOUT_MS_CONFIG
            ));
        }

        return currentDeadlineMs;
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
    public void close() {
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
    }

    @Override
    public void updateChangelogOffsets(final Map<TopicPartition, Long> offsets) {
        checkpointFileCache.putAll(offsets);
    }

    @Override
    public void checkpoint() {
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
            log.warn("Failed to write offset checkpoint file to {} for global stores." +
                " This may occur if OS cleaned the state.dir in case when it is located in the (default) ${java.io.tmpdir}/kafka-streams directory." +
                " Changing the location of state.dir may resolve the problem", checkpointFile, e);
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

    public final String changelogFor(final String storeName) {
        return storeToChangelogTopic.get(storeName);
    }
}
