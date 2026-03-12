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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.FixedOrderMap;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.processor.CommitCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.state.internals.LegacyCheckpointingStateStore;
import org.apache.kafka.streams.state.internals.RecordConverter;

import org.slf4j.Logger;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.processor.internals.RecordDeserializer.handleDeserializationFailure;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.converterForStore;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

/**
 * This class is responsible for the initialization, restoration, closing, flushing etc
 * of Global State Stores. There is only ever 1 instance of this class per Application Instance.
 */
public class GlobalStateManagerImpl implements GlobalStateManager {

    private static class StateStoreMetadata {
        final StateStore stateStore;
        final List<TopicPartition> changelogPartitions;
        final StateRestoreCallback restoreCallback;
        final Optional<InternalTopologyBuilder.ReprocessFactory<?, ?, ?, ?>> reprocessFactory;
        final RecordConverter recordConverter;
        final Map<TopicPartition, Long> highWatermarks;

        StateStoreMetadata(final StateStore stateStore,
                           final List<TopicPartition> changelogPartitions,
                           final Optional<InternalTopologyBuilder.ReprocessFactory<?, ?, ?, ?>> reprocessFactory,
                           final StateRestoreCallback restoreCallback,
                           final RecordConverter recordConverter,
                           final Map<TopicPartition, Long> highWatermarks) {
            this.stateStore = stateStore;
            this.changelogPartitions = changelogPartitions;
            this.reprocessFactory = reprocessFactory;
            this.restoreCallback = reprocessFactory.isPresent() ? null : restoreCallback;
            this.recordConverter = reprocessFactory.isPresent() ? null : recordConverter;
            this.highWatermarks = highWatermarks;
        }
    }

    private static final long NO_DEADLINE = -1L;

    private final Time time;
    private final Logger log;
    private final String logPrefix;
    private final StateDirectory stateDirectory;
    private final File baseDir;
    private final long taskTimeoutMs;
    private final ProcessorTopology topology;
    private final Duration pollMsPlusRequestTimeout;
    private final Consumer<byte[], byte[]> globalConsumer;
    private final StateRestoreListener stateRestoreListener;
    private final Map<TopicPartition, Long> currentOffsets;
    private final Map<String, String> storeToChangelogTopic;
    private final Set<String> globalStoreNames = new HashSet<>();
    private final FixedOrderMap<String, Optional<StateStore>> globalStores = new FixedOrderMap<>();
    private final Map<String, StateStoreMetadata> storeMetadata = new HashMap<>();
    private final boolean eosEnabled;
    private InternalProcessorContext<?, ?> globalProcessorContext;
    private DeserializationExceptionHandler deserializationExceptionHandler;
    private ProcessingExceptionHandler processingExceptionHandler;
    private Sensor droppedRecordsSensor;

    public GlobalStateManagerImpl(final LogContext logContext,
                                  final Time time,
                                  final ProcessorTopology topology,
                                  final Consumer<byte[], byte[]> globalConsumer,
                                  final StateDirectory stateDirectory,
                                  final StateRestoreListener stateRestoreListener,
                                  final StreamsConfig config) {
        this.time = time;
        this.topology = topology;
        this.stateDirectory = stateDirectory;
        baseDir = stateDirectory.globalStateDir();
        storeToChangelogTopic = topology.storeToChangelogTopic();
        currentOffsets = new HashMap<>();

        // Find non persistent store's topics
        for (final StateStore store : topology.globalStateStores()) {
            globalStoreNames.add(store.name());
        }

        log = logContext.logger(GlobalStateManagerImpl.class);
        logPrefix = logContext.logPrefix();
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
        deserializationExceptionHandler = config.deserializationExceptionHandler();
        @SuppressWarnings("deprecation")
        final boolean globalEnabled = config.getBoolean(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_GLOBAL_ENABLED_CONFIG);
        processingExceptionHandler = globalEnabled ? config.processingExceptionHandler() : null;
        eosEnabled = StreamsConfigUtils.eosEnabled(config);
    }

    @Override
    public void setGlobalProcessorContext(final InternalProcessorContext<?, ?> globalProcessorContext) {
        this.globalProcessorContext = globalProcessorContext;
    }

    @Override
    public Set<String> initialize() {
        droppedRecordsSensor = droppedRecordsSensor(
            Thread.currentThread().getName(),
            globalProcessorContext.taskId().toString(),
            globalProcessorContext.metrics()
        );

        final Map<TopicPartition, StateStore> wrappedStores = new HashMap<>();
        for (final StateStore stateStore : topology.globalStateStores()) {
            final List<TopicPartition> storePartitions = topicPartitionsForStore(stateStore);
            final StateStore maybeWrappedStore = LegacyCheckpointingStateStore.maybeWrapStore(
                    stateStore, eosEnabled, new HashSet<>(storePartitions), stateDirectory, null, logPrefix);
            maybeWrappedStore.init(globalProcessorContext, maybeWrappedStore);

            for (final TopicPartition storePartition : storePartitions) {
                wrappedStores.put(storePartition, maybeWrappedStore);
            }
        }

        // migrate offsets from legacy checkpoint file into the stores
        LegacyCheckpointingStateStore.migrateLegacyOffsets(logPrefix, stateDirectory, null, wrappedStores);

        for (final StateStoreMetadata metadata : storeMetadata.values()) {
            // load the committed offsets from the store
            final StateStore store = metadata.stateStore;
            if (store.persistent()) {
                for (final TopicPartition partition : metadata.changelogPartitions) {
                    final Long offset = store.committedOffset(partition);
                    if (offset != null) {
                        currentOffsets.put(partition, offset);
                    }
                }
            }

            // restore or reprocess each registered store using the now-populated currentOffsets
            try {
                if (metadata.reprocessFactory.isPresent()) {
                    reprocessState(metadata);
                } else {
                    restoreState(metadata);
                }
            } finally {
                globalConsumer.unsubscribe();
            }
        }

        return Collections.unmodifiableSet(globalStoreNames);
    }

    public StateStore globalStore(final String name) {
        return LegacyCheckpointingStateStore.maybeUnwrapStore(globalStores.getOrDefault(name, Optional.empty()).orElse(null));
    }

    @Override
    public StateStore store(final String name) {
        return globalStore(name);
    }

    public File baseDir() {
        return baseDir;
    }

    @Override
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback,
                              final CommitCallback ignored) {
        log.info("Registering global store {}", store.name());

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

        final Optional<InternalTopologyBuilder.ReprocessFactory<?, ?, ?, ?>> reprocessFactory = topology
                .storeNameToReprocessOnRestore().getOrDefault(store.name(), Optional.empty());
        storeMetadata.put(store.name(), new StateStoreMetadata(
                store, topicPartitions, reprocessFactory, stateRestoreCallback, converterForStore(store), highWatermarks));
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

    //Visible for testing
    public void setDeserializationExceptionHandler(final DeserializationExceptionHandler deserializationExceptionHandler) {
        this.deserializationExceptionHandler = deserializationExceptionHandler;
    }

    @SuppressWarnings({"rawtypes", "unchecked", "resource"})
    private void reprocessState(final StateStoreMetadata storeMetadata) {
        final InternalTopologyBuilder.ReprocessFactory<?, ?, ?, ?> reprocessFactory = storeMetadata.reprocessFactory.get();
        final Processor<?, ?, ?, ?> source = reprocessFactory.processorSupplier().get();
        source.init((ProcessorContext) globalProcessorContext);

        for (final TopicPartition topicPartition : storeMetadata.changelogPartitions) {
            long currentDeadline = NO_DEADLINE;

            globalConsumer.assign(Collections.singletonList(topicPartition));
            long offset;
            final Long checkpoint = currentOffsets.get(topicPartition);
            if (checkpoint != null) {
                globalConsumer.seek(topicPartition, checkpoint);
                offset = checkpoint;
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
                offset = getGlobalConsumerOffset(topicPartition);
            }
            final Long highWatermark = storeMetadata.highWatermarks.get(topicPartition);
            stateRestoreListener.onRestoreStart(topicPartition, storeMetadata.stateStore.name(), offset, highWatermark);

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

                long batchRestoreCount = 0;
                for (final ConsumerRecord<byte[], byte[]> record : records.records(topicPartition)) {
                    final ProcessorRecordContext recordContext =
                        new ProcessorRecordContext(
                            record.timestamp(),
                            record.offset(),
                            record.partition(),
                            record.topic(),
                            record.headers());
                    globalProcessorContext.setRecordContext(recordContext);

                    if (record.key() != null) {
                        // Deserialization phase
                        final Record<?, ?> deserializedRecord;
                        try {
                            deserializedRecord = new Record<>(
                                reprocessFactory.keyDeserializer().deserialize(record.topic(), record.key()),
                                reprocessFactory.valueDeserializer().deserialize(record.topic(), record.value()),
                                record.timestamp(),
                                record.headers());
                        } catch (final Exception deserializationException) {
                            // while Java distinguishes checked vs unchecked exceptions, other languages
                            // like Scala or Kotlin do not, and thus we need to catch `Exception`
                            // (instead of `RuntimeException`) to work well with those languages
                            handleDeserializationFailure(
                                deserializationExceptionHandler,
                                globalProcessorContext,
                                deserializationException,
                                record,
                                log,
                                droppedRecordsSensor,
                                null
                            );
                            continue; // Skip this record
                        }
                        final ProcessingExceptionHandler.Response response;
                        // Processing phase
                        try {
                            ((Processor) source).process(deserializedRecord);
                            restoreCount++;
                            batchRestoreCount++;
                        } catch (final Exception processingException) {
                            // while Java distinguishes checked vs unchecked exceptions, other languages
                            // like Scala or Kotlin do not, and thus we need to catch `Exception`
                            // (instead of `RuntimeException`) to work well with those languages
                            if (processingExceptionHandler != null) {
                                final ErrorHandlerContext errorHandlerContext = new DefaultErrorHandlerContext(
                                    globalProcessorContext,
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.headers(),
                                    reprocessFactory.processorName(),
                                    globalProcessorContext.taskId(),
                                    record.timestamp(),
                                    record.key(),
                                    record.value()
                                );
                                try {
                                    response =
                                        Objects.requireNonNull(processingExceptionHandler.handleError(
                                            errorHandlerContext,
                                            deserializedRecord,
                                            processingException
                                        ), "Invalid ProcessingExceptionHandler response");
                                    if (!response.deadLetterQueueRecords().isEmpty()) {
                                        log.warn("Dead letter queue records cannot be sent for global state/KTable processors. " +
                                                "DLQ support for global store/KTable will be added in a future release. " + "Record context: {}",
                                            errorHandlerContext);
                                    }
                                } catch (final Exception fatalUserException) {
                                    log.error(
                                            "Processing error callback failed after processing error for record: {}",
                                            errorHandlerContext,
                                            processingException
                                    );
                                    throw new FailedProcessingException(
                                            "Fatal user code error in processing error callback",
                                            null,
                                            fatalUserException
                                    );
                                }
                                
                                if (response.result() == ProcessingExceptionHandler.Result.FAIL) {
                                    log.error("Processing exception handler is set to fail upon" +
                                            " a processing error. If you would rather have the streaming pipeline" +
                                            " continue after a processing error, please set the " +
                                            PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.");
                                    throw new FailedProcessingException(null, processingException);
                                }
                                droppedRecordsSensor.record();
                            } else {
                                throw processingException;
                            }
                        }
                    }
                }

                offset = getGlobalConsumerOffset(topicPartition);

                stateRestoreListener.onBatchRestored(topicPartition, storeMetadata.stateStore.name(), offset, batchRestoreCount);
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeMetadata.stateStore.name(), restoreCount);
            currentOffsets.put(topicPartition, offset);

        }
    }

    private void restoreState(final StateStoreMetadata storeMetadata) {
        for (final TopicPartition topicPartition : storeMetadata.changelogPartitions) {
            long currentDeadline = NO_DEADLINE;

            globalConsumer.assign(Collections.singletonList(topicPartition));
            long offset;
            final Long checkpoint = currentOffsets.get(topicPartition);
            if (checkpoint != null) {
                globalConsumer.seek(topicPartition, checkpoint);
                offset = checkpoint;
            } else {
                globalConsumer.seekToBeginning(Collections.singletonList(topicPartition));
                offset = getGlobalConsumerOffset(topicPartition);
            }

            final Long highWatermark = storeMetadata.highWatermarks.get(topicPartition);
            final RecordBatchingStateRestoreCallback stateRestoreAdapter =
                StateRestoreCallbackAdapter.adapt(storeMetadata.restoreCallback);

            stateRestoreListener.onRestoreStart(topicPartition, storeMetadata.stateStore.name(), offset, highWatermark);
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
                        restoreRecords.add(storeMetadata.recordConverter.convert(record));
                    }
                }

                offset = getGlobalConsumerOffset(topicPartition);

                stateRestoreAdapter.restoreBatch(restoreRecords);
                stateRestoreListener.onBatchRestored(topicPartition, storeMetadata.stateStore.name(), offset, restoreRecords.size());
                restoreCount += restoreRecords.size();
            }
            stateRestoreListener.onRestoreEnd(topicPartition, storeMetadata.stateStore.name(), restoreCount);
            currentOffsets.put(topicPartition, offset);
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
    public void commit() {
        log.debug("Committing all global globalStores registered in the state manager");
        for (final Map.Entry<String, Optional<StateStore>> entry : globalStores.entrySet()) {
            if (entry.getValue().isPresent()) {
                final StateStore store = entry.getValue().get();
                try {
                    log.trace("Committing global store={}", store.name());
                    // construct per-store Map of offsets to commit
                    final List<TopicPartition> storePartitions = storeMetadata.get(store.name()).changelogPartitions;
                    final Map<TopicPartition, Long> storeOffsets = new HashMap<>(storePartitions.size());

                    // only add offsets for persistent stores
                    if (store.persistent()) {
                        for (final TopicPartition storePartition : storePartitions) {
                            storeOffsets.put(storePartition, currentOffsets.get(storePartition));
                        }
                    }
                    store.commit(storeOffsets);
                } catch (final RuntimeException e) {
                    throw new ProcessorStateException(
                        String.format("Failed to commit global state store %s", store.name()),
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
        currentOffsets.putAll(offsets);
    }

    @Override
    public TaskType taskType() {
        return TaskType.GLOBAL;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return Collections.unmodifiableMap(currentOffsets);
    }

    public final String changelogFor(final String storeName) {
        return storeToChangelogTopic.get(storeName);
    }
}
