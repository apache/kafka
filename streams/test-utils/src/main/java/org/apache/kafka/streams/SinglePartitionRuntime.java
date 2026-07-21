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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * The default {@link Runtime}: a single {@link StreamTask} for the whole topology, one partition
 * per topic.
 */
final class SinglePartitionRuntime implements Runtime {

    private static final Logger log = LoggerFactory.getLogger(SinglePartitionRuntime.class);

    private final StreamTask task; // null if the topology has only global processing
    private final InternalTopologyBuilder internalTopologyBuilder;
    private final GlobalStateManager globalStateManager;
    private final Map<String, TopicPartition> partitionsByInputTopic;
    private final Map<TopicPartition, AtomicLong> offsetsByTopicOrPatternPartition;
    private final MockProducer<byte[], byte[]> producer;
    private final Time wallClockTime;
    private final Host host;

    SinglePartitionRuntime(final StreamTask task,
                           final InternalTopologyBuilder internalTopologyBuilder,
                           final GlobalStateManager globalStateManager,
                           final Map<String, TopicPartition> partitionsByInputTopic,
                           final Map<TopicPartition, AtomicLong> offsetsByTopicOrPatternPartition,
                           final MockProducer<byte[], byte[]> producer,
                           final Time wallClockTime,
                           final Host host) {
        this.task = task;
        this.internalTopologyBuilder = internalTopologyBuilder;
        this.globalStateManager = globalStateManager;
        this.partitionsByInputTopic = partitionsByInputTopic;
        this.offsetsByTopicOrPatternPartition = offsetsByTopicOrPatternPartition;
        this.producer = producer;
        this.wallClockTime = wallClockTime;
        this.host = host;
    }

    @Override
    public void pipeRecord(final String topicName,
                           final long timestamp,
                           final byte[] key,
                           final byte[] value,
                           final Headers headers) {
        final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(topicName);
        final TopicPartition globalInputTopicPartition = host.globalPartitionOrNull(topicName);

        if (inputTopicOrPatternPartition == null && globalInputTopicPartition == null) {
            throw new IllegalArgumentException("Unknown topic: " + topicName);
        }

        if (inputTopicOrPatternPartition != null) {
            enqueueTaskRecord(topicName, inputTopicOrPatternPartition, timestamp, key, value, headers);
            completeAllProcessableWork();
        }

        if (globalInputTopicPartition != null) {
            host.processGlobalRecord(globalInputTopicPartition, timestamp, key, value, headers);
        }
    }

    private void enqueueTaskRecord(final String inputTopic,
                                   final TopicPartition topicOrPatternPartition,
                                   final long timestamp,
                                   final byte[] key,
                                   final byte[] value,
                                   final Headers headers) {
        final long offset = offsetsByTopicOrPatternPartition.get(topicOrPatternPartition).incrementAndGet() - 1;
        task.addRecords(topicOrPatternPartition, Collections.singleton(new ConsumerRecord<>(
            inputTopic,
            topicOrPatternPartition.partition(),
            offset,
            timestamp,
            TimestampType.CREATE_TIME,
            key == null ? ConsumerRecord.NULL_SIZE : key.length,
            value == null ? ConsumerRecord.NULL_SIZE : value.length,
            key,
            value,
            headers,
            Optional.empty()))
        );
    }

    @Override
    public void completeAllProcessableWork() {
        // for internally triggered processing (like wall-clock punctuations),
        // we might have buffered some records to internal topics that need to
        // be piped back in to kick-start the processing loop. This is idempotent
        // and therefore harmless in the case where all we've done is enqueued an
        // input record from the user.
        captureOutputsAndReEnqueueInternalResults();

        // If the topology only has global tasks, then `task` would be null.
        // For this method, it just means there's nothing to do.
        if (task != null) {
            task.resumePollingForPartitionsWithAvailableSpace();
            task.updateLags();
            while (task.hasRecordsQueued() && task.isProcessable(wallClockTime.milliseconds())) {
                // Process the record ...
                task.process(wallClockTime.milliseconds());
                task.maybePunctuateStreamTime();
                host.commit(task.prepareCommit(true));
                task.postCommit(true);
                captureOutputsAndReEnqueueInternalResults();
            }
            if (task.hasRecordsQueued()) {
                log.info("Due to the {} configuration, there are currently some records" +
                             " that cannot be processed. Advancing wall-clock time or" +
                             " enqueuing records on the empty topics will allow" +
                             " Streams to process more.",
                         StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            }
        }
    }

    private void captureOutputsAndReEnqueueInternalResults() {
        // Capture all the records sent to the producer ...
        final List<ProducerRecord<byte[], byte[]>> output = producer.history();
        producer.clear();

        for (final ProducerRecord<byte[], byte[]> record : output) {
            host.recordOutput(record.topic(), record);

            // Forward back into the topology if the produced record is to an internal or a source topic ...
            final String outputTopicName = record.topic();

            final TopicPartition inputTopicOrPatternPartition = getInputTopicOrPatternPartition(outputTopicName);
            final TopicPartition globalInputTopicPartition = host.globalPartitionOrNull(outputTopicName);

            if (inputTopicOrPatternPartition != null) {
                enqueueTaskRecord(
                    outputTopicName,
                    inputTopicOrPatternPartition,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers()
                );
            }

            if (globalInputTopicPartition != null) {
                host.processGlobalRecord(
                    globalInputTopicPartition,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers()
                );
            }
        }
    }

    private void validateSourceTopicNameRegexPattern(final String inputRecordTopic) {
        for (final String sourceTopicName : internalTopologyBuilder.fullSourceTopicNames()) {
            if (!sourceTopicName.equals(inputRecordTopic) && Pattern.compile(sourceTopicName).matcher(inputRecordTopic).matches()) {
                throw new TopologyException("Topology add source of type String for topic: " + sourceTopicName +
                                                " cannot contain regex pattern for input record topic: " + inputRecordTopic +
                                                " and hence cannot process the message.");
            }
        }
    }

    private TopicPartition getInputTopicOrPatternPartition(final String topicName) {
        if (!internalTopologyBuilder.fullSourceTopicNames().isEmpty()) {
            validateSourceTopicNameRegexPattern(topicName);
        }

        final TopicPartition topicPartition = partitionsByInputTopic.get(topicName);
        if (topicPartition == null) {
            for (final Map.Entry<String, TopicPartition> entry : partitionsByInputTopic.entrySet()) {
                if (Pattern.compile(entry.getKey()).matcher(topicName).matches()) {
                    return entry.getValue();
                }
            }
        }
        return topicPartition;
    }

    @Override
    public void advanceWallClockTime() {
        if (task != null) {
            task.maybePunctuateSystemTime();
            host.commit(task.prepareCommit(true));
            task.postCommit(true);
        }
        completeAllProcessableWork();
    }

    @Override
    public void suspendAndCloseTaskCleanly() {
        if (task != null) {
            task.suspend();
            task.prepareCommit(true);
            task.postCommit(true);
            task.closeClean();
        }
    }

    @Override
    public boolean hasRecordsQueued() {
        return task != null && task.hasRecordsQueued();
    }

    @Override
    public StateStore getStateStore(final String name,
                                    final boolean throwForBuiltInStores) {
        if (task != null) {
            // Accessing a store must not corrupt the task's record context. Only set a dummy
            // context when none exists yet (i.e. before any record has been processed) so that
            // direct store operations have a context to work with; never overwrite a live one.
            if (task.processorContext().recordContext() == null) {
                task.processorContext().setRecordContext(new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
            }
            final StateStore stateStore = ((ProcessorContextImpl) task.processorContext()).stateManager().store(name);
            if (stateStore != null) {
                if (throwForBuiltInStores) {
                    throwIfBuiltInStore(stateStore);
                }
                return stateStore;
            }
        }

        if (globalStateManager != null) {
            final StateStore stateStore = globalStateManager.store(name);
            if (stateStore != null) {
                if (throwForBuiltInStores) {
                    throwIfBuiltInStore(stateStore);
                }
                return stateStore;
            }
        }

        return null;
    }

    @Override
    public org.apache.kafka.streams.processor.api.ProcessorContext<?, ?> taskProcessorContext() {
        return task == null ? null : task.processorContext();
    }

    private static void throwIfBuiltInStore(final StateStore stateStore) {
        if (stateStore instanceof VersionedKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a versioned key-value store and should be accessed via `getVersionedKeyValueStore()`");
        }
        if (stateStore instanceof TimestampedKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a timestamped key-value store and should be accessed via `getTimestampedKeyValueStore()`");
        }
        if (stateStore instanceof ReadOnlyKeyValueStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a key-value store and should be accessed via `getKeyValueStore()`");
        }
        if (stateStore instanceof TimestampedWindowStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a timestamped window store and should be accessed via `getTimestampedWindowStore()`");
        }
        if (stateStore instanceof ReadOnlyWindowStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a window store and should be accessed via `getWindowStore()`");
        }
        if (stateStore instanceof ReadOnlySessionStore) {
            throw new IllegalArgumentException("Store " + stateStore.name()
                                                   + " is a session store and should be accessed via `getSessionStore()`");
        }
    }
}
