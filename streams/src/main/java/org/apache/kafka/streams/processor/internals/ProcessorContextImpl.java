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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.PositionSerde;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;
import static org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.getReadOnlyStore;
import static org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.wrapWithReadWriteStore;

public final class ProcessorContextImpl extends AbstractProcessorContext<Object, Object> implements RecordCollector.Supplier {
    // the below are null for standby tasks
    private StreamTask streamTask;
    private RecordCollector collector;

    private final ProcessorStateManager stateManager;
    private final boolean consistencyEnabled;

    final Map<String, DirtyEntryFlushListener> cacheNameToFlushListener = new HashMap<>();

    public ProcessorContextImpl(final TaskId id,
                                final StreamsConfig config,
                                final ProcessorStateManager stateMgr,
                                final StreamsMetricsImpl metrics,
                                final ThreadCache cache) {
        super(id, config, metrics, cache);
        stateManager = stateMgr;
        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false);
    }

    @Override
    public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
        if (stateManager.taskType() != TaskType.ACTIVE) {
            throw new IllegalStateException("Tried to transition processor context to active but the state manager's " +
                                                "type was " + stateManager.taskType());
        }
        this.streamTask = streamTask;
        this.collector = recordCollector;
        this.cache = newCache;
        addAllFlushListenersToNewCache();
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
        if (stateManager.taskType() != TaskType.STANDBY) {
            throw new IllegalStateException("Tried to transition processor context to standby but the state manager's " +
                                                "type was " + stateManager.taskType());
        }
        this.streamTask = null;
        this.collector = null;
        this.cache = newCache;
        addAllFlushListenersToNewCache();
    }

    @Override
    public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
        cacheNameToFlushListener.put(namespace, listener);
        cache.addDirtyEntryFlushListener(namespace, listener);
    }

    private void addAllFlushListenersToNewCache() {
        for (final Map.Entry<String, DirtyEntryFlushListener> cacheEntry : cacheNameToFlushListener.entrySet()) {
            cache.addDirtyEntryFlushListener(cacheEntry.getKey(), cacheEntry.getValue());
        }
    }

    @Override
    public ProcessorStateManager stateManager() {
        return stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return collector;
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp,
                          final Position position) {
        throwUnsupportedOperationExceptionIfStandby("logChange");

        final TopicPartition changelogPartition = stateManager().registeredChangelogPartitionFor(storeName);

        final Headers headers;
        if (!consistencyEnabled) {
            headers = null;
        } else {
            // Add the vector clock to the header part of every record
            headers = new RecordHeaders();
            headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
            headers.add(new RecordHeader(ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                    PositionSerde.serialize(position).array()));
        }

        collector.send(
            changelogPartition.topic(),
            key,
            value,
            headers,
            changelogPartition.partition(),
            timestamp,
            BYTES_KEY_SERIALIZER,
            BYTEARRAY_VALUE_SERIALIZER,
            null,
            null);
    }

    /**
     * @throws StreamsException if an attempt is made to access this state store from an unknown node
     * @throws UnsupportedOperationException if the current streamTask type is standby
     */
    @SuppressWarnings("unchecked")
    @Override
    public <S extends StateStore> S  getStateStore(final String name) {
        throwUnsupportedOperationExceptionIfStandby("getStateStore");
        if (currentNode() == null) {
            throw new StreamsException("Accessing from an unknown node");
        }

        final StateStore globalStore = stateManager.globalStore(name);
        if (globalStore != null) {
            return (S) getReadOnlyStore(globalStore);
        }

        if (!currentNode().stateStores.contains(name)) {
            throw new StreamsException("Processor " + currentNode().name() + " has no access to StateStore " + name +
                " as the store is not connected to the processor. If you add stores manually via '.addStateStore()' " +
                "make sure to connect the added store to the processor by providing the processor name to " +
                "'.addStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                "to connect the store to the corresponding operator, or they can provide a StoreBuilder by implementing " +
                "the stores() method on the Supplier itself. If you do not add stores manually, " +
                "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
        }

        final StateStore store = stateManager.store(name);
        return (S) wrapWithReadWriteStore(store);
    }

    @Override
    public <K, V> void forward(final K key,
                               final V value) {
        final Record<K, V> toForward = new Record<>(
            key,
            value,
            timestamp(),
            headers()
        );
        forward(toForward);
    }

    @Override
    public <K, V> void forward(final K key,
                               final V value,
                               final To to) {
        final ToInternal toInternal = new ToInternal(to);
        final Record<K, V> toForward = new Record<>(
            key,
            value,
            toInternal.hasTimestamp() ? toInternal.timestamp() : timestamp(),
            headers()
        );
        forward(toForward, toInternal.child());
    }

    @Override
    public <K, V> void forward(final FixedKeyRecord<K, V> record) {
        forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()));
    }

    @Override
    public <K, V> void forward(final FixedKeyRecord<K, V> record, final String childName) {
        forward(
            new Record<>(record.key(), record.value(), record.timestamp(), record.headers()),
            childName
        );
    }

    @Override
    public <K, V> void forward(final Record<K, V> record) {
        forward(record, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final Record<K, V> record, final String childName) {
        throwUnsupportedOperationExceptionIfStandby("forward");

        final ProcessorNode<?, ?, ?, ?> previousNode = currentNode();
        if (previousNode == null) {
            throw new StreamsException("Current node is unknown. This can happen if 'forward()' is called " +
                    "in an illegal scope. The root cause could be that a 'Processor' or 'Transformer' instance" +
                    " is shared. To avoid this error, make sure that your suppliers return new instances " +
                    "each time 'get()' of Supplier is called and do not return the same object reference " +
                    "multiple times.");
        }

        final ProcessorRecordContext previousContext = recordContext;

        try {
            // we don't want to set the recordContext if it's null, since that means that
            // the context itself is undefined. this isn't perfect, since downstream
            // old API processors wouldn't see the timestamps or headers of upstream
            // new API processors. But then again, from the perspective of those old-API
            // processors, even consulting the timestamp or headers when the record context
            // is undefined is itself not well defined. Plus, I don't think we need to worry
            // too much about heterogeneous applications, in which the upstream processor is
            // implementing the new API and the downstream one is implementing the old API.
            // So, this seems like a fine compromise for now.
            if (recordContext != null && (record.timestamp() != timestamp() || record.headers() != headers())) {
                recordContext = new ProcessorRecordContext(
                    record.timestamp(),
                    recordContext.offset(),
                    recordContext.partition(),
                    recordContext.topic(),
                    record.headers());
            }

            if (childName == null) {
                final List<? extends ProcessorNode<?, ?, ?, ?>> children = currentNode().children();
                for (final ProcessorNode<?, ?, ?, ?> child : children) {
                    forwardInternal((ProcessorNode<K, V, ?, ?>) child, record);
                }
            } else {
                final ProcessorNode<?, ?, ?, ?> child = currentNode().child(childName);
                if (child == null) {
                    throw new StreamsException("Unknown downstream node: " + childName
                                                   + " either does not exist or is not connected to this processor.");
                }
                forwardInternal((ProcessorNode<K, V, ?, ?>) child, record);
            }

        } finally {
            recordContext = previousContext;
            setCurrentNode(previousNode);
        }
    }

    private <K, V> void forwardInternal(final ProcessorNode<K, V, ?, ?> child,
                                        final Record<K, V> record) {
        setCurrentNode(child);

        child.process(record);

        if (child.isTerminalNode()) {
            streamTask.maybeRecordE2ELatency(record.timestamp(), currentSystemTimeMs(), child.name());
        }
    }

    @Override
    public void commit() {
        throwUnsupportedOperationExceptionIfStandby("commit");
        streamTask.requestCommit();
    }

    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        throwUnsupportedOperationExceptionIfStandby("schedule");
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(interval, "interval");
        final long intervalMs = validateMillisecondDuration(interval, msgPrefix);
        if (intervalMs < 1) {
            throw new IllegalArgumentException("The minimum supported scheduling interval is 1 millisecond.");
        }
        return streamTask.schedule(intervalMs, type, callback);
    }

    @Override
    public String topic() {
        throwUnsupportedOperationExceptionIfStandby("topic");
        return super.topic();
    }

    @Override
    public int partition() {
        throwUnsupportedOperationExceptionIfStandby("partition");
        return super.partition();
    }

    @Override
    public long offset() {
        throwUnsupportedOperationExceptionIfStandby("offset");
        return super.offset();
    }

    @Override
    public long timestamp() {
        throwUnsupportedOperationExceptionIfStandby("timestamp");
        return super.timestamp();
    }

    @Override
    public long currentStreamTimeMs() {
        return streamTask.streamTime();
    }

    @Override
    public ProcessorNode<?, ?, ?, ?> currentNode() {
        throwUnsupportedOperationExceptionIfStandby("currentNode");
        return super.currentNode();
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        throwUnsupportedOperationExceptionIfStandby("setRecordContext");
        super.setRecordContext(recordContext);
    }

    @Override
    public ProcessorRecordContext recordContext() {
        throwUnsupportedOperationExceptionIfStandby("recordContext");
        return super.recordContext();
    }

    private void throwUnsupportedOperationExceptionIfStandby(final String operationName) {
        if (taskType() == TaskType.STANDBY) {
            throw new UnsupportedOperationException(
                "this should not happen: " + operationName + "() is not supported in standby tasks.");
        }
    }
}
