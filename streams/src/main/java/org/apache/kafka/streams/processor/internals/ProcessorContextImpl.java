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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.processor.internals.AbstractReadOnlyDecorator.getReadOnlyStore;
import static org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.getReadWriteStore;

public class ProcessorContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {
    // the below are null for standby tasks
    private StreamTask streamTask;
    private RecordCollector collector;

    private final ToInternal toInternal = new ToInternal();
    private final static To SEND_TO_ALL = To.all();

    private final ProcessorStateManager stateManager;

    final Map<String, DirtyEntryFlushListener> cacheNameToFlushListener = new HashMap<>();

    public ProcessorContextImpl(final TaskId id,
                                final StreamsConfig config,
                                final ProcessorStateManager stateMgr,
                                final StreamsMetricsImpl metrics,
                                final ThreadCache cache) {
        super(id, config, metrics, cache);
        stateManager = stateMgr;
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
                          final long timestamp) {
        throwUnsupportedOperationExceptionIfStandby("logChange");

        final TopicPartition changelogPartition = stateManager().registeredChangelogPartitionFor(storeName);

        // Sending null headers to changelog topics (KIP-244)
        collector.send(
            changelogPartition.topic(),
            key,
            value,
            null,
            changelogPartition.partition(),
            timestamp,
            BYTES_KEY_SERIALIZER,
            BYTEARRAY_VALUE_SERIALIZER
        );
    }

    /**
     * @throws StreamsException if an attempt is made to access this state store from an unknown node
     * @throws UnsupportedOperationException if the current streamTask type is standby
     */
    @Override
    public StateStore getStateStore(final String name) {
        throwUnsupportedOperationExceptionIfStandby("getStateStore");
        if (currentNode() == null) {
            throw new StreamsException("Accessing from an unknown node");
        }

        final StateStore globalStore = stateManager.getGlobalStore(name);
        if (globalStore != null) {
            return getReadOnlyStore(globalStore);
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

        final StateStore store = stateManager.getStore(name);
        return getReadWriteStore(store);
    }

    @Override
    public <K, V> void forward(final K key,
                               final V value) {
        throwUnsupportedOperationExceptionIfStandby("forward");
        forward(key, value, SEND_TO_ALL);
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key,
                               final V value,
                               final int childIndex) {
        throwUnsupportedOperationExceptionIfStandby("forward");
        forward(
            key,
            value,
            To.child((currentNode().children()).get(childIndex).name()));
    }

    @Override
    @Deprecated
    public <K, V> void forward(final K key,
                               final V value,
                               final String childName) {
        throwUnsupportedOperationExceptionIfStandby("forward");
        forward(key, value, To.child(childName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key,
                               final V value,
                               final To to) {
        throwUnsupportedOperationExceptionIfStandby("forward");
        final ProcessorNode<?, ?, ?, ?> previousNode = currentNode();
        final ProcessorRecordContext previousContext = recordContext;

        try {
            toInternal.update(to);
            if (toInternal.hasTimestamp()) {
                recordContext = new ProcessorRecordContext(
                    toInternal.timestamp(),
                    recordContext.offset(),
                    recordContext.partition(),
                    recordContext.topic(),
                    recordContext.headers());
            }

            final String sendTo = toInternal.child();
            if (sendTo == null) {
                final List<? extends ProcessorNode<?, ?, ?, ?>> children = currentNode().children();
                for (final ProcessorNode<?, ?, ?, ?> child : children) {
                    forward((ProcessorNode<K, V, ?, ?>) child, key, value);
                }
            } else {
                final ProcessorNode<K, V, ?, ?> child = currentNode().getChild(sendTo);
                if (child == null) {
                    throw new StreamsException("Unknown downstream node: " + sendTo
                        + " either does not exist or is not connected to this processor.");
                }
                forward(child, key, value);
            }
        } finally {
            recordContext = previousContext;
            setCurrentNode(previousNode);
        }
    }

    private <K, V> void forward(final ProcessorNode<K, V, ?, ?> child,
                                final K key,
                                final V value) {
        setCurrentNode(child);
        child.process(key, value);
        if (child.isTerminalNode()) {
            streamTask.maybeRecordE2ELatency(timestamp(), currentSystemTimeMs(), child.name());
        }
    }

    @Override
    public void commit() {
        throwUnsupportedOperationExceptionIfStandby("commit");
        streamTask.requestCommit();
    }

    @Override
    @Deprecated
    public Cancellable schedule(final long intervalMs,
                                final PunctuationType type,
                                final Punctuator callback) {
        throwUnsupportedOperationExceptionIfStandby("schedule");
        if (intervalMs < 1) {
            throw new IllegalArgumentException("The minimum supported scheduling interval is 1 millisecond.");
        }
        return streamTask.schedule(intervalMs, type, callback);
    }

    @SuppressWarnings("deprecation") // removing #schedule(final long intervalMs,...) will fix this
    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        throwUnsupportedOperationExceptionIfStandby("schedule");
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(interval, "interval");
        return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
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
