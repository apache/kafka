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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code ProcessorContext} implementation that will throw on any forward call.
 */
public final class ForwardingDisabledProcessorContext<K, V>
    implements InternalProcessorContext<K, V> {

    private final InternalProcessorContext<K, V> delegate;

    private static final String EXPLANATION =
        "ProcessorContext#forward() is not supported from this context, "
            + "as the framework must ensure the key is not changed (#forward allows changing the key on "
            + "messages which are sent). Try another function, which doesn't allow the key to be changed "
            + "(for example - #tranformValues).";

    public ForwardingDisabledProcessorContext(final InternalProcessorContext<K, V> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public String applicationId() {
        return delegate.applicationId();
    }

    @Override
    public TaskId taskId() {
        return delegate.taskId();
    }

    @Override
    public Optional<RecordMetadata> recordMetadata() {
        return delegate.recordMetadata();
    }

    @Override
    public Serde<?> keySerde() {
        return delegate.keySerde();
    }

    @Override
    public Serde<?> valueSerde() {
        return delegate.valueSerde();
    }

    @Override
    public File stateDir() {
        return delegate.stateDir();
    }

    @Override
    public StreamsMetricsImpl metrics() {
        return delegate.metrics();
    }

    @Override
    public void setSystemTimeMs(final long timeMs) {
        delegate.setSystemTimeMs(timeMs);
    }

    @Override
    public ProcessorRecordContext recordContext() {
        return delegate.recordContext();
    }

    @Override
    public void setRecordContext(final ProcessorRecordContext recordContext) {
        delegate.setRecordContext(recordContext);
    }

    @Override
    public void setCurrentNode(final ProcessorNode<?, ?, ?, ?> currentNode) {
        delegate.setCurrentNode(currentNode);
    }

    @Override
    public ProcessorNode<?, ?, ?, ?> currentNode() {
        return delegate.currentNode();
    }

    @Override
    public ThreadCache cache() {
        return delegate.cache();
    }

    @Override
    public void initialize() {
        delegate.initialize();
    }

    @Override
    public void uninitialize() {
        delegate.uninitialize();
    }

    @Override
    public TaskType taskType() {
        return delegate.taskType();
    }

    @Override
    public void transitionToActive(final StreamTask streamTask,
                                   final RecordCollector recordCollector,
                                   final ThreadCache newCache) {
        delegate.transitionToActive(streamTask, recordCollector, newCache);
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
        delegate.transitionToStandby(newCache);
    }

    @Override
    public void registerCacheFlushListener(final String namespace,
                                           final DirtyEntryFlushListener listener) {
        delegate.registerCacheFlushListener(namespace, listener);
    }

    @Override
    public <T extends StateStore> T getStateStore(final StoreBuilder<T> builder) {
        return delegate.getStateStore(builder);
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp) {
        delegate.logChange(storeName, key, value, timestamp);
    }

    @Override
    public String changelogFor(final String storeName) {
        return delegate.changelogFor(storeName);
    }

    @Override
    public void register(final StateStore store,
                         final StateRestoreCallback stateRestoreCallback) {
        delegate.register(store, stateRestoreCallback);
    }

    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        return delegate.getStateStore(name);
    }

    @Deprecated
    @Override
    public Cancellable schedule(final long intervalMs,
                                final PunctuationType type,
                                final Punctuator callback) {
        return delegate.schedule(intervalMs, type, callback);
    }

    @Override
    public Cancellable schedule(final Duration interval,
                                final PunctuationType type,
                                final Punctuator callback) throws IllegalArgumentException {
        return delegate.schedule(interval, type, callback);
    }

    @Override
    public <K1, V1> void forward(final K1 key, final V1 value) {
        throw new StreamsException(EXPLANATION);
    }

    @Override
    public <K1, V1> void forward(final K1 key, final V1 value, final To to) {
        throw new StreamsException(EXPLANATION);
    }

    @Override
    public <K1 extends K, V1 extends V> void forward(final Record<K1, V1> record) {
        throw new StreamsException(EXPLANATION);
    }

    @Override
    public <K1 extends K, V1 extends V> void forward(final Record<K1, V1> record,
                                                     final String childName) {
        throw new StreamsException(EXPLANATION);
    }

    @Override
    public void commit() {
        delegate.commit();
    }

    @Override
    public String topic() {
        return delegate.topic();
    }

    @Override
    public int partition() {
        return delegate.partition();
    }

    @Override
    public long offset() {
        return delegate.offset();
    }

    @Override
    public Headers headers() {
        return delegate.headers();
    }

    @Override
    public long timestamp() {
        return delegate.timestamp();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return delegate.appConfigs();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return delegate.appConfigsWithPrefix(prefix);
    }

    @Override
    public long currentSystemTimeMs() {
        return delegate.currentSystemTimeMs();
    }

    @Override
    public long currentStreamTimeMs() {
        return delegate.currentSystemTimeMs();
    }
}
