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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;

import java.time.Duration;

import static org.apache.kafka.streams.processor.internals.AbstractReadWriteDecorator.wrapWithReadWriteStore;

public class GlobalProcessorContextImpl extends AbstractProcessorContext<Object, Object> {

    private final GlobalStateManager stateManager;
    private final Time time;

    public GlobalProcessorContextImpl(final StreamsConfig config,
                                      final GlobalStateManager stateMgr,
                                      final StreamsMetricsImpl metrics,
                                      final ThreadCache cache,
                                      final Time time) {
        super(new TaskId(-1, -1), config, metrics, cache);
        stateManager = stateMgr;
        this.time = time;
    }

    @Override
    protected StateManager stateManager() {
        return stateManager;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends StateStore> S getStateStore(final String name) {
        final StateStore store = stateManager.globalStore(name);
        return (S) wrapWithReadWriteStore(store);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final Record<K, V> record) {
        final ProcessorNode<?, ?, ?, ?> previousNode = currentNode();
        try {
            for (final ProcessorNode<?, ?, ?, ?> child : currentNode().children()) {
                setCurrentNode(child);
                ((ProcessorNode<K, V, ?, ?>) child).process(record);
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @Override
    public <K, V> void forward(final Record<K, V> record, final String childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    @Override
    public <KIn, VIn> void forward(final KIn key, final VIn value) {
        forward(new Record<>(key, value, timestamp(), headers()));
    }

    /**
     * No-op. This should only be called on GlobalStateStore#flush and there should be no child nodes
     */
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        if (!currentNode().children().isEmpty()) {
            throw new IllegalStateException("This method should only be called on 'GlobalStateStore.flush' that should not have any children.");
        }
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
    public void commit() {
        //no-op
    }

    @Override
    public long currentSystemTimeMs() {
        return time.milliseconds();
    }

    @Override
    public long currentStreamTimeMs() {
        throw new UnsupportedOperationException("There is no concept of stream-time for a global processor.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    @Override
    public void logChange(final String storeName,
                          final Bytes key,
                          final byte[] value,
                          final long timestamp,
                          final Position position) {
        throw new UnsupportedOperationException("this should not happen: logChange() not supported in global processor context.");
    }

    @Override
    public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
        throw new UnsupportedOperationException("this should not happen: transitionToActive() not supported in global processor context.");
    }

    @Override
    public void transitionToStandby(final ThreadCache newCache) {
        throw new UnsupportedOperationException("this should not happen: transitionToStandby() not supported in global processor context.");
    }

    @Override
    public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
        cache.addDirtyEntryFlushListener(namespace, listener);
    }
}
