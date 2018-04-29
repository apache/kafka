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
package org.apache.kafka.test;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.CompositeRestoreListener;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.ToInternal;
import org.apache.kafka.streams.processor.internals.WrappedBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InternalMockProcessorContext extends AbstractProcessorContext implements RecordCollector.Supplier {

    private final File stateDir;
    private final RecordCollector.Supplier recordCollectorSupplier;
    private final Map<String, StateStore> storeMap = new LinkedHashMap<>();
    private final Map<String, StateRestoreCallback> restoreFuncs = new HashMap<>();
    private final ToInternal toInternal = new ToInternal();

    private Serde<?> keySerde;
    private Serde<?> valSerde;
    private long timestamp = -1L;

    public InternalMockProcessorContext() {
        this(null,
            null,
            null,
            new StreamsMetricsImpl(new Metrics(), "mock"),
            new StreamsConfig(StreamsTestUtils.minimalStreamsConfig()),
            null,
            null
        );
    }

    public InternalMockProcessorContext(final File stateDir,
                                        final StreamsConfig config) {
        this(stateDir, null, null, new StreamsMetricsImpl(new Metrics(), "mock"), config, null, null);
    }

    public InternalMockProcessorContext(final StateSerdes<?, ?> serdes,
                                        final RecordCollector collector) {
        this(null, serdes.keySerde(), serdes.valueSerde(), collector, null);
    }

    public InternalMockProcessorContext(final StateSerdes<?, ?> serdes,
                                        final RecordCollector collector,
                                        final Metrics metrics) {
        this(
            null,
            serdes.keySerde(),
            serdes.valueSerde(),
            new StreamsMetricsImpl(metrics, "mock"),
            new StreamsConfig(StreamsTestUtils.minimalStreamsConfig()), new RecordCollector.Supplier() {
                @Override
                public RecordCollector recordCollector() {
                    return collector;
                }
            },
            null
        );
    }

    public InternalMockProcessorContext(final File stateDir,
                                        final Serde<?> keySerde,
                                        final Serde<?> valSerde,
                                        final RecordCollector collector,
                                        final ThreadCache cache) {
        this(stateDir,
            keySerde,
            valSerde,
            new StreamsMetricsImpl(new Metrics(), "mock"),
            new StreamsConfig(StreamsTestUtils.minimalStreamsConfig()),
            new RecordCollector.Supplier() {
                @Override
                public RecordCollector recordCollector() {
                    return collector;
                }
            },
            cache
        );
    }

    public InternalMockProcessorContext(final File stateDir,
                                        final Serde<?> keySerde,
                                        final Serde<?> valSerde,
                                        final StreamsMetricsImpl metrics,
                                        final StreamsConfig config,
                                        final RecordCollector.Supplier collectorSupplier,
                                        final ThreadCache cache) {
        super(new TaskId(0, 0),
            config,
            metrics,
            null,
            cache);
        this.stateDir = stateDir;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.recordCollectorSupplier = collectorSupplier;
    }

    @Override
    public RecordCollector recordCollector() {
        final RecordCollector recordCollector = recordCollectorSupplier.recordCollector();

        if (recordCollector == null) {
            throw new UnsupportedOperationException("No RecordCollector specified");
        }
        return recordCollector;
    }

    public void setKeySerde(final Serde<?> keySerde) {
        this.keySerde = keySerde;
    }

    public void setValueSerde(final Serde<?> valSerde) {
        this.valSerde = valSerde;
    }

    // serdes will override whatever specified in the configs
    @Override
    public Serde<?> keySerde() {
        return keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return valSerde;
    }

    // state mgr will be overridden by the state dir and store maps
    @Override
    public void initialized() {}

    @Override
    public File stateDir() {
        if (stateDir == null) {
            throw new UnsupportedOperationException("State directory not specified");
        }

        return stateDir;
    }

    @Override
    public void register(final StateStore store,
                         final boolean deprecatedAndIgnoredLoggingEnabled,
                         final StateRestoreCallback func) {
        storeMap.put(store.name(), store);
        restoreFuncs.put(store.name(), func);
    }

    @Override
    public StateStore getStateStore(final String name) {
        return storeMap.get(name);
    }

    @Override
    public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("schedule() not supported.");
    }

    @Override
    public void schedule(final long interval) { }

    @Override
    public void commit() { }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value) {
        forward(key, value, To.all());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        forward(key, value, To.child(((List<ProcessorNode>) currentNode().children()).get(childIndex).name()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value, final String childName) {
        forward(key, value, To.child(childName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        toInternal.update(to);
        if (toInternal.hasTimestamp()) {
            setTime(toInternal.timestamp());
        }
        final ProcessorNode thisNode = currentNode;
        try {
            for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
                if (toInternal.child() == null || toInternal.child().equals(childNode.name())) {
                    currentNode = childNode;
                    childNode.process(key, value);
                    toInternal.update(to); // need to reset because MockProcessorContext is shared over multiple Processors and toInternal might have been modified
                }
            }
        } finally {
            currentNode = thisNode;
        }
    }

    // allow only setting time but not other fields in for record context,
    // and also not throwing exceptions if record context is not available.
    public void setTime(final long timestamp) {
        if (recordContext != null) {
            recordContext = new ProcessorRecordContext(timestamp, recordContext.offset(), recordContext.partition(), recordContext.topic());
        }
        this.timestamp = timestamp;
    }

    @Override
    public long timestamp() {
        if (recordContext == null) {
            return timestamp;
        }
        return recordContext.timestamp();
    }

    @Override
    public String topic() {
        if (recordContext == null) {
            return null;
        }
        return recordContext.topic();
    }

    @Override
    public int partition() {
        if (recordContext == null) {
            return -1;
        }
        return recordContext.partition();
    }

    @Override
    public long offset() {
        if (recordContext == null) {
            return -1L;
        }
        return recordContext.offset();
    }

    Map<String, StateStore> allStateStores() {
        return Collections.unmodifiableMap(storeMap);
    }

    public void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog) {

        final BatchingStateRestoreCallback restoreCallback = getBatchingRestoreCallback(restoreFuncs.get(storeName));
        final StateRestoreListener restoreListener = getStateRestoreListener(restoreCallback);

        restoreListener.onRestoreStart(null, storeName, 0L, 0L);

        List<KeyValue<byte[], byte[]>> records = new ArrayList<>();
        for (KeyValue<byte[], byte[]> keyValue : changeLog) {
            records.add(keyValue);
        }

        restoreCallback.restoreAll(records);

        restoreListener.onRestoreEnd(null, storeName, 0L);
    }

    private StateRestoreListener getStateRestoreListener(StateRestoreCallback restoreCallback) {
        if (restoreCallback instanceof StateRestoreListener) {
            return (StateRestoreListener) restoreCallback;
        }

        return CompositeRestoreListener.NO_OP_STATE_RESTORE_LISTENER;
    }

    private BatchingStateRestoreCallback getBatchingRestoreCallback(StateRestoreCallback restoreCallback) {
        if (restoreCallback instanceof BatchingStateRestoreCallback) {
            return (BatchingStateRestoreCallback) restoreCallback;
        }

        return new WrappedBatchingStateRestoreCallback(restoreCallback);
    }

}
