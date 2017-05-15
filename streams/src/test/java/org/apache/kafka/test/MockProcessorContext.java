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

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MockProcessorContext implements InternalProcessorContext, RecordCollector.Supplier {

    private final Serde<?> keySerde;
    private final Serde<?> valSerde;
    private final RecordCollector.Supplier recordCollectorSupplier;
    private final File stateDir;
    private final Metrics metrics;
    private final StreamsMetrics streamsMetrics;
    private final ThreadCache cache;
    private final Map<String, StateStore> storeMap = new LinkedHashMap<>();

    private final Map<String, StateRestoreCallback> restoreFuncs = new HashMap<>();

    private long timestamp = -1L;
    private RecordContext recordContext;
    private ProcessorNode currentNode;

    public MockProcessorContext(final StateSerdes<?, ?> serdes, final RecordCollector collector) {
        this(null, serdes.keySerde(), serdes.valueSerde(), collector, null);
    }

    public MockProcessorContext(final File stateDir,
                                final Serde<?> keySerde,
                                final Serde<?> valSerde,
                                final RecordCollector collector,
                                final ThreadCache cache) {
        this(stateDir, keySerde, valSerde,
                new RecordCollector.Supplier() {
                    @Override
                    public RecordCollector recordCollector() {
                        return collector;
                    }
                },
                cache);
    }

    public MockProcessorContext(final File stateDir,
                                final Serde<?> keySerde,
                                final Serde<?> valSerde,
                                final RecordCollector.Supplier collectorSupplier,
                                final ThreadCache cache) {
        this.stateDir = stateDir;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        recordCollectorSupplier = collectorSupplier;
        metrics = new Metrics(new MetricConfig(), Collections.singletonList((MetricsReporter) new JmxReporter()), new MockTime(), true);
        this.cache = cache;
        streamsMetrics = new MockStreamsMetrics(metrics);
    }

    @Override
    public RecordCollector recordCollector() {
        final RecordCollector recordCollector = recordCollectorSupplier.recordCollector();

        if (recordCollector == null) {
            throw new UnsupportedOperationException("No RecordCollector specified");
        }
        return recordCollector;
    }

    public void setTime(final long timestamp) {
        if (recordContext != null) {
            recordContext = new ProcessorRecordContext(timestamp, recordContext.offset(), recordContext.partition(), recordContext.topic());
        }
        this.timestamp = timestamp;
    }

    public Metrics baseMetrics() {
        return metrics;
    }

    @Override
    public TaskId taskId() {
        return new TaskId(0, 0);
    }

    @Override
    public String applicationId() {
        return "mockApplication";
    }

    @Override
    public Serde<?> keySerde() {
        return keySerde;
    }

    @Override
    public Serde<?> valueSerde() {
        return valSerde;
    }

    @Override
    public ThreadCache getCache() {
        return cache;
    }

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
    public StreamsMetrics metrics() {
        return streamsMetrics;
    }

    @Override
    public void register(final StateStore store, final boolean loggingEnabled, final StateRestoreCallback func) {
        storeMap.put(store.name(), store);
        restoreFuncs.put(store.name(), func);
    }

    @Override
    public StateStore getStateStore(final String name) {
        return storeMap.get(name);
    }

    @Override public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("schedule() not supported.");
    }

    @Override
    public void schedule(final long interval) {
        throw new UnsupportedOperationException("schedule() not supported.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value) {
        final ProcessorNode thisNode = currentNode;
        for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
            currentNode = childNode;
            try {
                childNode.process(key, value);
            } finally {
                currentNode = thisNode;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        final ProcessorNode thisNode = currentNode;
        final ProcessorNode childNode = (ProcessorNode<K, V>) thisNode.children().get(childIndex);
        currentNode = childNode;
        try {
            childNode.process(key, value);
        } finally {
            currentNode = thisNode;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(final K key, final V value, final String childName) {
        final ProcessorNode thisNode = currentNode;
        for (final ProcessorNode childNode : (List<ProcessorNode<K, V>>) thisNode.children()) {
            if (childNode.name().equals(childName)) {
                currentNode = childNode;
                try {
                    childNode.process(key, value);
                } finally {
                    currentNode = thisNode;
                }
                break;
            }
        }
    }


    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
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

    @Override
    public long timestamp() {
        if (recordContext == null) {
            return timestamp;
        }
        return recordContext.timestamp();
    }

    @Override
    public Map<String, Object> appConfigs() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Object> appConfigsWithPrefix(final String prefix) {
        return Collections.emptyMap();
    }

    @Override
    public RecordContext recordContext() {
        return recordContext;
    }

    Map<String, StateStore> allStateStores() {
        return Collections.unmodifiableMap(storeMap);
    }

    public void restore(final String storeName, final Iterable<KeyValue<byte[], byte[]>> changeLog) {
        final StateRestoreCallback restoreCallback = restoreFuncs.get(storeName);
        for (final KeyValue<byte[], byte[]> entry : changeLog) {
            restoreCallback.restore(entry.key, entry.value);
        }
    }

    @Override
    public void setRecordContext(final RecordContext recordContext) {
        this.recordContext = recordContext;
    }

    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        this.currentNode = currentNode;
    }

    @Override
    public ProcessorNode currentNode() {
        return currentNode;
    }

    public void close() {
        metrics.close();
    }
}
