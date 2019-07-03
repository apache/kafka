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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.streams.state.internals.metrics.Sensors.createTaskAndStoreLatencyAndThroughputSensors;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V>
    extends WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
    implements KeyValueStore<K, V> {

    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private final String metricScope;
    protected final Time time;
    private Sensor putTime;
    private Sensor putIfAbsentTime;
    private Sensor getTime;
    private Sensor deleteTime;
    private Sensor putAllTime;
    private Sensor allTime;
    private Sensor rangeTime;
    private Sensor flushTime;
    private StreamsMetricsImpl metrics;
    private String taskName;

    MeteredKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                         final String metricScope,
                         final Time time,
                         final Serde<K> keySerde,
                         final Serde<V> valueSerde) {
        super(inner);
        this.metricScope = metricScope;
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().toString();
        final String metricsGroup = "stream-" + metricScope + "-metrics";
        final Map<String, String> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
        final Map<String, String> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

        initStoreSerde(context);

        putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        putIfAbsentTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-if-absent", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        putAllTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-all", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        getTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "get", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        allTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "all", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        rangeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "range", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        deleteTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "delete", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        final Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

        // register and possibly restore the state from the logs
        if (restoreTime.shouldRecord()) {
            measureLatency(
                () -> {
                    super.init(context, root);
                    return null;
                },
                restoreTime);
        } else {
            super.init(context, root);
        }
    }

    @SuppressWarnings("unchecked")
    void initStoreSerde(final ProcessorContext context) {
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<K, V> listener,
                                    final boolean sendOldValues) {
        final KeyValueStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                (rawKey, rawNewValue, rawOldValue, timestamp) -> listener.apply(
                    serdes.keyFrom(rawKey),
                    rawNewValue != null ? serdes.valueFrom(rawNewValue) : null,
                    rawOldValue != null ? serdes.valueFrom(rawOldValue) : null,
                    timestamp
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public V get(final K key) {
        try {
            if (getTime.shouldRecord()) {
                return measureLatency(() -> outerValue(wrapped().get(keyBytes(key))), getTime);
            } else {
                return outerValue(wrapped().get(keyBytes(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final V value) {
        try {
            if (putTime.shouldRecord()) {
                measureLatency(() -> {
                    wrapped().put(keyBytes(key), serdes.rawValue(value));
                    return null;
                }, putTime);
            } else {
                wrapped().put(keyBytes(key), serdes.rawValue(value));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        if (putIfAbsentTime.shouldRecord()) {
            return measureLatency(
                () -> outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value))),
                putIfAbsentTime);
        } else {
            return outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value)));
        }
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        if (putAllTime.shouldRecord()) {
            measureLatency(
                () -> {
                    wrapped().putAll(innerEntries(entries));
                    return null;
                },
                putAllTime);
        } else {
            wrapped().putAll(innerEntries(entries));
        }
    }

    @Override
    public V delete(final K key) {
        try {
            if (deleteTime.shouldRecord()) {
                return measureLatency(() -> outerValue(wrapped().delete(keyBytes(key))), deleteTime);
            } else {
                return outerValue(wrapped().delete(keyBytes(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        return new MeteredKeyValueIterator(
            wrapped().range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
            rangeTime);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator(wrapped().all(), allTime);
    }

    @Override
    public void flush() {
        if (flushTime.shouldRecord()) {
            measureLatency(
                () -> {
                    super.flush();
                    return null;
                },
                flushTime);
        } else {
            super.flush();
        }
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void close() {
        super.close();
        metrics.removeAllStoreLevelSensors(taskName, name());
    }

    private interface Action<V> {
        V execute();
    }

    private V measureLatency(final Action<V> action,
                             final Sensor sensor) {
        final long startNs = time.nanoseconds();
        try {
            return action.execute();
        } finally {
            metrics.recordLatency(sensor, startNs, time.nanoseconds());
        }
    }

    private V outerValue(final byte[] value) {
        return value != null ? serdes.valueFrom(value) : null;
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, V>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, V> entry : from) {
            byteEntries.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        return byteEntries;
    }

    private class MeteredKeyValueIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final Sensor sensor;
        private final long startNs;

        private MeteredKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                        final Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                outerValue(keyValue.value));
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
