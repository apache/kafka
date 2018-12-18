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
import org.apache.kafka.streams.state.KeyValueWithTimestampStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;

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
public class MeteredKeyValueWithTimestampStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueWithTimestampStore<K, V> {

    private final KeyValueStore<Bytes, byte[]> inner;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String metricScope;
    protected final Time time;

    private StateSerdes<K, V> serdes;
    private KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
    private KeyValueWithTimestampStoreBuilder.ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;
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

    MeteredKeyValueWithTimestampStore(final KeyValueStore<Bytes, byte[]> inner,
                                      final String metricScope,
                                      final Time time,
                                      final Serde<K> keySerde,
                                      final Serde<V> valueSerde) {
        super(inner);
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().toString();
        final String metricsGroup = "stream-" + metricScope + "-metrics";
        final Map<String, String> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
        final Map<String, String> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        valueAndTimestampSerializer = new KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerializer<>(serdes.valueSerializer());
        valueAndTimestampDeserializer = new KeyValueWithTimestampStoreBuilder.ValueAndTimestampDeserializer<>(serdes.valueDeserializer());

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
                    inner.init(context, root);
                    return null;
                },
                restoreTime);
        } else {
            inner.init(context, root);
        }
    }

    @Override
    public void close() {
        super.close();
        metrics.removeAllStoreLevelSensors(taskName, name());
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    @Override
    public ValueAndTimestamp<V> get(final K key) {
        try {
            if (getTime.shouldRecord()) {
                return measureLatency(() -> valueAndTimestampDeserializer.deserialize(null, inner.get(keyBytes(key))), getTime);
            } else {
                return valueAndTimestampDeserializer.deserialize(null, inner.get(keyBytes(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final ValueAndTimestamp<V> valueAndTimestamp) {
        if (valueAndTimestamp != null) {
            put(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
        } else {
            put(key, null, -1L);
        }
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long timestamp) {
        try {
            if (putTime.shouldRecord()) {
                measureLatency(() -> {
                    inner.put(keyBytes(key), valueAndTimestampSerializer.serialize(null, value, timestamp));
                    return null;
                }, putTime);
            } else {
                inner.put(keyBytes(key), valueAndTimestampSerializer.serialize(null, value, timestamp));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final ValueAndTimestamp<V> valueAndTimestamp) {
        if (valueAndTimestamp != null) {
            return putIfAbsent(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
        } else {
            return putIfAbsent(key, null, -1L);
        }
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final V value,
                                            final long timestamp) {
        if (putIfAbsentTime.shouldRecord()) {
            return measureLatency(
                () -> valueAndTimestampDeserializer.deserialize(
                    null,
                    inner.putIfAbsent(keyBytes(key), valueAndTimestampSerializer.serialize(null, value, timestamp))),
                putIfAbsentTime);
        } else {
            return valueAndTimestampDeserializer.deserialize(
                null, inner.putIfAbsent(keyBytes(key),
                    valueAndTimestampSerializer.serialize(null, value, timestamp)));
        }
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        if (putAllTime.shouldRecord()) {
            measureLatency(
                () -> {
                    inner.putAll(innerEntries(entries));
                    return null;
                },
                putAllTime);
        } else {
            inner.putAll(innerEntries(entries));
        }
    }

    @Override
    public ValueAndTimestamp<V> delete(final K key) {
        try {
            if (deleteTime.shouldRecord()) {
                return measureLatency(() -> valueAndTimestampDeserializer.deserialize(null, inner.delete(keyBytes(key))), deleteTime);
            } else {
                return valueAndTimestampDeserializer.deserialize(null, inner.delete(keyBytes(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from,
                                                           final K to) {
        return new MeteredKeyValueIterator(
            this.inner.range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
            this.rangeTime);
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        return new MeteredKeyValueIterator(this.inner.all(), this.allTime);
    }

    @Override
    public void flush() {
        if (flushTime.shouldRecord()) {
            measureLatency(
                () -> {
                    inner.flush();
                    return null;
                },
                flushTime);
        } else {
            inner.flush();
        }
    }

    private interface Action<V> {
        ValueAndTimestamp<V> execute();
    }

    private ValueAndTimestamp<V> measureLatency(final Action<V> action,
                                                final Sensor sensor) {
        final long startNs = time.nanoseconds();
        try {
            return action.execute();
        } finally {
            metrics.recordLatency(sensor, startNs, time.nanoseconds());
        }
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, ValueAndTimestamp<V>>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, ValueAndTimestamp<V>> entry : from) {
            final ValueAndTimestamp<V> valueAndTimestamp = entry.value;
            byteEntries.add(KeyValue.pair(
                Bytes.wrap(serdes.rawKey(entry.key)),
                valueAndTimestampSerializer.serialize(null, valueAndTimestamp)));
        }
        return byteEntries;
    }

    private class MeteredKeyValueIterator implements KeyValueIterator<K, ValueAndTimestamp<V>> {

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
        public KeyValue<K, ValueAndTimestamp<V>> next() {
            final KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(serdes.keyFrom(keyValue.key.get()), valueAndTimestampDeserializer.deserialize(null, keyValue.value));
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
                metrics.recordLatency(this.sensor, this.startNs, time.nanoseconds());
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}