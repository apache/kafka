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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
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
    private StateSerdes<K, V> serdes;
    private final LongSerializer longSerializer = new LongSerializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();

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
        final String name = name();
        final String tagKey = "task-id";
        final String taskName = context.taskId().toString();

        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.metrics = (StreamsMetricsImpl) context.metrics();
        this.putTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "put",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.putIfAbsentTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "put-if-absent",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.getTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "get",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.deleteTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "delete",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.putAllTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "put-all",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.allTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "all",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.rangeTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "range",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        this.flushTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "flush",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);
        final Sensor restoreTime = this.metrics.addLatencyAndThroughputSensor(
            taskName,
            metricScope,
            name,
            "restore",
            Sensor.RecordingLevel.DEBUG,
            tagKey, taskName);

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
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    @Override
    public ValueAndTimestamp<V> get(final K key) {
        try {
            if (getTime.shouldRecord()) {
                return measureLatency(() -> outerValue(inner.get(Bytes.wrap(serdes.rawKey(key)))), getTime);
            } else {
                return outerValue(inner.get(Bytes.wrap(serdes.rawKey(key))));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final ValueAndTimestamp<V> value) {
        put(key, value.value(), value.timestamp());
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long timestamp) {
        try {
            if (putTime.shouldRecord()) {
                measureLatency(() -> {
                    inner.put(Bytes.wrap(serdes.rawKey(key)), innerValue(value, timestamp));
                    return null;
                }, putTime);
            } else {
                inner.put(Bytes.wrap(serdes.rawKey(key)), innerValue(value, timestamp));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final ValueAndTimestamp<V> value) {
        return putIfAbsent(key, value.value(), value.timestamp());
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                                            final V value,
                                            final long timestamp) {
        if (putIfAbsentTime.shouldRecord()) {
            return measureLatency(
                () -> outerValue(inner.putIfAbsent(Bytes.wrap(serdes.rawKey(key)), innerValue(value, timestamp))),
                putIfAbsentTime);
        } else {
            return outerValue(inner.putIfAbsent(Bytes.wrap(serdes.rawKey(key)), innerValue(value, timestamp)));
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
                return measureLatency(() -> outerValue(inner.delete(Bytes.wrap(serdes.rawKey(key)))), deleteTime);
            } else {
                return outerValue(inner.delete(Bytes.wrap(serdes.rawKey(key))));
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

    private ValueAndTimestamp<V> outerValue(final byte[] rawValueAndTimestamp) {
        if (rawValueAndTimestamp == null) {
            return null;
        }

        final byte[] rawTimestamp = new byte[8];
        final byte[] rawValue = new byte[rawValueAndTimestamp.length - 8];

        System.arraycopy(rawValueAndTimestamp, 0, rawTimestamp, 0, 8);
        System.arraycopy(rawValueAndTimestamp, 8, rawValue, 0, rawValueAndTimestamp.length - 8);

        return new ValueAndTimestampImpl<>(
            valueSerde.deserializer().deserialize(null, rawValue),
            longDeserializer.deserialize(null, rawTimestamp));
    }

    private byte[] innerValue(final V value,
                              final long timestamp) {
        if (value == null) {
            return null;
        }

        final byte[] rawTimestamp = longSerializer.serialize(null, timestamp);
        final byte[] rawValue = valueSerde.serializer().serialize(null, value);

        final byte[] rawValueAndTimestamp = new byte[8 + rawValue.length];
        System.arraycopy(rawTimestamp, 0, rawValueAndTimestamp, 0, 8);
        System.arraycopy(rawValue, 0, rawValueAndTimestamp, 8, rawValue.length);

        return rawValueAndTimestamp;
    }

    private List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, ValueAndTimestamp<V>>> from) {
        final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, ValueAndTimestamp<V>> entry : from) {
            final ValueAndTimestamp<V> valueAndTimestamp = entry.value;
            byteEntries.add(KeyValue.pair(
                Bytes.wrap(serdes.rawKey(entry.key)),
                innerValue(valueAndTimestamp.value(), valueAndTimestamp.timestamp())));
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
            return KeyValue.pair(serdes.keyFrom(keyValue.key.get()), outerValue(keyValue.value));
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
