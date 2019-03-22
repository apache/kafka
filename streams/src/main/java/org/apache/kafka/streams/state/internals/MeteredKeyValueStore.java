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

import static org.apache.kafka.streams.state.internals.StoreMetrics.ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.DELETE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.FLUSH;
import static org.apache.kafka.streams.state.internals.StoreMetrics.GET;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_IF_ABSENT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RANGE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RESTORE;

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
    private StoreMetrics storeMetrics;
    private Sensor putTime;
    private Sensor putIfAbsentTime;
    private Sensor getTime;
    private Sensor deleteTime;
    private Sensor putAllTime;
    private Sensor allTime;
    private Sensor rangeTime;
    private Sensor flushTime;

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
        storeMetrics = new StoreMetrics(context, metricScope, name(), (StreamsMetricsImpl) context.metrics());

        initStoreSerde(context);

        putTime = storeMetrics.addSensor(PUT);
        putIfAbsentTime = storeMetrics.addSensor(PUT_IF_ABSENT);
        putAllTime = storeMetrics.addSensor(PUT_ALL);
        getTime = storeMetrics.addSensor(GET);
        allTime = storeMetrics.addSensor(ALL);
        rangeTime = storeMetrics.addSensor(RANGE);
        flushTime = storeMetrics.addSensor(FLUSH);
        deleteTime = storeMetrics.addSensor(DELETE);

        // register and possibly restore the state from the logs
        final Sensor restoreTime = storeMetrics.addSensor(RESTORE);
        StreamsMetricsImpl.maybeMeasureLatency(() -> {
                super.init(context, root);
            },
            time,
            restoreTime);
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
            return StreamsMetricsImpl.maybeMeasureLatency(() ->
                    outerValue(wrapped().get(keyBytes(key))),
                time,
                getTime);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final V value) {
        try {
            StreamsMetricsImpl.maybeMeasureLatency(() -> {
                    wrapped().put(keyBytes(key), serdes.rawValue(value));
                },
                time,
                putTime);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        return StreamsMetricsImpl.maybeMeasureLatency(() ->
                outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value))),
            time,
            putIfAbsentTime);
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        StreamsMetricsImpl.maybeMeasureLatency(() -> {
                wrapped().putAll(innerEntries(entries));
            },
            time,
            putAllTime);
    }

    @Override
    public V delete(final K key) {
        try {
            return StreamsMetricsImpl.maybeMeasureLatency(() ->
                    outerValue(wrapped().delete(keyBytes(key))),
                time,
                deleteTime);
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
        StreamsMetricsImpl.maybeMeasureLatency(super::flush, time, flushTime);
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void close() {
        super.close();
        storeMetrics.removeAllSensors();
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
                sensor.record(time.nanoseconds() - startNs);
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
