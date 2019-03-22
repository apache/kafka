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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.apache.kafka.streams.state.internals.StoreMetrics.FLUSH;
import static org.apache.kafka.streams.state.internals.StoreMetrics.GET;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RANGE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RESTORE;


public class MeteredWindowStore<K, V>
    extends WrappedStateStore<WindowStore<Bytes, byte[]>, Windowed<K>, V>
    implements WindowStore<K, V> {

    private final long windowSizeMs;
    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private final String metricScope;
    private final Time time;
    private StoreMetrics storeMetrics;
    private Sensor putTime;
    private Sensor getTime;
    private Sensor rangeTime;
    private Sensor flushTime;
    private ProcessorContext context;

    MeteredWindowStore(final WindowStore<Bytes, byte[]> inner,
                       final long windowSizeMs,
                       final String metricScope,
                       final Time time,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde) {
        super(inner);
        this.windowSizeMs = windowSizeMs;
        this.metricScope = metricScope;
        this.time = time;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context;
        storeMetrics = new StoreMetrics(context, metricScope, name(), (StreamsMetricsImpl) context.metrics());

        initStoreSerde(context);

        putTime = storeMetrics.addSensor(PUT);
        getTime = storeMetrics.addSensor(GET);
        rangeTime = storeMetrics.addSensor(RANGE);
        flushTime = storeMetrics.addSensor(FLUSH);

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
    public boolean setFlushListener(final CacheFlushListener<Windowed<K>, V> listener,
                                    final boolean sendOldValues) {
        final WindowStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                (key, newValue, oldValue, timestamp) -> listener.apply(
                    WindowKeySchema.fromStoreKey(key, windowSizeMs, serdes.keyDeserializer(), serdes.topic()),
                    newValue != null ? serdes.valueFrom(newValue) : null,
                    oldValue != null ? serdes.valueFrom(oldValue) : null,
                    timestamp
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public void put(final K key,
                    final V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final K key,
                    final V value,
                    final long windowStartTimestamp) {
        try {
            StreamsMetricsImpl.maybeMeasureLatency(() -> {
                    wrapped().put(keyBytes(key), serdes.rawValue(value), windowStartTimestamp);
                },
                time,
                putTime);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V fetch(final K key,
                   final long timestamp) {
        return StreamsMetricsImpl.maybeMeasureLatency(() -> {
                final byte[] result = wrapped().fetch(keyBytes(key), timestamp);
                if (result == null) {
                    return null;
                }
                return serdes.valueFrom(result);
            },
            time,
            getTime);
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        return new MeteredWindowStoreIterator<>(wrapped().fetch(keyBytes(key), timeFrom, timeTo),
            rangeTime,
            serdes,
            time);
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetchAll(...) is removed
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to,
                                                  final long timeFrom,
                                                  final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(from), keyBytes(to), timeFrom, timeTo),
            rangeTime,
            serdes,
            time);
    }

    @SuppressWarnings("deprecation") // note, this method must be kept if super#fetch(...) is removed
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetchAll(timeFrom, timeTo),
            rangeTime,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return new MeteredWindowedKeyValueIterator<>(wrapped().all(), rangeTime, serdes, time);
    }

    @Override
    public void flush() {
        StreamsMetricsImpl.maybeMeasureLatency(super::flush, time, flushTime);
    }

    @Override
    public void close() {
        super.close();
        storeMetrics.removeAllSensors();
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }
}
