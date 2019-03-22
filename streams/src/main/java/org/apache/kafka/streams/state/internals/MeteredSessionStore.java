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
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.DELETE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.FLUSH;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.GET;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PUT;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RANGE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RESTORE;

public class MeteredSessionStore<K, V>
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, Windowed<K>, V>
    implements SessionStore<K, V> {

    private final String metricScope;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;
    private StateSerdes<K, V> serdes;
    private StoreMetrics storeMetrics;
    private Sensor putTime;
    private Sensor getTime;
    private Sensor rangeTime;
    private Sensor flushTime;
    private Sensor deleteTime;

    MeteredSessionStore(final SessionStore<Bytes, byte[]> inner,
                        final String metricScope,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde,
                        final Time time) {
        super(inner);
        this.metricScope = metricScope;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        storeMetrics = new StoreMetrics(context, metricScope, name(), (StreamsMetricsImpl) context.metrics());

        initStoreSerde(context);

        putTime = storeMetrics.addSensor(PUT);
        getTime = storeMetrics.addSensor(GET);
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
    public boolean setFlushListener(final CacheFlushListener<Windowed<K>, V> listener,
                                    final boolean sendOldValues) {
        final SessionStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                (key, newValue, oldValue, timestamp) -> listener.apply(
                    SessionKeySchema.from(key, serdes.keyDeserializer(), serdes.topic()),
                    newValue != null ? serdes.valueFrom(newValue) : null,
                    oldValue != null ? serdes.valueFrom(oldValue) : null,
                    timestamp
                ),
                sendOldValues);
        }
        return false;
    }

    @Override
    public void put(final Windowed<K> sessionKey,
                    final V aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");

        try {
            StreamsMetricsImpl.maybeMeasureLatency(() -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    wrapped().put(new Windowed<>(key, sessionKey.window()), serdes.rawValue(aggregate));
                },
                time,
                putTime);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");

        try {
            StreamsMetricsImpl.maybeMeasureLatency(() -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    wrapped().remove(new Windowed<>(key, sessionKey.window()));
                },
                time,
                deleteTime);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key());
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V fetchSession(final K key, final long startTime, final long endTime) {
        Objects.requireNonNull(key, "key cannot be null");

        return StreamsMetricsImpl.maybeMeasureLatency(() -> {
                final Bytes bytesKey = keyBytes(key);
                return serdes.valueFrom(wrapped().fetchSession(bytesKey, startTime, endTime));
            },
            time,
            getTime);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        final Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().findSessions(
                bytesKey,
                earliestSessionEndTime,
                latestSessionStartTime),
            rangeTime,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K keyFrom,
                                                         final K keyTo,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        Objects.requireNonNull(keyFrom, "keyFrom cannot be null");
        Objects.requireNonNull(keyTo, "keyTo cannot be null");
        final Bytes bytesKeyFrom = keyBytes(keyFrom);
        final Bytes bytesKeyTo = keyBytes(keyTo);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().findSessions(
                bytesKeyFrom,
                bytesKeyTo,
                earliestSessionEndTime,
                latestSessionStartTime),
            rangeTime,
            serdes,
            time);
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
