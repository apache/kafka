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
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

public class MeteredSessionStore<K, V>
    extends WrappedStateStore<SessionStore<Bytes, byte[]>, Windowed<K>, V>
    implements SessionStore<K, V> {

    private final String metricsScope;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;
    private StateSerdes<K, V> serdes;
    private StreamsMetricsImpl streamsMetrics;
    private Sensor putSensor;
    private Sensor fetchSensor;
    private Sensor flushSensor;
    private Sensor removeSensor;
    private Sensor e2eLatencySensor;
    private InternalProcessorContext context;
    private final String threadId;
    private String taskId;

    MeteredSessionStore(final SessionStore<Bytes, byte[]> inner,
                        final String metricsScope,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde,
                        final Time time) {
        super(inner);
        threadId = Thread.currentThread().getName();
        this.metricsScope = metricsScope;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext) context : null;
        initStoreSerde(context);
        taskId = context.taskId().toString();
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(threadId, taskId, metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext) context : null;
        initStoreSerde(context);
        taskId = context.taskId().toString();
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(threadId, taskId, metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }

    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        fetchSensor = StateStoreMetrics.fetchSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        removeSensor = StateStoreMetrics.removeSensor(threadId, taskId, metricsScope, name(), streamsMetrics);
        e2eLatencySensor = StateStoreMetrics.e2ELatencySensor(taskId, metricsScope, name(), streamsMetrics);
    }


    private void initStoreSerde(final ProcessorContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        serdes = new StateSerdes<>(
            changelogTopic != null ?
                changelogTopic :
                ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName),
                WrappingNullableUtils.prepareKeySerde(keySerde, context.keySerde(), context.valueSerde()),
                WrappingNullableUtils.prepareValueSerde(valueSerde, context.keySerde(), context.valueSerde())
        );
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        serdes = new StateSerdes<>(
            changelogTopic != null ?
                changelogTopic :
                ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName),
                WrappingNullableUtils.prepareKeySerde(keySerde, context.keySerde(), context.valueSerde()),
                WrappingNullableUtils.prepareValueSerde(valueSerde, context.keySerde(), context.valueSerde())
        );
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
            maybeMeasureLatency(
                () -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    wrapped().put(new Windowed<>(key, sessionKey.window()), serdes.rawValue(aggregate));
                },
                time,
                putSensor
            );
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        try {
            maybeMeasureLatency(
                () -> {
                    final Bytes key = keyBytes(sessionKey.key());
                    wrapped().remove(new Windowed<>(key, sessionKey.window()));
                },
                time,
                removeSensor
            );
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key());
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V fetchSession(final K key, final long startTime, final long endTime) {
        Objects.requireNonNull(key, "key cannot be null");
        return maybeMeasureLatency(
            () -> {
                final Bytes bytesKey = keyBytes(key);
                final byte[] result = wrapped().fetchSession(bytesKey, startTime, endTime);
                if (result == null) {
                    return null;
                }
                return serdes.valueFrom(result);
            },
            time,
            fetchSensor
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(key)),
            fetchSensor,
            streamsMetrics,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetch(keyBytes(key)),
            fetchSensor,
            streamsMetrics,
            serdes,
            time
        );
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(from), keyBytes(to)),
            fetchSensor,
            streamsMetrics,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFetch(final K from,
                                                          final K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFetch(keyBytes(from), keyBytes(to)),
            fetchSensor,
            streamsMetrics,
            serdes,
            time
        );
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
            fetchSensor,
            streamsMetrics,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K key,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        final Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFindSessions(
                bytesKey,
                earliestSessionEndTime,
                latestSessionStartTime
            ),
            fetchSensor,
            streamsMetrics,
            serdes,
            time
        );
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
            fetchSensor,
            streamsMetrics,
            serdes,
            time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> backwardFindSessions(final K keyFrom,
                                                                 final K keyTo,
                                                                 final long earliestSessionEndTime,
                                                                 final long latestSessionStartTime) {
        Objects.requireNonNull(keyFrom, "keyFrom cannot be null");
        Objects.requireNonNull(keyTo, "keyTo cannot be null");
        final Bytes bytesKeyFrom = keyBytes(keyFrom);
        final Bytes bytesKeyTo = keyBytes(keyTo);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().backwardFindSessions(
                bytesKeyFrom,
                bytesKeyTo,
                earliestSessionEndTime,
                latestSessionStartTime
            ),
            fetchSensor,
            streamsMetrics,
            serdes,
            time
        );
    }

    @Override
    public void flush() {
        maybeMeasureLatency(super::flush, time, flushSensor);
    }

    @Override
    public void close() {
        try {
            wrapped().close();
        } finally {
            streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId, name());
        }
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private void maybeRecordE2ELatency() {
        // Context is null if the provided context isn't an implementation of InternalProcessorContext.
        // In that case, we _can't_ get the current timestamp, so we don't record anything.
        if (e2eLatencySensor.shouldRecord() && context != null) {
            final long currentTime = time.milliseconds();
            final long e2eLatency =  currentTime - context.timestamp();
            e2eLatencySensor.record(e2eLatency, currentTime);
        }
    }
}
