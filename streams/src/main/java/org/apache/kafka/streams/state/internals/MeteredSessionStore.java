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

import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.streams.state.internals.metrics.Sensors.createTaskAndStoreLatencyAndThroughputSensors;

public class MeteredSessionStore<K, V> extends WrappedStateStore.AbstractStateStore implements SessionStore<K, V> {
    private final SessionStore<Bytes, byte[]> inner;
    private final String metricScope;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Time time;
    private StateSerdes<K, V> serdes;
    private StreamsMetricsImpl metrics;
    private Sensor putTime;
    private Sensor fetchTime;
    private Sensor flushTime;
    private Sensor removeTime;
    private String taskName;

    MeteredSessionStore(final SessionStore<Bytes, byte[]> inner,
                        final String metricScope,
                        final Serde<K> keySerde,
                        final Serde<V> valueSerde,
                        final Time time) {
        super(inner);
        this.inner = inner;
        this.metricScope = metricScope;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        //noinspection unchecked
        this.serdes = new StateSerdes<>(ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        this.metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().toString();
        final String metricsGroup = "stream-" + metricScope + "-metrics";
        final Map<String, String> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
        final Map<String, String> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

        putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        removeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "remove", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        final Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

        // register and possibly restore the state from the logs
        final long startNs = time.relativeNanoseconds();
        try {
            inner.init(context, root);
        } finally {
            this.metrics.recordLatency(
                restoreTime,
                startNs,
                time.relativeNanoseconds()
            );
        }
    }

    @Override
    public void close() {
        super.close();
        metrics.removeAllStoreLevelSensors(taskName, name());
    }


    @Override
    public KeyValueIterator<Windowed<K>, V> findSessions(final K key,
                                                         final long earliestSessionEndTime,
                                                         final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");
        final Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(inner.findSessions(bytesKey,
                                                                        earliestSessionEndTime,
                                                                        latestSessionStartTime),
                                                     fetchTime,
                                                     metrics,
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
        return new MeteredWindowedKeyValueIterator<>(inner.findSessions(bytesKeyFrom,
                                                                        bytesKeyTo,
                                                                        earliestSessionEndTime,
                                                                        latestSessionStartTime),
                                                     fetchTime,
                                                     metrics,
                                                     serdes,
                                                     time);
    }

    @Override
    public void remove(final Windowed<K> sessionKey) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        final long startNs = time.relativeNanoseconds();
        try {
            final Bytes key = keyBytes(sessionKey.key());
            inner.remove(new Windowed<>(key, sessionKey.window()));
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key());
            throw new ProcessorStateException(message, e);
        } finally {
            this.metrics.recordLatency(removeTime, startNs, time.relativeNanoseconds());
        }
    }

    @Override
    public void put(final Windowed<K> sessionKey, final V aggregate) {
        Objects.requireNonNull(sessionKey, "sessionKey can't be null");
        final long startNs = time.relativeNanoseconds();
        try {
            final Bytes key = keyBytes(sessionKey.key());
            this.inner.put(new Windowed<>(key, sessionKey.window()), serdes.rawValue(aggregate));
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        } finally {
            this.metrics.recordLatency(this.putTime, startNs, time.relativeNanoseconds());
        }
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    @Override
    public void flush() {
        final long startNs = time.relativeNanoseconds();
        try {
            this.inner.flush();
        } finally {
            this.metrics.recordLatency(this.flushTime, startNs, time.relativeNanoseconds());
        }
    }
}
