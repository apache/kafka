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

import java.util.Map;

import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.streams.state.internals.metrics.Sensors.createTaskAndStoreLatencyAndThroughputSensors;

public class MeteredWindowStore<K, V> extends WrappedStateStore<WindowStore<Bytes, byte[]>> implements WindowStore<K, V> {

    private final String metricScope;
    private final Time time;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private StreamsMetricsImpl metrics;
    private Sensor putTime;
    private Sensor fetchTime;
    private Sensor flushTime;
    private StateSerdes<K, V> serdes;
    private ProcessorContext context;
    private String taskName;

    MeteredWindowStore(final WindowStore<Bytes, byte[]> inner,
                       final String metricScope,
                       final Time time,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde) {
        super(inner);
        this.metricScope = metricScope;
        this.time = time;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context;
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().toString();
        final String metricsGroup = "stream-" + metricScope + "-metrics";
        final Map<String, String> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
        final Map<String, String> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

        putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        final Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

        // register and possibly restore the state from the logs
        final long startNs = time.nanoseconds();
        try {
            super.init(context, root);
        } finally {
            metrics.recordLatency(
                restoreTime,
                startNs,
                time.nanoseconds()
            );
        }
    }

    @Override
    public void close() {
        super.close();
        metrics.removeAllStoreLevelSensors(taskName, name());
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
        final long startNs = time.nanoseconds();
        try {
            wrapped().put(keyBytes(key), serdes.rawValue(value), windowStartTimestamp);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        } finally {
            metrics.recordLatency(putTime, startNs, time.nanoseconds());
        }
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    @Override
    public V fetch(final K key,
                   final long timestamp) {
        final long startNs = time.nanoseconds();
        try {
            final byte[] result = wrapped().fetch(keyBytes(key), timestamp);
            if (result == null) {
                return null;
            }
            return serdes.valueFrom(result);
        } finally {
            metrics.recordLatency(fetchTime, startNs, time.nanoseconds());
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public WindowStoreIterator<V> fetch(final K key,
                                        final long timeFrom,
                                        final long timeTo) {
        return new MeteredWindowStoreIterator<>(wrapped().fetch(keyBytes(key), timeFrom, timeTo),
                                                fetchTime,
                                                metrics,
                                                serdes,
                                                time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return new MeteredWindowedKeyValueIterator<>(wrapped().all(), fetchTime, metrics, serdes, time);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom,
                                                     final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetchAll(timeFrom, timeTo),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    @SuppressWarnings("deprecation")
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from,
                                                  final K to,
                                                  final long timeFrom,
                                                  final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(
            wrapped().fetch(keyBytes(from), keyBytes(to), timeFrom, timeTo),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    @Override
    public void flush() {
        final long startNs = time.nanoseconds();
        try {
            super.flush();
        } finally {
            metrics.recordLatency(flushTime, startNs, time.nanoseconds());
        }
    }

}
