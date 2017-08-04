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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * Metered {@link KeyValueStore} wrapper is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 *
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<K, V> {

    private final KeyValueStore<K, V> inner;
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
    private StreamsMetrics metrics;
    private ProcessorContext context;
    private StateStore root;

    // always wrap the store with the metered store
    public MeteredKeyValueStore(final KeyValueStore<K, V> inner,
                                final String metricScope,
                                final Time time) {
        super(inner);
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time != null ? time : Time.SYSTEM;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        final String tagKey = "task-id";
        final String tagValue = context.taskId().toString();

        this.context = context;
        this.root = root;

        this.metrics = context.metrics();
        this.putTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "put",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.putIfAbsentTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "put-if-absent",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.getTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "get",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.deleteTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "delete",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.putAllTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "put-all",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.allTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "all",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.rangeTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "range",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        this.flushTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "flush",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);
        final Sensor restoreTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name(), "restore",
                Sensor.RecordingLevel.DEBUG, tagKey, tagValue);

        // register and possibly restore the state from the logs
        if (restoreTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.init(MeteredKeyValueStore.this.context, MeteredKeyValueStore.this.root);
                    return null;
                }
            }, restoreTime);
        } else {
            inner.init(MeteredKeyValueStore.this.context, MeteredKeyValueStore.this.root);
        }

    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    interface Action<V> {
        V execute();
    }

    @Override
    public V get(final K key) {
        if (getTime.shouldRecord()) {
            return measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    return inner.get(key);
                }
            }, getTime);
        } else {
            return inner.get(key);
        }
    }

    @Override
    public void put(final K key, final V value) {
        if (putTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.put(key, value);
                    return null;
                }
            }, putTime);
        } else {
            inner.put(key, value);
        }
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        if (putIfAbsentTime.shouldRecord()) {
            return measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    return inner.putIfAbsent(key, value);
                }
            }, putIfAbsentTime);
        } else {
            return inner.putIfAbsent(key, value);
        }

    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        if (putAllTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.putAll(entries);
                    return null;
                }
            }, putAllTime);
        } else {
            inner.putAll(entries);
        }
    }

    @Override
    public V delete(final K key) {
        if (deleteTime.shouldRecord()) {
            return measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    return inner.delete(key);
                }
            }, deleteTime);
        } else {
            return inner.delete(key);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MeteredKeyValueIterator<>(this.inner.range(from, to), this.rangeTime);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator<>(this.inner.all(), this.allTime);
    }

    @Override
    public void flush() {
        if (flushTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.flush();
                    return null;
                }
            }, flushTime);
        } else {
            inner.flush();
        }

    }

    private V measureLatency(final Action<V> action, final Sensor sensor) {
        final long startNs = time.nanoseconds();
        try {
            return action.execute();
        } finally {
            metrics.recordLatency(sensor, startNs, time.nanoseconds());
        }
    }

    private class MeteredKeyValueIterator<K1, V1> implements KeyValueIterator<K1, V1> {

        private final KeyValueIterator<K1, V1> iter;
        private final Sensor sensor;
        private final long startNs;

        MeteredKeyValueIterator(KeyValueIterator<K1, V1> iter, Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K1, V1> next() {
            return iter.next();
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
        public K1 peekNextKey() {
            return iter.peekNextKey();
        }
    }
}
