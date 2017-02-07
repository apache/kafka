/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StreamsMetricsImpl;
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
    private Sensor restoreTime;
    private StreamsMetricsImpl metrics;


    private K key;
    private V value;
    private Runnable getDelegate = new Runnable() {
        @Override
        public void run() {
            value = inner.get(key);
        }
    };
    private Runnable putDelegate = new Runnable() {
        @Override
        public void run() {
            inner.put(key, value);
        }
    };
    private Runnable putIfAbsentDelegate = new Runnable() {
        @Override
        public void run() {
            value = inner.putIfAbsent(key, value);
        }
    };
    private List<KeyValue<K, V>> entries;
    private Runnable putAllDelegate = new Runnable() {
        @Override
        public void run() {
            inner.putAll(entries);
        }
    };
    private Runnable deleteDelegate = new Runnable() {
        @Override
        public void run() {
            value = inner.delete(key);
        }
    };
    private Runnable flushDelegate = new Runnable() {
        @Override
        public void run() {
            inner.flush();
        }
    };
    private ProcessorContext context;
    private StateStore root;
    private Runnable initDelegate = new Runnable() {
        @Override
        public void run() {
            inner.init(context, root);
        }
    };

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
        final String name = name();
        this.context = context;
        this.root = root;
        this.metrics = (StreamsMetricsImpl) context.metrics();
        this.putTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "put", Sensor.RecordingLevel.DEBUG);
        this.putIfAbsentTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "put-if-absent", Sensor.RecordingLevel.DEBUG);
        this.getTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "get", Sensor.RecordingLevel.DEBUG);
        this.deleteTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "delete", Sensor.RecordingLevel.DEBUG);
        this.putAllTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "put-all", Sensor.RecordingLevel.DEBUG);
        this.allTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "all", Sensor.RecordingLevel.DEBUG);
        this.rangeTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "range", Sensor.RecordingLevel.DEBUG);
        this.flushTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "flush", Sensor.RecordingLevel.DEBUG);
        this.restoreTime = this.metrics.addLatencyAndThroughputSensor(metricScope, name, "restore", Sensor.RecordingLevel.DEBUG);

        // register and possibly restore the state from the logs
        metrics.measureLatencyNs(time, initDelegate, this.restoreTime);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    @Override
    public V get(K key) {
        this.key = key;
        metrics.measureLatencyNs(time, getDelegate, this.getTime);
        return value;
    }

    @Override
    public void put(K key, V value) {
        this.key = key;
        this.value = value;
        metrics.measureLatencyNs(time, putDelegate, this.putTime);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        this.key = key;
        this.value = value;
        metrics.measureLatencyNs(time, putIfAbsentDelegate, this.putIfAbsentTime);
        return this.value;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        this.entries = entries;
        metrics.measureLatencyNs(time, putAllDelegate, this.putAllTime);
    }

    @Override
    public V delete(K key) {
        this.key = key;
        metrics.measureLatencyNs(time, deleteDelegate, this.deleteTime);
        return value;
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
        metrics.measureLatencyNs(time, flushDelegate, this.flushTime);
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
