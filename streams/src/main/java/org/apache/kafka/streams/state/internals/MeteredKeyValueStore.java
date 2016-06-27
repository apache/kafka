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
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * Metered KeyValueStore wrapper is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 *
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected final KeyValueStore<K, V> inner;
    protected final String metricScope;
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
    private StreamsMetrics metrics;

    // always wrap the store with the metered store
    public MeteredKeyValueStore(final KeyValueStore<K, V> inner, String metricScope, Time time) {
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time != null ? time : new SystemTime();
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        final String name = name();
        this.metrics = context.metrics();
        this.putTime = this.metrics.addLatencySensor(metricScope, name, "put");
        this.putIfAbsentTime = this.metrics.addLatencySensor(metricScope, name, "put-if-absent");
        this.getTime = this.metrics.addLatencySensor(metricScope, name, "get");
        this.deleteTime = this.metrics.addLatencySensor(metricScope, name, "delete");
        this.putAllTime = this.metrics.addLatencySensor(metricScope, name, "put-all");
        this.allTime = this.metrics.addLatencySensor(metricScope, name, "all");
        this.rangeTime = this.metrics.addLatencySensor(metricScope, name, "range");
        this.flushTime = this.metrics.addLatencySensor(metricScope, name, "flush");
        this.restoreTime = this.metrics.addLatencySensor(metricScope, name, "restore");

        // register and possibly restore the state from the logs
        long startNs = time.nanoseconds();
        try {
            inner.init(context, root);
        } finally {
            this.metrics.recordLatency(this.restoreTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public V get(K key) {
        long startNs = time.nanoseconds();
        try {
            return this.inner.get(key);
        } finally {
            this.metrics.recordLatency(this.getTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void put(K key, V value) {
        long startNs = time.nanoseconds();
        try {
            this.inner.put(key, value);
        } finally {
            this.metrics.recordLatency(this.putTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        long startNs = time.nanoseconds();
        try {
            return this.inner.putIfAbsent(key, value);
        } finally {
            this.metrics.recordLatency(this.putIfAbsentTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        long startNs = time.nanoseconds();
        try {
            this.inner.putAll(entries);
        } finally {
            this.metrics.recordLatency(this.putAllTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public V delete(K key) {
        long startNs = time.nanoseconds();
        try {
            return this.inner.delete(key);
        } finally {
            this.metrics.recordLatency(this.deleteTime, startNs, time.nanoseconds());
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
    public void close() {
        inner.close();
    }

    @Override
    public void flush() {
        long startNs = time.nanoseconds();
        try {
            this.inner.flush();
        } finally {
            this.metrics.recordLatency(this.flushTime, startNs, time.nanoseconds());
        }
    }

    private class MeteredKeyValueIterator<K1, V1> implements KeyValueIterator<K1, V1> {

        private final KeyValueIterator<K1, V1> iter;
        private final Sensor sensor;
        private final long startNs;

        public MeteredKeyValueIterator(KeyValueIterator<K1, V1> iter, Sensor sensor) {
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
    }
}
