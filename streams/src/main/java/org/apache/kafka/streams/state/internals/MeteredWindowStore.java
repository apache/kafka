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
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class MeteredWindowStore<K, V> implements WindowStore<K, V> {

    protected final WindowStore<K, V> inner;
    protected final String metricScope;
    protected final Time time;

    private Sensor putTime;
    private Sensor fetchTime;
    private Sensor flushTime;
    private Sensor restoreTime;
    private StreamsMetricsImpl metrics;

    private ProcessorContext context;
    private StateStore root;
    private Runnable initDelegate = new Runnable() {
        @Override
        public void run() {
            inner.init(context, root);
        }
    };

    private K key;
    private V value;
    private long timestamp;
    private Runnable putDelegate = new Runnable() {
        @Override
        public void run() {
            inner.put(key, value);
        }
    };
    private Runnable putTsDelegate = new Runnable() {
        @Override
        public void run() {
            inner.put(key, value, timestamp);
        }
    };
    private Runnable flushDelegate = new Runnable() {
        @Override
        public void run() {
            inner.flush();
        }
    };

    // always wrap the store with the metered store
    public MeteredWindowStore(final WindowStore<K, V> inner, String metricScope, Time time) {
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time != null ? time : Time.SYSTEM;
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        final String name = name();
        this.context = context;
        this.root = root;
        this.metrics = (StreamsMetricsImpl) context.metrics();
        this.putTime = this.metrics.addLatencySensor(metricScope, name, "put", Sensor.RecordingLevel.DEBUG);
        this.fetchTime = this.metrics.addLatencySensor(metricScope, name, "fetch", Sensor.RecordingLevel.DEBUG);
        this.flushTime = this.metrics.addLatencySensor(metricScope, name, "flush", Sensor.RecordingLevel.DEBUG);
        this.restoreTime = this.metrics.addLatencySensor(metricScope, name, "restore", Sensor.RecordingLevel.DEBUG);

        // register and possibly restore the state from the logs
        metrics.measureLatencyNs(time, initDelegate, this.restoreTime);
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        return new MeteredWindowStoreIterator<>(this.inner.fetch(key, timeFrom, timeTo), this.fetchTime);
    }

    @Override
    public void put(K key, V value) {
        this.key = key;
        this.value = value;
        metrics.measureLatencyNs(time, putDelegate, this.putTime);
    }

    @Override
    public void put(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        metrics.measureLatencyNs(time, putTsDelegate, this.putTime);
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public void flush() {
        metrics.measureLatencyNs(time, flushDelegate, this.flushTime);
    }

    private class MeteredWindowStoreIterator<E> implements WindowStoreIterator<E> {

        private final WindowStoreIterator<E> iter;
        private final Sensor sensor;
        private final long startNs;

        public MeteredWindowStoreIterator(WindowStoreIterator<E> iter, Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Long, E> next() {
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

    WindowStore<K, V> inner() {
        return inner;
    }
}
