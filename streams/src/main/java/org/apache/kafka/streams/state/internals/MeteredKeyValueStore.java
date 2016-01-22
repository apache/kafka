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

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.StreamingMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.Entry;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Serdes;

import java.util.List;

/**
 * Metered KeyValueStore wrapper is used for both recording operation metrics and Kafka based logging purposes, and hence its
 * inner KeyValueStore implementation do not need to provide its own logging functionality.
 *
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected final KeyValueStore<K, V> inner;
    protected final StoreChangeLogger.ValueGetter getter;
    protected final Serdes<K, V> serialization;
    protected final String metricScope;
    protected final Time time;

    private Sensor putTime;
    private Sensor getTime;
    private Sensor deleteTime;
    private Sensor putAllTime;
    private Sensor allTime;
    private Sensor rangeTime;
    private Sensor flushTime;
    private Sensor restoreTime;
    private StreamingMetrics metrics;

    private boolean loggingEnabled = true;
    private StoreChangeLogger<K, V> changeLogger = null;

    // always wrap the store with the metered store
    public MeteredKeyValueStore(final KeyValueStore<K, V> inner, Serdes<K, V> serialization, String metricScope, Time time) {
        this.inner = inner;
        this.getter = new StoreChangeLogger.ValueGetter<K, V>() {
            public V get(K key) {
                return inner.get(key);
            }
        };
        this.serialization = serialization;
        this.metricScope = metricScope;
        this.time = time != null ? time : new SystemTime();
    }

    public MeteredKeyValueStore<K, V> disableLogging() {
        loggingEnabled = false;
        return this;
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(ProcessorContext context) {
        final String name = name();
        this.metrics = context.metrics();
        this.putTime = this.metrics.addLatencySensor(metricScope, name, "put");
        this.getTime = this.metrics.addLatencySensor(metricScope, name, "get");
        this.deleteTime = this.metrics.addLatencySensor(metricScope, name, "delete");
        this.putAllTime = this.metrics.addLatencySensor(metricScope, name, "put-all");
        this.allTime = this.metrics.addLatencySensor(metricScope, name, "all");
        this.rangeTime = this.metrics.addLatencySensor(metricScope, name, "range");
        this.flushTime = this.metrics.addLatencySensor(metricScope, name, "flush");
        this.restoreTime = this.metrics.addLatencySensor(metricScope, name, "restore");

        serialization.init(context);
        this.changeLogger = this.loggingEnabled ? new StoreChangeLogger<>(name, context, serialization) : null;

        // register and possibly restore the state from the logs
        long startNs = time.nanoseconds();
        inner.init(context);
        try {
            final Deserializer<K> keyDeserializer = serialization.keyDeserializer();
            final Deserializer<V> valDeserializer = serialization.valueDeserializer();

            context.register(this, loggingEnabled, new StateRestoreCallback() {
                @Override
                public void restore(byte[] key, byte[] value) {
                    inner.put(keyDeserializer.deserialize(name, key),
                            valDeserializer.deserialize(name, value));
                }
            });
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

            if (loggingEnabled) {
                changeLogger.add(key);
                changeLogger.maybeLogChange(this.getter);
            }
        } finally {
            this.metrics.recordLatency(this.putTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        long startNs = time.nanoseconds();
        try {
            this.inner.putAll(entries);

            if (loggingEnabled) {
                for (Entry<K, V> entry : entries) {
                    K key = entry.key();
                    changeLogger.add(key);
                }
                changeLogger.maybeLogChange(this.getter);
            }
        } finally {
            this.metrics.recordLatency(this.putAllTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public V delete(K key) {
        long startNs = time.nanoseconds();
        try {
            V value = this.inner.delete(key);

            removed(key);

            return value;
        } finally {
            this.metrics.recordLatency(this.deleteTime, startNs, time.nanoseconds());
        }
    }

    /**
     * Called when the underlying {@link #inner} {@link KeyValueStore} removes an entry in response to a call from this
     * store.
     *
     * @param key the key for the entry that the inner store removed
     */
    protected void removed(K key) {
        if (loggingEnabled) {
            changeLogger.delete(key);
            changeLogger.maybeLogChange(this.getter);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MeteredKeyValueIterator<K, V>(this.inner.range(from, to), this.rangeTime);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator<K, V>(this.inner.all(), this.allTime);
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

            if (loggingEnabled)
                changeLogger.logChange(this.getter);
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
        public Entry<K1, V1> next() {
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
