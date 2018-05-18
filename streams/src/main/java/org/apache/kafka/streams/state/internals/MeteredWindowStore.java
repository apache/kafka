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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class MeteredWindowStore<K, V> extends WrappedStateStore.AbstractStateStore implements WindowStore<K, V> {

    private final WindowStore<Bytes, byte[]> inner;
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

    MeteredWindowStore(final WindowStore<Bytes, byte[]> inner,
                       final String metricScope,
                       final Time time,
                       final Serde<K> keySerde,
                       final Serde<V> valueSerde) {
        super(inner);
        this.inner = inner;
        this.metricScope = metricScope;
        this.time = time;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;
        this.serdes = new StateSerdes<>(ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
        final String tagKey = "task-id";
        final String taskName = context.taskId().toString();
        this.metrics = (StreamsMetricsImpl) context.metrics();
        this.putTime = this.metrics.addLatencyAndThroughputSensor(taskName, metricScope, name(), "put",
                                                                  Sensor.RecordingLevel.DEBUG, tagKey, taskName);
        this.fetchTime = this.metrics.addLatencyAndThroughputSensor(taskName, metricScope, name(), "fetch",
                                                                    Sensor.RecordingLevel.DEBUG, tagKey, taskName);
        this.flushTime = this.metrics.addLatencyAndThroughputSensor(taskName, metricScope, name(), "flush",
                                                                    Sensor.RecordingLevel.DEBUG, tagKey, taskName);
        final Sensor restoreTime = this.metrics.addLatencyAndThroughputSensor(taskName, metricScope, name(), "restore",
                                                                              Sensor.RecordingLevel.DEBUG, tagKey, taskName);
        // register and possibly restore the state from the logs
        final long startNs = time.nanoseconds();
        try {
            inner.init(context, root);
        } finally {
            this.metrics.recordLatency(restoreTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void put(final K key, final V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final K key, final V value, final long timestamp) {
        final long startNs = time.nanoseconds();
        try {
            inner.put(keyBytes(key), serdes.rawValue(value), timestamp);
        } finally {
            metrics.recordLatency(this.putTime, startNs, time.nanoseconds());
        }
    }

    private Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    @Override
    public V fetch(final K key, final long timestamp) {
        final long startNs = time.nanoseconds();
        try {
            final byte[] result = inner.fetch(keyBytes(key), timestamp);
            if (result == null) {
                return null;
            }
            return serdes.valueFrom(result);
        } finally {
            metrics.recordLatency(this.fetchTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        return new MeteredWindowStoreIterator<>(inner.fetch(keyBytes(key), timeFrom, timeTo),
                                                fetchTime,
                                                metrics,
                                                serdes,
                                                time);
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        return new MeteredWindowedKeyValueIterator<>(inner.all(), fetchTime, metrics, serdes, time);
    }
    
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom, final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(inner.fetchAll(timeFrom, timeTo), 
                                                     fetchTime, 
                                                     metrics, 
                                                     serdes, 
                                                     time);
    }
    
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final long timeFrom, final long timeTo) {
        return new MeteredWindowedKeyValueIterator<>(inner.fetch(keyBytes(from), keyBytes(to), timeFrom, timeTo),
                                                     fetchTime,
                                                     metrics,
                                                     serdes,
                                                     time);
    }

    @Override
    public void flush() {
        final long startNs = time.nanoseconds();
        try {
            inner.flush();
        } finally {
            metrics.recordLatency(flushTime, startNs, time.nanoseconds());
        }
    }

}
