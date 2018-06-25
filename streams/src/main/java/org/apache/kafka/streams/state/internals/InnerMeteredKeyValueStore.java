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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
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
class InnerMeteredKeyValueStore<K, IK, V, IV> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<K, V> {

    private final KeyValueStore<IK, IV> inner;
    private final String metricScope;
    // convert types from outer store type to inner store type
    private final TypeConverter<K, IK, V, IV> typeConverter;
    protected final Time time;
    private Sensor putTime;
    private Sensor putIfAbsentTime;
    private Sensor getTime;
    private Sensor deleteTime;
    private Sensor putAllTime;
    private Sensor allTime;
    private Sensor rangeTime;
    private Sensor flushTime;
    private StreamsMetricsImpl metrics;
    private ProcessorContext context;
    private StateStore root;

    /**
     * For a period of time we will have 2 store hierarchies. 1 which is built by a
     * {@link org.apache.kafka.streams.state.StoreSupplier} where the outer most store will be of user defined
     * type, i.e, &lt;String,Integer&gt;, and another where the outermost store will be of type &lt;Bytes,byte[]&gt;
     * This interface is so we don't need to have 2 complete implementations for collecting the metrics, rather
     * we just provide an instance of this to do the type conversions from the outer store types to the inner store types.
     * @param <K>  key type of the outer store
     * @param <IK> key type of the inner store
     * @param <V>  value type of the outer store
     * @param <IV> value type of the inner store
     */
    interface TypeConverter<K, IK, V, IV> {
        IK innerKey(final K key);
        IV innerValue(final V value);
        List<KeyValue<IK, IV>> innerEntries(final List<KeyValue<K, V>> from);
        V outerValue(final IV value);
        KeyValue<K, V> outerKeyValue(final KeyValue<IK, IV> from);
        K outerKey(final IK ik);
    }

    // always wrap the store with the metered store
    InnerMeteredKeyValueStore(final KeyValueStore<IK, IV> inner,
                              final String metricScope,
                              final TypeConverter<K, IK, V, IV> typeConverter,
                              final Time time) {
        super(inner);
        this.inner = inner;
        this.metricScope = metricScope;
        this.typeConverter = typeConverter;
        this.time = time != null ? time : Time.SYSTEM;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        final String name = name();
        final String tagKey = "task-id";
        final String taskName = context.taskId().toString();
        this.context = context;
        this.root = root;
        this.metrics = (StreamsMetricsImpl) context.metrics();
        this.putTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                  metricScope,
                                                                  name,
                                                                  "put",
                                                                  Sensor.RecordingLevel.DEBUG,
                                                                  tagKey, taskName);
        this.putIfAbsentTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                          metricScope,
                                                                          name,
                                                                          "put-if-absent",
                                                                          Sensor.RecordingLevel.DEBUG,
                                                                          tagKey, taskName);
        this.getTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                  metricScope,
                                                                  name,
                                                                  "get",
                                                                  Sensor.RecordingLevel.DEBUG,
                                                                  tagKey, taskName);
        this.deleteTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                     metricScope,
                                                                     name,
                                                                     "delete",
                                                                     Sensor.RecordingLevel.DEBUG,
                                                                     tagKey, taskName);
        this.putAllTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                     metricScope,
                                                                     name,
                                                                     "put-all",
                                                                     Sensor.RecordingLevel.DEBUG,
                                                                     tagKey, taskName);
        this.allTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                  metricScope,
                                                                  name,
                                                                  "all",
                                                                  Sensor.RecordingLevel.DEBUG,
                                                                  tagKey, taskName);
        this.rangeTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                    metricScope,
                                                                    name,
                                                                    "range",
                                                                    Sensor.RecordingLevel.DEBUG,
                                                                    tagKey, taskName);
        this.flushTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                    metricScope,
                                                                    name,
                                                                    "flush",
                                                                    Sensor.RecordingLevel.DEBUG,
                                                                    tagKey, taskName);
        final Sensor restoreTime = this.metrics.addLatencyAndThroughputSensor(taskName,
                                                                              metricScope,
                                                                              name,
                                                                              "restore",
                                                                              Sensor.RecordingLevel.DEBUG,
                                                                              tagKey, taskName);

        // register and possibly restore the state from the logs
        if (restoreTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.init(InnerMeteredKeyValueStore.this.context, InnerMeteredKeyValueStore.this.root);
                    return null;
                }
            }, restoreTime);
        } else {
            inner.init(InnerMeteredKeyValueStore.this.context, InnerMeteredKeyValueStore.this.root);
        }
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    private interface Action<V> {
        V execute();
    }

    @Override
    public V get(final K key) {
        try {
            if (getTime.shouldRecord()) {
                return measureLatency(new Action<V>() {
                    @Override
                    public V execute() {
                        return typeConverter.outerValue(inner.get(typeConverter.innerKey(key)));
                    }
                }, getTime);
            } else {
                return typeConverter.outerValue(inner.get(typeConverter.innerKey(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key, final V value) {
        try {
            if (putTime.shouldRecord()) {
                measureLatency(new Action<V>() {
                    @Override
                    public V execute() {
                        inner.put(typeConverter.innerKey(key), typeConverter.innerValue(value));
                        return null;
                    }
                }, putTime);
            } else {
                inner.put(typeConverter.innerKey(key), typeConverter.innerValue(value));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        if (putIfAbsentTime.shouldRecord()) {
            return measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    return typeConverter.outerValue(inner.putIfAbsent(typeConverter.innerKey(key), typeConverter.innerValue(value)));
                }
            }, putIfAbsentTime);
        } else {
            return typeConverter.outerValue(inner.putIfAbsent(typeConverter.innerKey(key), typeConverter.innerValue(value)));
        }

    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        if (putAllTime.shouldRecord()) {
            measureLatency(new Action<V>() {
                @Override
                public V execute() {
                    inner.putAll(typeConverter.innerEntries(entries));
                    return null;
                }
            }, putAllTime);
        } else {
            inner.putAll(typeConverter.innerEntries(entries));
        }
    }

    @Override
    public V delete(final K key) {
        try {
            if (deleteTime.shouldRecord()) {
                return measureLatency(new Action<V>() {
                    @Override
                    public V execute() {
                        return typeConverter.outerValue(inner.delete(typeConverter.innerKey(key)));
                    }
                }, deleteTime);
            } else {
                return typeConverter.outerValue(inner.delete(typeConverter.innerKey(key)));
            }
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return new MeteredKeyValueIterator(this.inner.range(typeConverter.innerKey(from), typeConverter.innerKey(to)), this.rangeTime);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new MeteredKeyValueIterator(this.inner.all(), this.allTime);
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

    private class MeteredKeyValueIterator implements KeyValueIterator<K, V> {

        private final KeyValueIterator<IK, IV> iter;
        private final Sensor sensor;
        private final long startNs;

        MeteredKeyValueIterator(KeyValueIterator<IK, IV> iter, Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            return typeConverter.outerKeyValue(iter.next());
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
        public K peekNextKey() {
            return typeConverter.outerKey(iter.peekNextKey());
        }
    }
}
