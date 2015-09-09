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

package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RestoreFunc;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MeteredKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected final KeyValueStore<K, V> inner;

    private final Time time;
    private final String group;
    private final Sensor putTime;
    private final Sensor getTime;
    private final Sensor deleteTime;
    private final Sensor putAllTime;
    private final Sensor allTime;
    private final Sensor rangeTime;
    private final Sensor flushTime;
    private final Sensor restoreTime;
    private final Metrics metrics;

    private final String topic;
    private final int partition;
    private final Set<K> dirty;
    private final int maxDirty;
    private final ProcessorContext context;

    // always wrap the logged store with the metered store
    public MeteredKeyValueStore(final String name, final KeyValueStore<K, V> inner, ProcessorContext context, String group, Time time) {
        this.inner = inner;

        this.time = time;
        this.group = group;
        this.metrics = context.metrics();
        this.putTime = createSensor(name, "put");
        this.getTime = createSensor(name, "get");
        this.deleteTime = createSensor(name, "delete");
        this.putAllTime = createSensor(name, "put-all");
        this.allTime = createSensor(name, "all");
        this.rangeTime = createSensor(name, "range");
        this.flushTime = createSensor(name, "flush");
        this.restoreTime = createSensor(name, "restore");

        this.topic = name;
        this.partition = context.id();

        this.context = context;

        this.dirty = new HashSet<K>();
        this.maxDirty = 100;        // TODO: this needs to be configurable

        // register and possibly restore the state from the logs
        long startNs = time.nanoseconds();
        try {
            final Deserializer<K> keyDeserializer = (Deserializer<K>) context.keyDeserializer();
            final Deserializer<V> valDeserializer = (Deserializer<V>) context.valueDeserializer();

            context.register(this, new RestoreFunc() {
                @Override
                public void apply(byte[] key, byte[] value) {
                    inner.put(keyDeserializer.deserialize(topic, key),
                        valDeserializer.deserialize(topic, value));
                }
            });
        } finally {
            recordLatency(this.restoreTime, startNs, time.nanoseconds());
        }
    }

    private Sensor createSensor(String storeName, String operation) {
        Sensor parent = metrics.sensor(operation);
        addLatencyMetrics(parent, operation);
        Sensor sensor = metrics.sensor(storeName + "- " + operation, parent);
        addLatencyMetrics(sensor, operation, "store-name", storeName);
        return sensor;
    }

    private void addLatencyMetrics(Sensor sensor, String opName, String... kvs) {
        maybeAddMetric(sensor, new MetricName(opName + "-avg-latency-ms", group, "The average latency in milliseconds of the key-value store operation.", kvs), new Avg());
        maybeAddMetric(sensor, new MetricName(opName + "-max-latency-ms", group, "The max latency in milliseconds of the key-value store operation.", kvs), new Max());
        maybeAddMetric(sensor, new MetricName(opName + "-qps", group, "The average number of occurance of the given key-value store operation per second.", kvs), new Rate(new Count()));
    }

    private void maybeAddMetric(Sensor sensor, MetricName name, MeasurableStat stat) {
        if (!metrics.metrics().containsKey(name))
            sensor.add(name, stat);
    }

    @Override
    public String name() {
        return inner.name();
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
            recordLatency(this.getTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void put(K key, V value) {
        long startNs = time.nanoseconds();
        try {
            this.inner.put(key, value);

            this.dirty.add(key);
            if (this.dirty.size() > this.maxDirty)
                logChange();
        } finally {
            recordLatency(this.putTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        long startNs = time.nanoseconds();
        try {
            this.inner.putAll(entries);

            for (Entry<K, V> entry : entries) {
                this.dirty.add(entry.key());
            }

            if (this.dirty.size() > this.maxDirty)
                logChange();
        } finally {
            recordLatency(this.putAllTime, startNs, time.nanoseconds());
        }
    }

    @Override
    public void delete(K key) {
        long startNs = time.nanoseconds();
        try {
            this.inner.delete(key);

            this.dirty.add(key);
            if (this.dirty.size() > this.maxDirty)
                logChange();
        } finally {
            recordLatency(this.deleteTime, startNs, time.nanoseconds());
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
            logChange();
        } finally {
            recordLatency(this.flushTime, startNs, time.nanoseconds());
        }
    }

    private void logChange() {
        RecordCollector collector = ((ProcessorContextImpl) context).recordCollector();
        Serializer<K> keySerializer = (Serializer<K>) context.keySerializer();
        Serializer<V> valueSerializer = (Serializer<V>) context.valueSerializer();

        if (collector != null) {
            for (K k : this.dirty) {
                V v = this.inner.get(k);
                collector.send(new ProducerRecord<>(this.topic, this.partition, k, v), keySerializer, valueSerializer);
            }
            this.dirty.clear();
        }
    }

    private void recordLatency(Sensor sensor, long startNs, long endNs) {
        sensor.record((endNs - startNs) / 1000000, endNs);
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
                recordLatency(this.sensor, this.startNs, time.nanoseconds());
            }
        }

    }

}
