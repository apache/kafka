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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

class MeteredWindowedKeyValueIterator<K, V> implements KeyValueIterator<Windowed<K>, V>, MeteredIterator {

    private final KeyValueIterator<Windowed<Bytes>, byte[]> iter;
    private final Sensor operationSensor;
    private final Sensor iteratorSensor;
    private final StreamsMetrics metrics;
    private final Function<byte[], K> deserializeKey;
    private final Function<byte[], V> deserializeValue;
    private final long startNs;
    private final long startTimestampMs;
    private final Time time;
    private final LongAdder numOpenIterators;
    private final Set<MeteredIterator> openIterators;

    MeteredWindowedKeyValueIterator(final KeyValueIterator<Windowed<Bytes>, byte[]> iter,
                                    final Sensor operationSensor,
                                    final Sensor iteratorSensor,
                                    final StreamsMetrics metrics,
                                    final Function<byte[], K> deserializeKey,
                                    final Function<byte[], V> deserializeValue,
                                    final Time time,
                                    final LongAdder numOpenIterators,
                                    final Set<MeteredIterator> openIterators) {
        this.iter = iter;
        this.operationSensor = operationSensor;
        this.iteratorSensor = iteratorSensor;
        this.metrics = metrics;
        this.deserializeKey = deserializeKey;
        this.deserializeValue = deserializeValue;
        this.startNs = time.nanoseconds();
        this.startTimestampMs = time.milliseconds();
        this.time = time;
        this.numOpenIterators = numOpenIterators;
        this.openIterators = openIterators;
        numOpenIterators.increment();
        openIterators.add(this);
    }

    @Override
    public long startTimestamp() {
        return this.startTimestampMs;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public KeyValue<Windowed<K>, V> next() {
        final KeyValue<Windowed<Bytes>, byte[]> next = iter.next();
        return KeyValue.pair(windowedKey(next.key), deserializeValue.apply(next.value));
    }

    private Windowed<K> windowedKey(final Windowed<Bytes> bytesKey) {
        final K key = deserializeKey.apply(bytesKey.key().get());
        return new Windowed<>(key, bytesKey.window());
    }

    @Override
    public void close() {
        try {
            iter.close();
        } finally {
            final long duration = time.nanoseconds() - startNs;
            operationSensor.record(duration);
            iteratorSensor.record(duration);
            numOpenIterators.decrement();
            openIterators.remove(this);
        }
    }

    @Override
    public Windowed<K> peekNextKey() {
        return windowedKey(iter.peekNextKey());
    }
}
