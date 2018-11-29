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
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;

class MeteredWindowStoreIterator<V> implements WindowStoreIterator<V> {

    private final WindowStoreIterator<byte[]> iter;
    private final Sensor sensor;
    private final StreamsMetrics metrics;
    private final StateSerdes<?, V> serdes;
    private final long startNs;
    private final Time time;

    MeteredWindowStoreIterator(final WindowStoreIterator<byte[]> iter,
                               final Sensor sensor,
                               final StreamsMetrics metrics,
                               final StateSerdes<?, V> serdes,
                               final Time time) {
        this.iter = iter;
        this.sensor = sensor;
        this.metrics = metrics;
        this.serdes = serdes;
        this.startNs = time.relativeNanoseconds();
        this.time = time;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public KeyValue<Long, V> next() {
        final KeyValue<Long, byte[]> next = iter.next();
        return KeyValue.pair(next.key, serdes.valueFrom(next.value));
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
            metrics.recordLatency(this.sensor, this.startNs, time.relativeNanoseconds());
        }
    }

    @Override
    public Long peekNextKey() {
        return iter.peekNextKey();
    }
}
