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
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyRecordIterator;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Shared metering lifecycle for the {@code Metered*WithHeaders} iterators that back the
 * headers-aware IQv2 range/window/session query types and yield {@link ReadOnlyRecord}s.
 *
 * <p>Every such iterator opens over a raw {@code KeyValueIterator<RawKey, byte[]>} and needs the
 * same bookkeeping: stamp the open time (for the {@code oldest-iterator-open-since-ms} metric),
 * register in {@code numOpenIterators}/{@code openIterators}, and on {@link #close()} record the
 * operation and iterator-duration sensors and deregister. Only {@link #next()} genuinely differs
 * per store -- raw key/value types, value deserialization, key derivation, timestamp source, and
 * whether a negative/absent timestamp is rejected -- so subclasses implement just that.
 *
 * @param <RawKey> the raw iterator's key type
 * @param <K>      the {@link ReadOnlyRecord} key type
 * @param <V>      the {@link ReadOnlyRecord} value type
 */
abstract class AbstractMeteredReadOnlyRecordIterator<RawKey, K, V>
    implements ReadOnlyRecordIterator<K, V>, MeteredIterator {

    final KeyValueIterator<RawKey, byte[]> iter;
    private final Sensor operationSensor;
    private final Sensor iteratorSensor;
    private final Time time;
    private final LongAdder numOpenIterators;
    private final Set<MeteredIterator> openIterators;
    private final long startNs;
    private final long startTimestampMs;

    AbstractMeteredReadOnlyRecordIterator(final KeyValueIterator<RawKey, byte[]> iter,
                                          final Sensor operationSensor,
                                          final Sensor iteratorSensor,
                                          final Time time,
                                          final LongAdder numOpenIterators,
                                          final Set<MeteredIterator> openIterators) {
        this.iter = iter;
        this.operationSensor = operationSensor;
        this.iteratorSensor = iteratorSensor;
        this.time = time;
        this.numOpenIterators = numOpenIterators;
        this.openIterators = openIterators;
        this.startNs = time.nanoseconds();
        this.startTimestampMs = time.milliseconds();
        numOpenIterators.increment();
        openIterators.add(this);
    }

    @Override
    public long startTimestamp() {
        return startTimestampMs;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
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
}
