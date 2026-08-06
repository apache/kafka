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
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * Shared metering lifecycle for the metered iterators of the {@code Metered*WithHeaders} stores,
 * whatever result type they yield: the {@code KeyValueIterator}s returned by the store's own range/
 * fetch/find methods and the {@code ReadOnlyRecordIterator}s that back the headers-aware IQv2
 * range/window/session query types.
 *
 * <p>Every such iterator opens over a raw {@code KeyValueIterator<RawKey, byte[]>} and needs the
 * same bookkeeping: stamp the open time (for the {@code oldest-iterator-open-since-ms} metric),
 * register in {@code numOpenIterators}/{@code openIterators}, and on {@link #close()} record the
 * operation and iterator-duration sensors and deregister. This base is deliberately result-type
 * agnostic -- it implements only {@link MeteredIterator} and does not bind the yielded key/value
 * types -- so each subclass declares its own result interface (a {@code KeyValueIterator} or a
 * {@code ReadOnlyRecordIterator}) and implements just the parts that genuinely differ: the
 * deserializing {@code next()} (and, for the {@code KeyValueIterator}s, a peeking {@code hasNext()}
 * and {@code peekNextKey()}).
 *
 * @param <RawKey> the raw iterator's key type
 */
abstract class AbstractMeteredIterator<RawKey> implements MeteredIterator {

    final KeyValueIterator<RawKey, byte[]> iter;
    private final Sensor operationSensor;
    private final Sensor iteratorSensor;
    private final Time time;
    private final LongAdder numOpenIterators;
    private final Set<MeteredIterator> openIterators;
    private final long startNs;
    private final long startTimestampMs;

    AbstractMeteredIterator(final KeyValueIterator<RawKey, byte[]> iter,
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

    // Final: the constructor's openIterators.add(this) sorts through this via the set's
    // startTimestamp comparator, i.e. on a not-yet-fully-constructed object. Keeping it final stops a
    // subclass from overriding it with something that reads its own not-yet-assigned state.
    @Override
    public final long startTimestamp() {
        return startTimestampMs;
    }

    /**
     * Delegates to the raw iterator. Subclasses that buffer a peeked element (the
     * {@code KeyValueIterator}s) override this to also account for the buffered element.
     */
    public boolean hasNext() {
        return iter.hasNext();
    }

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
