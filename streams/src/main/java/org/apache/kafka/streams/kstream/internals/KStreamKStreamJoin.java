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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.KeyAndJoinSide;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private final String otherWindowName;
    private final long joinBeforeMs;
    private final long joinAfterMs;
    private final long joinGraceMs;

    private final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends R> joiner;
    private final boolean outer;
    private final Optional<String> outerJoinWindowName;
    private final boolean isLeftSide;

    private final KStreamImplJoin.MaxObservedStreamTime maxObservedStreamTime;

    KStreamKStreamJoin(final boolean isLeftSide,
                       final String otherWindowName,
                       final long joinBeforeMs,
                       final long joinAfterMs,
                       final long joinGraceMs,
                       final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends R> joiner,
                       final boolean outer,
                       final Optional<String> outerJoinWindowName,
                       final KStreamImplJoin.MaxObservedStreamTime maxObservedStreamTime) {
        this.isLeftSide = isLeftSide;
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joinGraceMs = joinGraceMs;
        this.joiner = joiner;
        this.outer = outer;
        this.outerJoinWindowName = outerJoinWindowName;
        this.maxObservedStreamTime = maxObservedStreamTime;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor extends AbstractProcessor<K, V1> {
        private final Predicate<Windowed<KeyAndJoinSide<K>>> recordWindowHasClosed =
            windowedKey -> windowedKey.window().start() + joinAfterMs + joinGraceMs < maxObservedStreamTime.get();

        private WindowStore<K, V2> otherWindowStore;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;
        private Optional<WindowStore<KeyAndJoinSide<K>, LeftOrRightValue>> outerJoinWindowStore = Optional.empty();

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindowStore = context.getStateStore(otherWindowName);
            outerJoinWindowStore = outerJoinWindowName.map(name -> context.getStateStore(name));
        }

        @Override
        public void process(final K key, final V1 value) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            boolean needOuterJoin = outer;

            final long inputRecordTimestamp = context().timestamp();
            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

            maxObservedStreamTime.advance(inputRecordTimestamp);

            // Emit all non-joined records which window has closed
            if (inputRecordTimestamp == maxObservedStreamTime.get()) {
                outerJoinWindowStore.ifPresent(store -> emitNonJoinedOuterRecords(store));
            }

            try (final WindowStoreIterator<V2> iter = otherWindowStore.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    final long otherRecordTimestamp = otherRecord.key;

                    outerJoinWindowStore.ifPresent(store -> {
                        // Delete the joined record from the non-joined outer window store
                        store.put(KeyAndJoinSide.make(!isLeftSide, key), null, otherRecordTimestamp);
                    });

                    context().forward(
                        key,
                        joiner.apply(key, value, otherRecord.value),
                        To.all().withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));
                }

                if (needOuterJoin) {
                    // The maxStreamTime contains the max time observed in both sides of the join.
                    // Having access to the time observed in the other join side fixes the following
                    // problem:
                    //
                    // Say we have a window size of 5 seconds
                    //  1. A non-joined record wth time T10 is seen in the left-topic (maxLeftStreamTime: 10)
                    //     The record is not processed yet, and is added to the outer-join store
                    //  2. A non-joined record with time T2 is seen in the right-topic (maxRightStreamTime: 2)
                    //     The record is not processed yet, and is added to the outer-join store
                    //  3. A joined record with time T11 is seen in the left-topic (maxLeftStreamTime: 11)
                    //     It is time to look at the expired records. T10 and T2 should be emitted, but
                    //     because T2 was late, then it is not fetched by the window store, so it is not processed
                    //
                    // See KStreamKStreamLeftJoinTest.testLowerWindowBound() tests
                    //
                    // This condition below allows us to process the out-of-order records without the need
                    // to hold it in the temporary outer store
                    if (!outerJoinWindowStore.isPresent() || timeTo < maxObservedStreamTime.get()) {
                        context().forward(key, joiner.apply(key, value, null));
                    } else {
                        outerJoinWindowStore.ifPresent(store -> store.put(
                            KeyAndJoinSide.make(isLeftSide, key),
                            LeftOrRightValue.make(isLeftSide, value),
                            inputRecordTimestamp));
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void emitNonJoinedOuterRecords(final WindowStore<KeyAndJoinSide<K>, LeftOrRightValue> store) {
            try (final KeyValueIterator<Windowed<KeyAndJoinSide<K>>, LeftOrRightValue> it = store.all()) {
                while (it.hasNext()) {
                    final KeyValue<Windowed<KeyAndJoinSide<K>>, LeftOrRightValue> record = it.next();

                    final Windowed<KeyAndJoinSide<K>> windowedKey = record.key;
                    final LeftOrRightValue value = record.value;

                    // Skip next records if window has not closed
                    if (windowedKey.window().start() + joinAfterMs + joinGraceMs >= maxObservedStreamTime.get()) {
                        break;
                    }

                    final K key = windowedKey.key().getKey();
                    final long time = windowedKey.window().start();

                    final R nullJoinedValue;
                    if (isLeftSide) {
                        nullJoinedValue = joiner.apply(key,
                            (V1) value.getLeftValue(),
                            (V2) value.getRightValue());
                    } else {
                        nullJoinedValue = joiner.apply(key,
                            (V1) value.getRightValue(),
                            (V2) value.getLeftValue());
                    }

                    context().forward(key, nullJoinedValue, To.all().withTimestamp(time));

                    // Delete the key from the outer window store now it is emitted
                    store.put(record.key.key(), null, record.key.window().start());
                }
            }
        }
    }
}
