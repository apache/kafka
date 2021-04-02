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
import org.apache.kafka.streams.StreamsConfig;
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
import org.apache.kafka.streams.state.internals.ValueOrOtherValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong maxObservedStreamTime;
    private final boolean thisJoin;

    KStreamKStreamJoin(final boolean thisJoin,
                       final String otherWindowName,
                       final long joinBeforeMs,
                       final long joinAfterMs,
                       final long joinGraceMs,
                       final ValueJoinerWithKey<? super K, ? super V1, ? super V2, ? extends R> joiner,
                       final boolean outer,
                       final Optional<String> outerJoinWindowName,
                       final AtomicLong maxObservedStreamTime) {
        this.thisJoin = thisJoin;
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
        private static final boolean DISABLE_OUTER_JOIN_SPURIOUS_RESULTS_FIX_DEFAULT = false;

        private WindowStore<K, V2> otherWindow;
        private StreamsMetricsImpl metrics;
        private Sensor droppedRecordsSensor;
        private Optional<WindowStore<KeyAndJoinSide<K>, ValueOrOtherValue>> outerJoinWindowStore = Optional.empty();
        private boolean internalOuterJoinFixDisabled;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindow = context.getStateStore(otherWindowName);

            internalOuterJoinFixDisabled = internalOuterJoinFixDisabled(context.appConfigs());
            if (!internalOuterJoinFixDisabled) {
                outerJoinWindowStore = outerJoinWindowName.map(name -> context.getStateStore(name));
            }
        }

        private boolean internalOuterJoinFixDisabled(final Map<String, Object> configs) {
            final Object value = configs.get(StreamsConfig.InternalConfig.INTERNAL_DISABLE_OUTER_JOIN_SPURIOUS_RESULTS_FIX);
            if (value == null) {
                return DISABLE_OUTER_JOIN_SPURIOUS_RESULTS_FIX_DEFAULT;
            }

            if (value instanceof Boolean) {
                return (Boolean) value;
            } else {
                return Boolean.valueOf((String) value);
            }
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

            // maxObservedStreamTime is updated and shared between left and right sides, so we can
            // process a non-join record immediately if it is late
            final long maxStreamTime = maxObservedStreamTime.updateAndGet(time -> Math.max(time, context().timestamp()));

            boolean needOuterJoin = outer;

            final long inputRecordTimestamp = context().timestamp();
            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

            try (final WindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, V2> otherRecord = iter.next();
                    final long otherRecordTimestamp = otherRecord.key;
                    context().forward(
                        key,
                        joiner.apply(key, value, otherRecord.value),
                        To.all().withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));

                    outerJoinWindowStore.ifPresent(store -> {
                        // Delete the other joined key from the outer non-joined store now to prevent
                        // further processing
                        final KeyAndJoinSide<K> otherJoinKey = KeyAndJoinSide.make(!thisJoin, key);
                        if (store.fetch(otherJoinKey, otherRecordTimestamp) != null) {
                            store.put(otherJoinKey, null, otherRecordTimestamp);
                        }
                    });
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
                    // the condition below allows us to process the late record without the need
                    // to hold it in the temporary outer store
                    if (internalOuterJoinFixDisabled || timeTo < maxStreamTime) {
                        context().forward(key, joiner.apply(key, value, null));
                    } else {
                        outerJoinWindowStore.ifPresent(store -> store.put(
                            KeyAndJoinSide.make(thisJoin, key),
                            makeValueOrOtherValue(thisJoin, value),
                            inputRecordTimestamp));
                    }
                }

                outerJoinWindowStore.ifPresent(store -> {
                    // only emit left/outer non-joined if the stream time has advanced (inputRecordTime = maxStreamTime)
                    // if the current record is late, then there is no need to check for expired records
                    if (inputRecordTimestamp == maxStreamTime) {
                        maybeEmitOuterExpiryRecords(store, maxStreamTime);
                    }
                });
            }
        }

        private ValueOrOtherValue makeValueOrOtherValue(final boolean thisJoin, final V1 value) {
            return thisJoin
                ? ValueOrOtherValue.makeValue(value)
                : ValueOrOtherValue.makeOtherValue(value);
        }

        @SuppressWarnings("unchecked")
        private void maybeEmitOuterExpiryRecords(final WindowStore<KeyAndJoinSide<K>, ValueOrOtherValue> store, final long maxStreamTime) {
            try (final KeyValueIterator<Windowed<KeyAndJoinSide<K>>, ValueOrOtherValue> it = store.all()) {
                while (it.hasNext()) {
                    final KeyValue<Windowed<KeyAndJoinSide<K>>, ValueOrOtherValue> e = it.next();

                    // Skip next records if the oldest record has not expired yet
                    if (e.key.window().end() + joinGraceMs >= maxStreamTime) {
                        break;
                    }

                    final K key = e.key.key().getKey();

                    // Emit the record by joining with a null value. But the order varies depending whether
                    // this join is using a reverse joiner or not. Also whether the returned record from the
                    // outer window store is a V1 or V2 value.
                    if (thisJoin) {
                        if (e.key.key().isThisJoin()) {
                            context().forward(key, joiner.apply(key, (V1) e.value.getThisValue(), null));
                        } else {
                            context().forward(key, joiner.apply(key, null, (V2) e.value.getOtherValue()));
                        }
                    } else {
                        if (e.key.key().isThisJoin()) {
                            context().forward(key, joiner.apply(key, null, (V2) e.value.getThisValue()));
                        } else {
                            context().forward(key, joiner.apply(key, (V1) e.value.getOtherValue(), null));
                        }
                    }

                    // Delete the key from tne outer window store now it is emitted
                    store.put(e.key.key(), null, e.key.window().start());
                }
            }
        }
    }
}
