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
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTracker;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTrackerSupplier;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

abstract class KStreamKStreamJoin<K, VLeft, VRight, VOut, VThis, VOther> implements ProcessorSupplier<K, VThis, K, VOut> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private final StoreFactory otherWindowStoreFactory;
    private final long joinBeforeMs;
    private final long joinAfterMs;
    private final long joinGraceMs;
    private final boolean enableSpuriousResultFix;
    private final long windowsBeforeMs;
    private final long windowsAfterMs;

    private final boolean outer;
    private final Optional<StoreFactory> outerJoinWindowStoreFactory;
    private final ValueJoinerWithKey<? super K, ? super VThis, ? super VOther, ? extends VOut> joiner;

    private final TimeTrackerSupplier sharedTimeTrackerSupplier;

    KStreamKStreamJoin(final JoinWindowsInternal windows,
                       final ValueJoinerWithKey<? super K, ? super VThis, ? super VOther, ? extends VOut> joiner,
                       final boolean outer,
                       final long joinBeforeMs,
                       final long joinAfterMs,
                       final TimeTrackerSupplier sharedTimeTrackerSupplier,
                       final StoreFactory otherWindowStoreFactory,
                       final Optional<StoreFactory> outerJoinWindowStoreFactory) {
        this.otherWindowStoreFactory = otherWindowStoreFactory;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.windowsAfterMs = windows.afterMs;
        this.windowsBeforeMs = windows.beforeMs;
        this.joinGraceMs = windows.gracePeriodMs();
        this.enableSpuriousResultFix = windows.spuriousResultFixEnabled();
        this.joiner = joiner;
        this.outer = outer;
        this.outerJoinWindowStoreFactory = outerJoinWindowStoreFactory;
        this.sharedTimeTrackerSupplier = sharedTimeTrackerSupplier;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        // use ordered set for deterministic topology string in tests
        final Set<StoreBuilder<?>> stores = new LinkedHashSet<>();
        stores.add(new FactoryWrappingStoreBuilder<>(otherWindowStoreFactory));

        if (outerJoinWindowStoreFactory.isPresent() && enableSpuriousResultFix) {
            stores.add(new FactoryWrappingStoreBuilder<>(outerJoinWindowStoreFactory.get()));
        }
        return stores;
    }

    protected abstract class KStreamKStreamJoinProcessor extends ContextualProcessor<K, VThis, K, VOut> {
        private WindowStore<K, VOther> otherWindowStore;
        private Sensor droppedRecordsSensor;
        private Optional<KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>>> outerJoinStore = Optional.empty();
        private InternalProcessorContext<K, VOut> internalProcessorContext;
        private TimeTracker sharedTimeTracker;

        @Override
        public void init(final ProcessorContext<K, VOut> context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext<K, VOut>) context;

            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
            otherWindowStore = context.getStateStore(otherWindowStoreFactory.storeName());
            sharedTimeTracker = sharedTimeTrackerSupplier.get(context.taskId());

            if (enableSpuriousResultFix) {
                outerJoinStore = outerJoinWindowStoreFactory.map(s -> context.getStateStore(s.storeName()));

                sharedTimeTracker.setEmitInterval(
                    StreamsConfig.InternalConfig.getLong(
                        context.appConfigs(),
                        EMIT_INTERVAL_MS_KSTREAMS_OUTER_JOIN_SPURIOUS_RESULTS_FIX,
                        1000L
                    )
                );
            }
        }

        @Override
        public void process(final Record<K, VThis> record) {

            final long inputRecordTimestamp = record.timestamp();

            sharedTimeTracker.advanceStreamTime(inputRecordTimestamp);

            if (outer && record.key() == null && record.value() != null) {
                context().forward(record.withValue(joiner.apply(record.key(), record.value(), null)));
                return;
            } else if (StreamStreamJoinUtil.skipRecord(record, LOG, droppedRecordsSensor, context())) {
                return;
            }

            // Emit all non-joined records which window has closed
            if (inputRecordTimestamp == sharedTimeTracker.streamTime) {
                outerJoinStore.ifPresent(store -> emitNonJoinedOuterRecords(store, record));
            }

            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);
            try (final WindowStoreIterator<VOther> iter = otherWindowStore.fetch(record.key(), timeFrom, timeTo)) {
                final boolean needOuterJoin = outer && !iter.hasNext();
                iter.forEachRemaining(otherRecord -> emitInnerJoin(record, otherRecord, inputRecordTimestamp));

                if (needOuterJoin) {
                    // The maxStreamTime contains the max time observed in both sides of the join.
                    // Having access to the time observed in the other join side fixes the following
                    // problem:
                    //
                    // Say we have a window size of 5 seconds
                    //  1. A non-joined record with time T10 is seen in the left-topic (maxLeftStreamTime: 10)
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
                    if (!outerJoinStore.isPresent() || timeTo < sharedTimeTracker.streamTime) {
                        context().forward(record.withValue(joiner.apply(record.key(), record.value(), null)));
                    } else {
                        sharedTimeTracker.updatedMinTime(inputRecordTimestamp);
                        putInOuterJoinStore(record);
                    }
                }
            }
        }

        protected abstract TimestampedKeyAndJoinSide<K> makeThisKey(final K key, final long inputRecordTimestamp);

        protected abstract LeftOrRightValue<VLeft, VRight> makeThisValue(final VThis thisValue);

        protected abstract TimestampedKeyAndJoinSide<K> makeOtherKey(final K key, final long timestamp);

        protected abstract VThis thisValue(final LeftOrRightValue<? extends VLeft, ? extends VRight> leftOrRightValue);

        protected abstract VOther otherValue(final LeftOrRightValue<? extends VLeft, ? extends VRight> leftOrRightValue);

        private void emitNonJoinedOuterRecords(final KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>> store,
                                               final Record<K, VThis> record) {

            // calling `store.all()` creates an iterator what is an expensive operation on RocksDB;
            // to reduce runtime cost, we try to avoid paying those cost

            // only try to emit left/outer join results if there _might_ be any result records
            if (sharedTimeTracker.minTime + joinAfterMs + joinGraceMs >= sharedTimeTracker.streamTime) {
                return;
            }
            // throttle the emit frequency to a (configurable) interval;
            // we use processing time to decouple from data properties,
            // as throttling is a non-functional performance optimization
            if (internalProcessorContext.currentSystemTimeMs() < sharedTimeTracker.nextTimeToEmit) {
                return;
            }

            // Ensure `nextTimeToEmit` is synced with `currentSystemTimeMs`, if we dont set it everytime,
            // they can get out of sync during a clock drift
            sharedTimeTracker.nextTimeToEmit = internalProcessorContext.currentSystemTimeMs();
            sharedTimeTracker.advanceNextTimeToEmit();

            // reset to MAX_VALUE in case the store is empty
            sharedTimeTracker.minTime = Long.MAX_VALUE;

            try (final KeyValueIterator<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>> it = store.all()) {
                TimestampedKeyAndJoinSide<K> prevKey = null;

                boolean outerJoinLeftWindowOpen = false;
                boolean outerJoinRightWindowOpen = false;
                while (it.hasNext()) {
                    final KeyValue<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>> nextKeyValue = it.next();
                    final TimestampedKeyAndJoinSide<K> timestampedKeyAndJoinSide = nextKeyValue.key;
                    sharedTimeTracker.minTime = timestampedKeyAndJoinSide.timestamp();
                    if (outerJoinLeftWindowOpen && outerJoinRightWindowOpen) {
                        // if windows are open for both joinSides we can break since there are no more candidates to emit
                        break;
                    }

                    // Continue with the next outer record if window for this joinSide has not closed yet
                    // There might be an outer record for the other joinSide which window has not closed yet
                    // We rely on the <timestamp><left/right-boolean><key> ordering of KeyValueIterator
                    if (isOuterJoinWindowOpen(timestampedKeyAndJoinSide)) {
                        if (timestampedKeyAndJoinSide.isLeftSide()) {
                            outerJoinLeftWindowOpen = true; // there are no more candidates to emit on left-outerJoin-side
                        } else {
                            outerJoinRightWindowOpen = true; // there are no more candidates to emit on right-outerJoin-side
                        }
                        // We continue with the next outer record
                        continue;
                    }

                    final LeftOrRightValue<VLeft, VRight> leftOrRightValue = nextKeyValue.value;
                    forwardNonJoinedOuterRecords(record, timestampedKeyAndJoinSide, leftOrRightValue);

                    if (prevKey != null && !prevKey.equals(timestampedKeyAndJoinSide)) {
                        // blind-delete the previous key from the outer window store now it is emitted;
                        // we do this because this delete would remove the whole list of values of the same key,
                        // and hence if we delete eagerly and then fail, we would miss emitting join results of the later
                        // values in the list.
                        // we do not use delete() calls since it would incur extra get()
                        store.put(prevKey, null);
                    }
                    prevKey = timestampedKeyAndJoinSide;
                }

                // at the end of the iteration, we need to delete the last key
                if (prevKey != null) {
                    store.put(prevKey, null);
                }
            }
        }

        private void forwardNonJoinedOuterRecords(final Record<K, VThis> record,
                                                  final TimestampedKeyAndJoinSide<K> timestampedKeyAndJoinSide,
                                                  final LeftOrRightValue<VLeft, VRight> leftOrRightValue) {
            final K key = timestampedKeyAndJoinSide.key();
            final long timestamp = timestampedKeyAndJoinSide.timestamp();
            final VThis thisValue = thisValue(leftOrRightValue);
            final VOther otherValue = otherValue(leftOrRightValue);
            final VOut nullJoinedValue = joiner.apply(key, thisValue, otherValue);
            context().forward(
                    record.withKey(key).withValue(nullJoinedValue).withTimestamp(timestamp)
            );
        }

        private boolean isOuterJoinWindowOpen(final TimestampedKeyAndJoinSide<K> timestampedKeyAndJoinSide) {
            final long outerJoinLookBackTimeMs = getOuterJoinLookBackTimeMs(timestampedKeyAndJoinSide);
            return sharedTimeTracker.minTime + outerJoinLookBackTimeMs + joinGraceMs >= sharedTimeTracker.streamTime;
        }

        private long getOuterJoinLookBackTimeMs(
            final TimestampedKeyAndJoinSide<K> timestampedKeyAndJoinSide) {
            // depending on the JoinSide we fill in the outerJoinLookBackTimeMs
            if (timestampedKeyAndJoinSide.isLeftSide()) {
                return windowsAfterMs; // On the left-JoinSide we look back in time
            } else {
                return windowsBeforeMs; // On the right-JoinSide we look forward in time
            }
        }

        private void emitInnerJoin(final Record<K, VThis> thisRecord, final KeyValue<Long, VOther> otherRecord,
                                   final long inputRecordTimestamp) {
            outerJoinStore.ifPresent(store -> {
                // use putIfAbsent to first read and see if there's any values for the key,
                // if yes delete the key, otherwise do not issue a put;
                // we may delete some values with the same key early but since we are going
                // range over all values of the same key even after failure, since the other window-store
                // is only cleaned up by stream time, so this is okay for at-least-once.
                final TimestampedKeyAndJoinSide<K> otherKey = makeOtherKey(thisRecord.key(), otherRecord.key);
                store.putIfAbsent(otherKey, null);
            });

            context().forward(
                    thisRecord.withValue(joiner.apply(thisRecord.key(), thisRecord.value(), otherRecord.value))
                            .withTimestamp(Math.max(inputRecordTimestamp, otherRecord.key)));
        }

        private void putInOuterJoinStore(final Record<K, VThis> thisRecord) {
            outerJoinStore.ifPresent(store -> {
                final TimestampedKeyAndJoinSide<K> thisKey = makeThisKey(thisRecord.key(), thisRecord.timestamp());
                final LeftOrRightValue<VLeft, VRight> thisValue = makeThisValue(thisRecord.value());
                store.put(thisKey, thisValue);
            });
        }

        @Override
        public void close() {
            sharedTimeTrackerSupplier.remove(context().taskId());
        }
    }
}
