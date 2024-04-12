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

import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTrackerSupplier;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamLeftJoin<K, VL, VR, VOut> extends KStreamKStreamJoin<K, VL, VR, VOut, VL, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamLeftJoin.class);

    private final long joinBeforeMs;
    private final long joinAfterMs;
    private final long joinGraceMs;
    private final long windowsBeforeMs;
    private final long windowsAfterMs;

    private final boolean outer;
    private final ValueJoinerWithKey<? super K, ? super VL, ? super VR, ? extends VOut> joiner;


    KStreamKStreamLeftJoin(final String otherWindowName,
                       final JoinWindowsInternal windows,
                       final ValueJoinerWithKey<? super K, ? super VL, ? super VR, ? extends VOut> joiner,
                       final boolean outer,
                       final Optional<String> outerJoinWindowName,
                       final TimeTrackerSupplier sharedTimeTrackerSupplier) {
        super(otherWindowName, sharedTimeTrackerSupplier, windows.spuriousResultFixEnabled(), outerJoinWindowName);
        this.joinBeforeMs = windows.beforeMs;
        this.joinAfterMs = windows.afterMs;
        this.windowsAfterMs = windows.afterMs;
        this.windowsBeforeMs = windows.beforeMs;
        this.joinGraceMs = windows.gracePeriodMs();
        this.joiner = joiner;
        this.outer = outer;
    }

    @Override
    public Processor<K, VL, K, VOut> get() {
        return new KStreamKStreamJoinLeftProcessor();
    }

    private class KStreamKStreamJoinLeftProcessor extends KStreamKStreamJoinProcessor {

        @Override
        public void process(final Record<K, VL> record) {
            final long inputRecordTimestamp = record.timestamp();
            final long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
            final long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

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

            boolean needOuterJoin = outer;
            try (final WindowStoreIterator<VR> iter = otherWindowStore.fetch(record.key(), timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                    final KeyValue<Long, VR> rightRecord = iter.next();
                    final long otherRecordTimestamp = rightRecord.key;

                    outerJoinStore.ifPresent(store -> {
                        // use putIfAbsent to first read and see if there's any values for the key,
                        // if yes delete the key, otherwise do not issue a put;
                        // we may delete some values with the same key early but since we are going
                        // range over all values of the same key even after failure, since the other window-store
                        // is only cleaned up by stream time, so this is okay for at-least-once.
                        store.putIfAbsent(TimestampedKeyAndJoinSide.makeRight(record.key(), otherRecordTimestamp), null);
                    });

                    context().forward(
                        record.withValue(joiner.apply(record.key(), record.value(), rightRecord.value))
                               .withTimestamp(Math.max(inputRecordTimestamp, otherRecordTimestamp)));
                }

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
                        outerJoinStore.ifPresent(store -> store.put(
                            TimestampedKeyAndJoinSide.makeLeft(record.key(), inputRecordTimestamp),
                            LeftOrRightValue.makeLeft(record.value())));
                    }
                }
            }
        }

        private void emitNonJoinedOuterRecords(
            final KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VL, VR>> store,
            final Record<K, VL> record) {

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

            try (final KeyValueIterator<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VL, VR>> it = store.all()) {
                TimestampedKeyAndJoinSide<K> prevKey = null;

                boolean outerJoinLeftWindowOpen = false;
                boolean outerJoinRightWindowOpen = false;
                while (it.hasNext()) {
                    if (outerJoinLeftWindowOpen && outerJoinRightWindowOpen) {
                        // if windows are open for both joinSides we can break since there are no more candidates to emit
                        break;
                    }
                    final KeyValue<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VL, VR>> next = it.next();
                    final TimestampedKeyAndJoinSide<K> timestampedKeyAndJoinSide = next.key;
                    final long timestamp = timestampedKeyAndJoinSide.getTimestamp();
                    sharedTimeTracker.minTime = timestamp;

                    // Continue with the next outer record if window for this joinSide has not closed yet
                    // There might be an outer record for the other joinSide which window has not closed yet
                    // We rely on the <timestamp><left/right-boolean><key> ordering of KeyValueIterator
                    final long outerJoinLookBackTimeMs = getOuterJoinLookBackTimeMs(timestampedKeyAndJoinSide);
                    if (sharedTimeTracker.minTime + outerJoinLookBackTimeMs + joinGraceMs >= sharedTimeTracker.streamTime) {
                        if (timestampedKeyAndJoinSide.isLeftSide()) {
                            outerJoinLeftWindowOpen = true; // there are no more candidates to emit on left-outerJoin-side
                        } else {
                            outerJoinRightWindowOpen = true; // there are no more candidates to emit on right-outerJoin-side
                        }
                        // We continue with the next outer record
                        continue;
                    }
                    
                    final K key = timestampedKeyAndJoinSide.getKey();
                    final LeftOrRightValue<VL, VR> leftOrRightValue = next.value;
                    final VOut nullJoinedValue = getNullJoinedValue(key, leftOrRightValue);
                    context().forward(
                        record.withKey(key).withValue(nullJoinedValue).withTimestamp(timestamp)
                    );

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

        private VOut getNullJoinedValue(final K key, final LeftOrRightValue<VL, VR> leftOrRightValue) {
            return joiner.apply(key, leftOrRightValue.getLeftValue(), leftOrRightValue.getRightValue());
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

    }
}
