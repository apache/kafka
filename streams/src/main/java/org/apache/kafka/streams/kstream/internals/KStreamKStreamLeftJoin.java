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
import org.apache.kafka.streams.state.internals.LeftOrRightValue;
import org.apache.kafka.streams.state.internals.TimestampedKeyAndJoinSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamLeftJoin<K, VL, VR, VOut> extends KStreamKStreamJoin<K, VL, VR, VOut, VL, VR> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamKStreamLeftJoin.class);

    KStreamKStreamLeftJoin(final String otherWindowName,
            final JoinWindowsInternal windows,
            final ValueJoinerWithKey<? super K, ? super VL, ? super VR, ? extends VOut> joiner,
            final boolean outer,
            final Optional<String> outerJoinWindowName,
            final TimeTrackerSupplier sharedTimeTrackerSupplier) {
        super(otherWindowName, sharedTimeTrackerSupplier, windows.spuriousResultFixEnabled(), outerJoinWindowName,
                windows.beforeMs, windows.afterMs, windows, outer, joiner, JoinSideFactory.makeLeft());
    }

    @Override
    public Processor<K, VL, K, VOut> get() {
        return new KStreamKStreamJoinLeftProcessor();
    }

    private class KStreamKStreamJoinLeftProcessor extends KStreamKStreamJoinProcessor {

        @Override
        public void process(final Record<K, VL> record) {
            sharedTimeTracker.advanceStreamTime(record.timestamp());
            if (outer && record.key() == null && record.value() != null) {
                context().forward(record.withValue(joiner.apply(record.key(), record.value(), null)));
                return;
            } else if (StreamStreamJoinUtil.skipRecord(record, LOG, droppedRecordsSensor, context())) {
                return;
            }
            final long inputRecordTimestamp = record.timestamp();

            // Emit all non-joined records which window has closed
            if (inputRecordTimestamp == sharedTimeTracker.streamTime) {
                outerJoinStore.ifPresent(store -> emitNonJoinedOuterRecords(store, record));
            }

            // TODO: Make these only if other store is present
            final TimestampedKeyAndJoinSide<K> thisKey =
                    joinSide.thisKey(record.key(), inputRecordTimestamp);
            final LeftOrRightValue<VL, VR> thisValue = joinSide.thisValue(record.value());

            performInnerJoin(record, inputRecordTimestamp, thisKey, thisValue);
        }

        private void emitNonJoinedOuterRecords(
                final KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VL, VR>> store,
                final Record<K, ?> record) {

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
                        // if windows are open for both joinSides we can break since there are no more candidates to
                        // emit
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
                    if (sharedTimeTracker.minTime + outerJoinLookBackTimeMs + joinGraceMs
                            >= sharedTimeTracker.streamTime) {
                        if (timestampedKeyAndJoinSide.isLeftSide()) {
                            outerJoinLeftWindowOpen =
                                    true; // there are no more candidates to emit on left-outerJoin-side
                        } else {
                            outerJoinRightWindowOpen =
                                    true; // there are no more candidates to emit on right-outerJoin-side
                        }
                        // We continue with the next outer record
                        continue;
                    }

                    final K key = timestampedKeyAndJoinSide.getKey();
                    final LeftOrRightValue<VL, VR> leftOrRightValue = next.value;
                    final VOut nullJoinedValue =
                            joiner.apply(key, leftOrRightValue.getLeftValue(), leftOrRightValue.getRightValue());
                    context().forward(
                            record.withKey(key).withValue(nullJoinedValue).withTimestamp(timestamp)
                    );

                    if (prevKey != null && !prevKey.equals(timestampedKeyAndJoinSide)) {
                        // blind-delete the previous key from the outer window store now it is emitted;
                        // we do this because this delete would remove the whole list of values of the same key,
                        // and hence if we delete eagerly and then fail, we would miss emitting join results of the
                        // later
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
    }
}
