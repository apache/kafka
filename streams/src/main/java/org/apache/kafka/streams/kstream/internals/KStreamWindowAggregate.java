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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.processor.internals.StoreFactory.FactoryWrappingStoreBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamWindowAggregate<KIn, VIn, VAgg, W extends Window> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private static final Logger log = LoggerFactory.getLogger(KStreamWindowAggregate.class);

    private final String storeName;
    private final StoreFactory storeFactory;
    private final Windows<W> windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;
    private final EmitStrategy emitStrategy;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(final Windows<W> windows,
                                  final StoreFactory storeFactory,
                                  final EmitStrategy emitStrategy,
                                  final Initializer<VAgg> initializer,
                                  final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.windows = windows;
        this.storeName = storeFactory.name();
        this.storeFactory = storeFactory;
        this.emitStrategy = emitStrategy;
        this.initializer = initializer;
        this.aggregator = aggregator;

        if (emitStrategy.type() == StrategyType.ON_WINDOW_CLOSE) {
            if (!(windows instanceof TimeWindows)) {
                throw new IllegalArgumentException("ON_WINDOW_CLOSE strategy is only supported for "
                    + "TimeWindows and SlidingWindows for TimeWindowedKStream");
            }
        }
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(new FactoryWrappingStoreBuilder<>(storeFactory));
    }

    @Override
    public Processor<KIn, VIn, Windowed<KIn>, Change<VAgg>> get() {
        return new KStreamWindowAggregateProcessor(storeName, emitStrategy, sendOldValues);
    }

    public Windows<W> windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamWindowAggregateProcessor extends AbstractKStreamTimeWindowAggregateProcessor<KIn, VIn, VAgg> {
        protected KStreamWindowAggregateProcessor(final String storeName, final EmitStrategy emitStrategy, final boolean sendOldValues) {
            super(storeName, emitStrategy, sendOldValues);
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            if (record.key() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    log.warn(
                        "Skipping record due to null key. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    log.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return;
            }

            // first get the matching windows
            final long timestamp = record.timestamp();
            updateObservedStreamTime(timestamp);
            final long windowCloseTime = observedStreamTime - windows.gracePeriodMs();

            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window whose end time is still larger than the window close time,
            // and create the new window for the rest of unmatched window that do not exist yet;
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                final Long windowStart = entry.getKey();
                final long windowEnd = entry.getValue().end();
                if (windowEnd > windowCloseTime) {
                    final ValueAndTimestamp<VAgg> oldAggAndTimestamp = windowStore.fetch(record.key(), windowStart);
                    VAgg oldAgg = getValueOrNull(oldAggAndTimestamp);

                    final VAgg newAgg;
                    final long newTimestamp;

                    if (oldAgg == null) {
                        oldAgg = initializer.apply();
                        newTimestamp = record.timestamp();
                    } else {
                        newTimestamp = Math.max(record.timestamp(), oldAggAndTimestamp.timestamp());
                    }

                    newAgg = aggregator.apply(record.key(), record.value(), oldAgg);

                    // update the store with the new value
                    windowStore.put(record.key(), ValueAndTimestamp.make(newAgg, newTimestamp), windowStart);
                    maybeForwardUpdate(record, entry.getValue(), oldAgg, newAgg, newTimestamp);
                } else {
                    final String windowString = "[" + windowStart + "," + windowEnd + ")";
                    logSkippedRecordForExpiredWindow(log, record.timestamp(), windowCloseTime, windowString);
                }
            }

            maybeForwardFinalResult(record, windowCloseTime);
        }

        @Override
        protected long emitRangeLowerBound(final long windowCloseTime) {
            // Since time window end timestamp is exclusive, we set the inclusive lower bound plus 1;
            // Set lower bound to 0L for the first time emit so that when we fetchAll, we fetch from 0L
            return lastEmitWindowCloseTime == ConsumerRecord.NO_TIMESTAMP ?
                0L : Math.max(0L, lastEmitWindowCloseTime - windows.size()) + 1;
        }

        @Override
        protected long emitRangeUpperBound(final long windowCloseTime) {
            return windowCloseTime - windows.size();
        }

        @Override
        protected boolean shouldRangeFetch(final long emitRangeLowerBound, final long emitRangeUpperBound) {
            // Don't fetch store if the new emit window close time doesn't
            // progress enough to cover next window;
            // Note since window-end time is exclusive we would not count windows whose end time == lower bound, hence
            // would minus 1 when finding matched windows
            if (lastEmitWindowCloseTime != ConsumerRecord.NO_TIMESTAMP) {
                final Map<Long, W> matchedCloseWindows = windows.windowsFor(emitRangeUpperBound);
                final Map<Long, W> matchedEmitWindows = windows.windowsFor(emitRangeLowerBound - 1);

                if (matchedCloseWindows.equals(matchedEmitWindows)) {
                    log.trace("No new windows to emit. LastEmitCloseTime={}, emitRangeLowerBound={}, emitRangeUpperBound={}",
                            lastEmitWindowCloseTime, emitRangeLowerBound, emitRangeUpperBound);
                    return false;
                }
            }

            return true;
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<KIn>, VAgg> view() {
        return new KTableValueGetterSupplier<Windowed<KIn>, VAgg>() {

            public KTableValueGetter<Windowed<KIn>, VAgg> get() {
                return new KStreamWindowAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[] {storeName};
            }
        };
    }

    private class KStreamWindowAggregateValueGetter implements KTableValueGetter<Windowed<KIn>, VAgg> {
        private TimestampedWindowStore<KIn, VAgg> windowStore;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            windowStore = context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ValueAndTimestamp<VAgg> get(final Windowed<KIn> windowedKey) {
            final KIn key = windowedKey.key();
            final W window = (W) windowedKey.window();
            return windowStore.fetch(key, window.start());
        }

        @Override
        public boolean isVersioned() {
            return false;
        }
    }
}
