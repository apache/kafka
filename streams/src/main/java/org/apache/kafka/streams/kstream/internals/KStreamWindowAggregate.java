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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTracker;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emitFinalLatencySensor;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emittedRecordsSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamWindowAggregate<KIn, VIn, VAgg, W extends Window> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;
    private final EmitStrategy emitStrategy;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(final Windows<W> windows,
                                  final String storeName,
                                  final Initializer<VAgg> initializer,
                                  final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this(windows, storeName, EmitStrategy.onWindowUpdate(), initializer, aggregator);
    }

    public KStreamWindowAggregate(final Windows<W> windows,
                                  final String storeName,
                                  final EmitStrategy emitStrategy,
                                  final Initializer<VAgg> initializer,
                                  final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.windows = windows;
        this.storeName = storeName;
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
    public Processor<KIn, VIn, Windowed<KIn>, Change<VAgg>> get() {
        return new KStreamWindowAggregateProcessor();
    }

    public Windows<W> windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamWindowAggregateProcessor extends ContextualProcessor<KIn, VIn, Windowed<KIn>, Change<VAgg>> {
        private TimestampedWindowStore<KIn, VAgg> windowStore;
        private TimestampedTupleForwarder<Windowed<KIn>, VAgg> tupleForwarder;
        private Sensor droppedRecordsSensor;
        private Sensor emittedRecordsSensor;
        private Sensor emitFinalLatencySensor;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
        private long lastEmitWindowCloseTime = ConsumerRecord.NO_TIMESTAMP;
        private InternalProcessorContext<Windowed<KIn>, Change<VAgg>> internalProcessorContext;
        private final TimeTracker timeTracker = new TimeTracker();
        private final Time time = Time.SYSTEM;

        @Override
        public void init(final ProcessorContext<Windowed<KIn>, Change<VAgg>> context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext<Windowed<KIn>, Change<VAgg>>) context;
            final StreamsMetricsImpl metrics = internalProcessorContext.metrics();
            final String threadId = Thread.currentThread().getName();
            droppedRecordsSensor = droppedRecordsSensor(threadId, context.taskId().toString(), metrics);
            emittedRecordsSensor = emittedRecordsSensor(threadId, context.taskId().toString(),
                internalProcessorContext.currentNode().name(), metrics);
            emitFinalLatencySensor = emitFinalLatencySensor(threadId, context.taskId().toString(),
                internalProcessorContext.currentNode().name(), metrics);
            windowStore = context.getStateStore(storeName);

            if (emitStrategy.type() == StrategyType.ON_WINDOW_CLOSE) {
                // Don't set flush lister which emit cache results
                tupleForwarder = new TimestampedTupleForwarder<>(
                    windowStore,
                    context,
                    sendOldValues);
            } else {
                tupleForwarder = new TimestampedTupleForwarder<>(
                    windowStore,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }

            // Restore last emit close time for ON_WINDOW_CLOSE strategy
            if (emitStrategy.type() == StrategyType.ON_WINDOW_CLOSE) {
                final Long lastEmitTime = internalProcessorContext.processorMetadataForKey(storeName);
                if (lastEmitTime != null) {
                    lastEmitWindowCloseTime = lastEmitTime;
                }
                final long emitInterval = StreamsConfig.InternalConfig.getLong(
                    context.appConfigs(),
                    EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION,
                    1000L
                );
                timeTracker.setEmitInterval(emitInterval);
            }
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
            observedStreamTime = Math.max(observedStreamTime, timestamp);
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
                    if (emitStrategy.type() == StrategyType.ON_WINDOW_UPDATE) {
                        tupleForwarder.maybeForward(
                            record.withKey(new Windowed<>(record.key(), entry.getValue()))
                                .withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null))
                                .withTimestamp(newTimestamp));
                    }
                } else {
                    if (context().recordMetadata().isPresent()) {
                        final RecordMetadata recordMetadata = context().recordMetadata().get();
                        log.warn(
                            "Skipping record for expired window. " +
                                "topic=[{}] " +
                                "partition=[{}] " +
                                "offset=[{}] " +
                                "timestamp=[{}] " +
                                "window=[{},{}) " +
                                "expiration=[{}] " +
                                "streamTime=[{}]",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                            record.timestamp(),
                            windowStart, windowEnd,
                            windowCloseTime,
                            observedStreamTime
                        );
                    } else {
                        log.warn(
                            "Skipping record for expired window. Topic, partition, and offset not known. " +
                                "timestamp=[{}] " +
                                "window=[{},{}] " +
                                "expiration=[{}] " +
                                "streamTime=[{}]",
                            record.timestamp(),
                            windowStart, windowEnd,
                            windowCloseTime,
                            observedStreamTime
                        );
                    }
                    droppedRecordsSensor.record();
                }
            }

            tryEmitFinalResult(record, windowCloseTime);
        }

        private void tryEmitFinalResult(final Record<KIn, VIn> record, final long windowCloseTime) {
            if (emitStrategy.type() != StrategyType.ON_WINDOW_CLOSE) {
                return;
            }

            final long now = internalProcessorContext.currentSystemTimeMs();
            // Throttle emit frequency as an optimization, the tradeoff is that we need to remember the
            // window close time when we emitted last time so that we can restart from there in the next emit
            if (now < timeTracker.nextTimeToEmit) {
                return;
            }

            // Schedule next emit time based on now to avoid the case that if system time jumps a lot,
            // this can be triggered every time
            timeTracker.nextTimeToEmit = now;
            timeTracker.advanceNextTimeToEmit();

            // Window close time has not progressed, there will be no windows to close hence no records to emit
            if (lastEmitWindowCloseTime != ConsumerRecord.NO_TIMESTAMP && lastEmitWindowCloseTime >= windowCloseTime) {
                return;
            }

            final long emitRangeUpperBoundInclusive = windowCloseTime - windows.size();
            // No window has ever closed and hence no need to emit any records
            if (emitRangeUpperBoundInclusive < 0) {
                return;
            }


            // Set emitRangeLowerBoundInclusive to -1L if lastEmitWindowCloseTime was not set so that
            // we would fetch from 0L for the first time; otherwise set it to lastEmitWindowCloseTime - windows.size().
            //
            // Note if we get here, it means emitRangeUpperBoundInclusive > 0, which means windowCloseTime > windows.size(),
            // Because we always set lastEmitWindowCloseTime to windowCloseTime before, it means
            // lastEmitWindowCloseTime - windows.size() should always > 0
            // As a result, emitRangeLowerBoundInclusive is always >= 0
            final long emitRangeLowerBoundInclusive = lastEmitWindowCloseTime == ConsumerRecord.NO_TIMESTAMP ?
                -1L : lastEmitWindowCloseTime - windows.size();

            if (lastEmitWindowCloseTime != ConsumerRecord.NO_TIMESTAMP) {
                final Map<Long, W> matchedCloseWindows = windows.windowsFor(emitRangeUpperBoundInclusive);
                final Map<Long, W> matchedEmitWindows = windows.windowsFor(emitRangeLowerBoundInclusive);

                // Don't fetch store if there is no new stores that are closed since the last time we emitted
                if (matchedCloseWindows.equals(matchedEmitWindows)) {
                    log.trace("no new windows to emit. LastEmitCloseTime={}, newCloseTime={}",
                            lastEmitWindowCloseTime, windowCloseTime);
                    return;
                }
            }

            final long startMs = time.milliseconds();

            final KeyValueIterator<Windowed<KIn>, ValueAndTimestamp<VAgg>> windowToEmit = windowStore
                .fetchAll(emitRangeLowerBoundInclusive + 1, emitRangeUpperBoundInclusive);

            int emittedCount = 0;
            while (windowToEmit.hasNext()) {
                emittedCount++;
                final KeyValue<Windowed<KIn>, ValueAndTimestamp<VAgg>> kv = windowToEmit.next();
                tupleForwarder.maybeForward(
                    record.withKey(kv.key)
                        .withValue(new Change<>(kv.value.value(), null))
                        .withTimestamp(kv.value.timestamp())
                        .withHeaders(record.headers()));
            }
            emittedRecordsSensor.record(emittedCount);
            emitFinalLatencySensor.record(time.milliseconds() - startMs);

            lastEmitWindowCloseTime = windowCloseTime;
            internalProcessorContext.addProcessorMetadataKeyValue(storeName, windowCloseTime);
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
    }
}
