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
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emitFinalLatencySensor;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emittedRecordsSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

public class KStreamSessionWindowAggregate<KIn, VIn, VAgg> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamSessionWindowAggregate.class);

    private final String storeName;
    private final SessionWindows windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;
    private final Merger<? super KIn, VAgg> sessionMerger;
    private final EmitStrategy emitStrategy;

    private boolean sendOldValues = false;

    public KStreamSessionWindowAggregate(final SessionWindows windows,
                                         final String storeName,
                                         final EmitStrategy emitStrategy,
                                         final Initializer<VAgg> initializer,
                                         final Aggregator<? super KIn, ? super VIn, VAgg> aggregator,
                                         final Merger<? super KIn, VAgg> sessionMerger) {
        this.windows = windows;
        this.storeName = storeName;
        this.emitStrategy = emitStrategy;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.sessionMerger = sessionMerger;
    }

    @Override
    public Processor<KIn, VIn, Windowed<KIn>, Change<VAgg>> get() {
        return new KStreamSessionWindowAggregateProcessor();
    }

    public SessionWindows windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamSessionWindowAggregateProcessor extends
        ContextualProcessor<KIn, VIn, Windowed<KIn>, Change<VAgg>> {

        private SessionStore<KIn, VAgg> store;
        private TimestampedTupleForwarder<Windowed<KIn>, VAgg> tupleForwarder;
        private Sensor droppedRecordsSensor;
        private Sensor emittedRecordsSensor;
        private Sensor emitFinalLatencySensor;
        private long lastEmitWindowCloseTime = ConsumerRecord.NO_TIMESTAMP;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
        private InternalProcessorContext<Windowed<KIn>, Change<VAgg>> internalProcessorContext;

        private final Time time = Time.SYSTEM;
        protected final KStreamImplJoin.TimeTracker timeTracker = new KStreamImplJoin.TimeTracker();

        @Override
        public void init(final ProcessorContext<Windowed<KIn>, Change<VAgg>> context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext<Windowed<KIn>, Change<VAgg>>) context;
            final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
            final String threadId = Thread.currentThread().getName();
            final String processorName = internalProcessorContext.currentNode().name();
            droppedRecordsSensor = droppedRecordsSensor(threadId, context.taskId().toString(), metrics);
            emittedRecordsSensor = emittedRecordsSensor(threadId, context.taskId().toString(), processorName, metrics);
            emitFinalLatencySensor = emitFinalLatencySensor(threadId, context.taskId().toString(), processorName, metrics);
            store = context.getStateStore(storeName);

            if (emitStrategy.type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
                // Restore last emit close time for ON_WINDOW_CLOSE strategy
                final Long lastEmitWindowCloseTime = internalProcessorContext.processorMetadataForKey(storeName);
                if (lastEmitWindowCloseTime != null) {
                    this.lastEmitWindowCloseTime = lastEmitWindowCloseTime;
                }
                final long emitInterval = StreamsConfig.InternalConfig.getLong(
                        context.appConfigs(),
                        EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION,
                        1000L
                );
                timeTracker.setEmitInterval(emitInterval);

                tupleForwarder = new TimestampedTupleForwarder<>(context, sendOldValues);
            } else {
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new SessionCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (record.key() == null) {
                logSkippedRecordForNullKey();
                return;
            }

            final long timestamp = record.timestamp();
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long windowCloseTime = observedStreamTime - windows.gracePeriodMs() - windows.inactivityGap();

            final List<KeyValue<Windowed<KIn>, VAgg>> merged = new ArrayList<>();
            final SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            VAgg agg = initializer.apply();

            try (
                final KeyValueIterator<Windowed<KIn>, VAgg> iterator = store.findSessions(
                    record.key(),
                    timestamp - windows.inactivityGap(),
                    timestamp + windows.inactivityGap()
                )
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<KIn>, VAgg> next = iterator.next();
                    merged.add(next);
                    agg = sessionMerger.apply(record.key(), agg, next.value);
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow) next.key.window());
                }
            }

            if (mergedWindow.end() < windowCloseTime) {
                logSkippedRecordForExpiredWindow(timestamp, windowCloseTime, mergedWindow);
            } else {
                if (!mergedWindow.equals(newSessionWindow)) {
                    for (final KeyValue<Windowed<KIn>, VAgg> session : merged) {
                        store.remove(session.key);

                        maybeForwardUpdate(session.key, session.value, null);
                    }
                }

                agg = aggregator.apply(record.key(), record.value(), agg);
                final Windowed<KIn> sessionKey = new Windowed<>(record.key(), mergedWindow);
                store.put(sessionKey, agg);

                maybeForwardUpdate(sessionKey, null, agg);
            }

            maybeForwardFinalResult(record, windowCloseTime);
        }

        private void maybeForwardUpdate(final Windowed<KIn> windowedkey,
                                        final VAgg oldAgg,
                                        final VAgg newAgg) {
            if (emitStrategy.type() == EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
                return;
            }

            // Update the sent record timestamp to the window end time if possible
            final long newTimestamp = windowedkey.window().end();
            tupleForwarder.maybeForward(new Record<>(windowedkey, new Change<>(newAgg, sendOldValues ? oldAgg : null), newTimestamp));
        }

        // TODO: consolidate SessionWindow with TimeWindow to merge common functions
        private void maybeForwardFinalResult(final Record<KIn, VIn> record, final long windowCloseTime) {
            if (shouldEmitFinal(windowCloseTime)) {
                final long emitRangeUpperBound = emitRangeUpperBound(windowCloseTime);

                // if the upper bound is smaller than 0, then there's no window closed ever;
                // and we can skip range fetching
                if (emitRangeUpperBound >= 0) {
                    final long emitRangeLowerBound = emitRangeLowerBound();

                    if (shouldRangeFetch(emitRangeLowerBound, emitRangeUpperBound)) {
                        fetchAndEmit(record, windowCloseTime, emitRangeLowerBound, emitRangeUpperBound);
                    }
                }
            }
        }

        private boolean shouldEmitFinal(final long windowCloseTime) {
            if (emitStrategy.type() != EmitStrategy.StrategyType.ON_WINDOW_CLOSE) {
                return false;
            }

            final long now = internalProcessorContext.currentSystemTimeMs();
            // Throttle emit frequency
            if (now < timeTracker.nextTimeToEmit) {
                return false;
            }

            // Schedule next emit time based on now to avoid the case that if system time jumps a lot,
            // this can be triggered every time
            timeTracker.nextTimeToEmit = now;
            timeTracker.advanceNextTimeToEmit();

            // Only EMIT if the window close time does progress
            return lastEmitWindowCloseTime == ConsumerRecord.NO_TIMESTAMP || lastEmitWindowCloseTime < windowCloseTime;
        }

        private long emitRangeLowerBound() {
            return Math.max(0L, lastEmitWindowCloseTime);
        }

        private long emitRangeUpperBound(final long windowCloseTime) {
            // Session window's start and end timestamps are inclusive, so
            // we should minus 1 for the inclusive closed window-end upper bound
            return windowCloseTime - 1;
        }

        private boolean shouldRangeFetch(final long emitRangeLowerBound, final long emitRangeUpperBound) {
            // since a session window could be a single point (i.e. [t, t]),
            // we need to range fetch and emit even if the upper and lower bound are the same
            return emitRangeUpperBound >= emitRangeLowerBound;
        }

        private void fetchAndEmit(final Record<KIn, VIn> record,
                                  final long windowCloseTime,
                                  final long emitRangeLowerBound,
                                  final long emitRangeUpperBound) {
            final long startMs = time.milliseconds();

            // Only time ordered (indexed) session store should have implemented
            // this function, otherwise a not-supported exception would throw
            final KeyValueIterator<Windowed<KIn>, VAgg> windowToEmit = store
                .findSessions(emitRangeLowerBound, emitRangeUpperBound);

            int emittedCount = 0;
            while (windowToEmit.hasNext()) {
                emittedCount++;
                final KeyValue<Windowed<KIn>, VAgg> kv = windowToEmit.next();

                tupleForwarder.maybeForward(
                    record.withKey(kv.key)
                        .withValue(new Change<>(kv.value, null))
                        // set the timestamp as the window end timestamp
                        .withTimestamp(kv.key.window().end())
                        .withHeaders(record.headers()));
            }
            emittedRecordsSensor.record(emittedCount);
            emitFinalLatencySensor.record(time.milliseconds() - startMs);

            lastEmitWindowCloseTime = windowCloseTime;
            internalProcessorContext.addProcessorMetadataKeyValue(storeName, windowCloseTime);
        }

        private void logSkippedRecordForNullKey() {
            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn(
                        "Skipping record due to null key. "
                                + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                );
            } else {
                LOG.warn(
                        "Skipping record due to null key. Topic, partition, and offset not known."
                );
            }
            droppedRecordsSensor.record();
        }

        private void logSkippedRecordForExpiredWindow(final long timestamp,
                                                      final long windowExpire,
                                                      final SessionWindow window) {
            final String windowString = "[" + window.start() + "," + window.end() + "]";

            if (context().recordMetadata().isPresent()) {
                final RecordMetadata recordMetadata = context().recordMetadata().get();
                LOG.warn("Skipping record for expired window. " +
                                "topic=[{}] " +
                                "partition=[{}] " +
                                "offset=[{}] " +
                                "timestamp=[{}] " +
                                "window={} " +
                                "expiration=[{}] " +
                                "streamTime=[{}]",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        timestamp,
                        windowString,
                        windowExpire,
                        observedStreamTime
                );
            } else {
                LOG.warn("Skipping record for expired window. Topic, partition, and offset not known. " +
                                "timestamp=[{}] " +
                                "window={} " +
                                "expiration=[{}] " +
                                "streamTime=[{}]",
                        timestamp,
                        windowString,
                        windowExpire,
                        observedStreamTime
                );
            }
            droppedRecordsSensor.record();
        }
    }

    private SessionWindow mergeSessionWindow(final SessionWindow one, final SessionWindow two) {
        final long start = Math.min(one.start(), two.start());
        final long end = Math.max(one.end(), two.end());
        return new SessionWindow(start, end);
    }

    @Override
    public KTableValueGetterSupplier<Windowed<KIn>, VAgg> view() {
        return new KTableValueGetterSupplier<Windowed<KIn>, VAgg>() {
            @Override
            public KTableValueGetter<Windowed<KIn>, VAgg> get() {
                return new KTableSessionWindowValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTableSessionWindowValueGetter implements KTableValueGetter<Windowed<KIn>, VAgg> {

        private SessionStore<KIn, VAgg> store;

        @Override
        public void init(final ProcessorContext<?, ?> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<VAgg> get(final Windowed<KIn> key) {
            return ValueAndTimestamp.make(
                store.fetchSession(key.key(), key.window().start(), key.window().end()),
                key.window().end());
        }
    }
}
