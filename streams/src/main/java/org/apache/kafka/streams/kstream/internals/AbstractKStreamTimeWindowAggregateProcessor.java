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

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emitFinalLatencySensor;
import static org.apache.kafka.streams.processor.internals.metrics.ProcessorNodeMetrics.emittedRecordsSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.KStreamImplJoin.TimeTracker;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;

public abstract class AbstractKStreamTimeWindowAggregateProcessor<KIn, VIn, VAgg> extends ContextualProcessor<KIn, VIn, Windowed<KIn>, Change<VAgg>> {

    private final Time time = Time.SYSTEM;
    private final String storeName;
    private final EmitStrategy emitStrategy;
    private final boolean sendOldValues;
    protected final TimeTracker timeTracker = new TimeTracker();

    private TimestampedTupleForwarder<Windowed<KIn>, VAgg> tupleForwarder;
    protected TimestampedWindowStore<KIn, VAgg> windowStore;
    protected Sensor droppedRecordsSensor;
    protected Sensor emittedRecordsSensor;
    protected Sensor emitFinalLatencySensor;
    protected long lastEmitWindowCloseTime = ConsumerRecord.NO_TIMESTAMP;
    protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    protected InternalProcessorContext<Windowed<KIn>, Change<VAgg>> internalProcessorContext;

    protected AbstractKStreamTimeWindowAggregateProcessor(final String storeName,
                                                          final EmitStrategy emitStrategy,
                                                          final boolean sendOldValues) {
        this.storeName = storeName;
        this.emitStrategy = emitStrategy;
        this.sendOldValues = sendOldValues;
    }

    @Override
    public void init(final ProcessorContext<Windowed<KIn>, Change<VAgg>> context) {
        super.init(context);
        internalProcessorContext = (InternalProcessorContext<Windowed<KIn>, Change<VAgg>>) context;
        final StreamsMetricsImpl metrics = internalProcessorContext.metrics();
        final String threadId = Thread.currentThread().getName();
        final String processorName = internalProcessorContext.currentNode().name();
        droppedRecordsSensor = droppedRecordsSensor(threadId, context.taskId().toString(), metrics);
        emittedRecordsSensor = emittedRecordsSensor(threadId, context.taskId().toString(), processorName, metrics);
        emitFinalLatencySensor = emitFinalLatencySensor(threadId, context.taskId().toString(), processorName, metrics);
        windowStore = context.getStateStore(storeName);

        if (emitStrategy.type() == StrategyType.ON_WINDOW_CLOSE) {
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
                windowStore,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }
    }

    protected void maybeForwardUpdate(final Record<KIn, VIn> record,
                                      final Window window,
                                      final VAgg oldAgg,
                                      final VAgg newAgg,
                                      final long newTimestamp) {
        if (emitStrategy.type() == StrategyType.ON_WINDOW_CLOSE) {
            return;
        }

        tupleForwarder.maybeForward(
            record.withKey(new Windowed<>(record.key(), window))
                .withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null))
                .withTimestamp(newTimestamp));
    }

    protected void maybeForwardFinalResult(final Record<KIn, VIn> record, final long windowCloseTime) {
        if (shouldEmitFinal(windowCloseTime)) {
            final long emitRangeUpperBound = emitRangeUpperBound(windowCloseTime);

            // if the upper bound is smaller than 0, then there's no window closed ever;
            // and we can skip range fetching
            if (emitRangeUpperBound >= 0) {
                final long emitRangeLowerBound = emitRangeLowerBound(windowCloseTime);

                if (shouldRangeFetch(emitRangeLowerBound, emitRangeUpperBound)) {
                    fetchAndEmit(record, windowCloseTime, emitRangeLowerBound, emitRangeUpperBound);
                }
            }
        }
    }

    protected void logSkippedRecordForExpiredWindow(final Logger log,
                                                    final long timestamp,
                                                    final long windowExpire,
                                                    final String window) {
        if (context().recordMetadata().isPresent()) {
            final RecordMetadata recordMetadata = context().recordMetadata().get();
            log.warn("Skipping record for expired window. " +
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
                window,
                windowExpire,
                observedStreamTime
            );
        } else {
            log.warn("Skipping record for expired window. Topic, partition, and offset not known. " +
                "timestamp=[{}] " +
                "window={} " +
                "expiration=[{}] " +
                "streamTime=[{}]",
                timestamp,
                window,
                windowExpire,
                observedStreamTime
            );
        }
        droppedRecordsSensor.record();
    }

    protected void updateObservedStreamTime(final long timestamp) {
        observedStreamTime = Math.max(observedStreamTime, timestamp);
    }

    private boolean shouldEmitFinal(final long windowCloseTime) {
        if (emitStrategy.type() != StrategyType.ON_WINDOW_CLOSE) {
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

    private void fetchAndEmit(final Record<KIn, VIn> record,
                              final long windowCloseTime,
                              final long emitRangeLowerBound,
                              final long emitRangeUpperBound) {
        final long startMs = time.milliseconds();

        final KeyValueIterator<Windowed<KIn>, ValueAndTimestamp<VAgg>> windowToEmit = windowStore
            .fetchAll(emitRangeLowerBound, emitRangeUpperBound);

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

    // upper and lower bound are inclusive; the bounds could be negative in which case we would skip range fetching and emitting
    abstract protected long emitRangeLowerBound(final long windowCloseTime);

    abstract protected long emitRangeUpperBound(final long windowCloseTime);

    abstract protected boolean shouldRangeFetch(final long emitRangeLowerBound, final long emitRangeUpperBound);
}
