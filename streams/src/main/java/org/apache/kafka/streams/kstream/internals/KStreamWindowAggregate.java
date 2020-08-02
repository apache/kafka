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
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrLateRecordDropSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamWindowAggregate<K, V, Agg, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<Agg> initializer;
    private final Aggregator<? super K, ? super V, Agg> aggregator;
    private final String deadLetterNodeName;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(final Windows<W> windows,
                                  final String storeName,
                                  final Initializer<Agg> initializer,
                                  final Aggregator<? super K, ? super V, Agg> aggregator) {
        this(windows, storeName, initializer, aggregator, null);
    }

    public KStreamWindowAggregate(
        final Windows<W> windows,
        final String storeName,
        final Initializer<Agg> initializer,
        final Aggregator<? super K, ? super V, Agg> aggregator,
        final String deadLetterNodeName
    ) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.deadLetterNodeName = deadLetterNodeName;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamWindowAggregateProcessor();
    }

    public Windows<W> windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamWindowAggregateProcessor extends AbstractProcessor<K, V> {
        private TimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<Windowed<K>, Agg> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private InternalProcessorContext internalProcessorContext;
        private Sensor lateRecordDropSensor;
        private Sensor droppedRecordsSensor;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext) context;
            metrics = internalProcessorContext.metrics();
            final String threadId = Thread.currentThread().getName();
            lateRecordDropSensor = droppedRecordsSensorOrLateRecordDropSensor(
                threadId,
                context.taskId().toString(),
                internalProcessorContext.currentNode().name(),
                metrics
            );
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(threadId, context.taskId().toString(), metrics);
            windowStore = (TimestampedWindowStore<K, Agg>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                windowStore,
                context,
                new TimestampedCacheFlushListener<>(context, deadLetterNodeName == null ? Collections.emptySet() : Collections.singleton(deadLetterNodeName)),
                sendOldValues
            );
        }

        @Override
        public void process(final K key, final V value) {
            if (key == null) {
                log.warn(
                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            // first get the matching windows
            final long timestamp = context().timestamp();
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs();

            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                final Long windowStart = entry.getKey();
                final long windowEnd = entry.getValue().end();
                if (windowEnd > closeTime) {
                    final ValueAndTimestamp<Agg> oldAggAndTimestamp = windowStore.fetch(key, windowStart);
                    Agg oldAgg = getValueOrNull(oldAggAndTimestamp);

                    final Agg newAgg;
                    final long newTimestamp;

                    if (oldAgg == null) {
                        oldAgg = initializer.apply();
                        newTimestamp = context().timestamp();
                    } else {
                        newTimestamp = Math.max(context().timestamp(), oldAggAndTimestamp.timestamp());
                    }

                    newAgg = aggregator.apply(key, value, oldAgg);

                    // update the store with the new value
                    windowStore.put(key, ValueAndTimestamp.make(newAgg, newTimestamp), windowStart);
                    tupleForwarder.maybeForward(
                        new Windowed<>(key, entry.getValue()),
                        newAgg,
                        sendOldValues ? oldAgg : null,
                        To.all()
                            .withTimestamp(newTimestamp)
                            .withExclusions(deadLetterNodeName == null ? Collections.emptySet() : Collections.singleton(deadLetterNodeName))
                    );
                } else {
                    log.warn(
                        "Skipping record for expired window. " +
                            "key=[{}] " +
                            "topic=[{}] " +
                            "partition=[{}] " +
                            "offset=[{}] " +
                            "timestamp=[{}] " +
                            "window=[{},{}) " +
                            "expiration=[{}] " +
                            "streamTime=[{}]",
                        key,
                        context().topic(),
                        context().partition(),
                        context().offset(),
                        context().timestamp(),
                        windowStart, windowEnd,
                        closeTime,
                        observedStreamTime
                    );
                    if (deadLetterNodeName != null) {
                        context().forward(key, value, To.child(deadLetterNodeName));
                    }
                    lateRecordDropSensor.record();
                }
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, Agg> view() {
        return new KTableValueGetterSupplier<Windowed<K>, Agg>() {

            public KTableValueGetter<Windowed<K>, Agg> get() {
                return new KStreamWindowAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }


    private class KStreamWindowAggregateValueGetter implements KTableValueGetter<Windowed<K>, Agg> {
        private TimestampedWindowStore<K, Agg> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            windowStore = (TimestampedWindowStore<K, Agg>) context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public ValueAndTimestamp<Agg> get(final Windowed<K> windowedKey) {
            final K key = windowedKey.key();
            final W window = (W) windowedKey.window();
            return windowStore.fetch(key, window.start());
        }

        @Override
        public void close() {
        }
    }
}
