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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamWindowAggregate<KIn, VIn, VAgg, W extends Window> implements KStreamAggProcessorSupplier<KIn, VIn, Windowed<KIn>, VAgg> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<VAgg> initializer;
    private final Aggregator<? super KIn, ? super VIn, VAgg> aggregator;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(final Windows<W> windows,
                                  final String storeName,
                                  final Initializer<VAgg> initializer,
                                  final Aggregator<? super KIn, ? super VIn, VAgg> aggregator) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
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
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

        @Override
        public void init(final ProcessorContext<Windowed<KIn>, Change<VAgg>> context) {
            super.init(context);
            final InternalProcessorContext<Windowed<KIn>, Change<VAgg>> internalProcessorContext =
                (InternalProcessorContext<Windowed<KIn>, Change<VAgg>>) context;
            final StreamsMetricsImpl metrics = internalProcessorContext.metrics();
            final String threadId = Thread.currentThread().getName();
            droppedRecordsSensor = droppedRecordsSensor(threadId, context.taskId().toString(), metrics);
            windowStore = context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                windowStore,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
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
            final long closeTime = observedStreamTime - windows.gracePeriodMs();

            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                final Long windowStart = entry.getKey();
                final long windowEnd = entry.getValue().end();
                if (windowEnd > closeTime) {
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
                    tupleForwarder.maybeForward(
                        record.withKey(new Windowed<>(record.key(), entry.getValue()))
                            .withValue(new Change<>(newAgg, sendOldValues ? oldAgg : null))
                            .withTimestamp(newTimestamp));
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
                            closeTime,
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
                            closeTime,
                            observedStreamTime
                        );
                    }
                    droppedRecordsSensor.record();
                }
            }
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
