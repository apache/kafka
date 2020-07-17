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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrLateRecordDropSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamSlidingWindowAggregate<K, V, Agg, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<Agg> initializer;
    private final Aggregator<? super K, ? super V, Agg> aggregator;

    private boolean sendOldValues = false;

    public KStreamSlidingWindowAggregate(final Windows<W> windows,
                                  final String storeName,
                                  final Initializer<Agg> initializer,
                                  final Aggregator<? super K, ? super V, Agg> aggregator) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamSlidingWindowAggregateProcessor();
    }

    public Windows<W> windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }


    private class KStreamSlidingWindowAggregateProcessor extends AbstractProcessor<K, V> {
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
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
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

            final long timestamp = context().timestamp();
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs();
            //Create two new windows defined by the new record: left window ends at the record's time
            final TimeWindow leftWindow = new TimeWindow(timestamp - windows.size(), timestamp);
            //right window starts one after the record's time
            final TimeWindow rightWindow = new TimeWindow(timestamp + 1, timestamp + 1 + windows.size());

            Agg oldAgg = initializer.apply();
            try (
                    //Fetch all the windows that have a start time between timestamp and timestamp+windowSize
                    final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetchAll(
                            timestamp - windows.size(),
                            timestamp
                    )
            ) {
                //Updating the already created windows that the new record falls within
                while (iterator.hasNext()) {
                    final Agg newAgg;
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next = iterator.next();
                    final long windowStart = next.value.timestamp();
                    final long windowEnd = next.value.timestamp() + windows.size();
                    if (next.value.timestamp() + windows.size() > closeTime) {
                        //get aggregate from existing window
                        oldAgg = getValueOrNull(next.value);
                        //add record's value to existing aggregate
                        newAgg = aggregator.apply(key, value, oldAgg);
                        windowStore.put(next.key.key(),
                                ValueAndTimestamp.make(newAgg, windowStart),
                                windowStart);
                        tupleForwarder.maybeForward(
                                next.key,
                                newAgg,
                                sendOldValues ? oldAgg : null,
                               windowStart);
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
                        lateRecordDropSensor.record();
                    }
                }
                //check to see if new right window created by the record already exists
                final ValueAndTimestamp<Agg> rightWindowAggAndTimestamp = windowStore.fetch(key, rightWindow.start());
                final Agg recordAgg = getValueOrNull(rightWindowAggAndTimestamp);
                //Grace check for the new right window
                if (rightWindow.end() > closeTime) {
                    //If the right windows needs to have a new aggregate updated
                    if (recordAgg == null && timestamp != observedStreamTime) {
                        windowStore.put(key,
                                ValueAndTimestamp.make(oldAgg, rightWindowAggAndTimestamp.timestamp()),
                                rightWindow.start());
                        tupleForwarder.maybeForward(
                                new Windowed<K>(key, rightWindow),
                                oldAgg,
                                sendOldValues ? null : null,
                                rightWindowAggAndTimestamp.timestamp());

                    } else if (timestamp == observedStreamTime) {
                        windowStore.put(key,
                                ValueAndTimestamp.make(initializer.apply(), rightWindowAggAndTimestamp.timestamp()),
                                rightWindow.start());
                    }
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
                            rightWindow.start(), rightWindow.start(),
                            closeTime,
                            observedStreamTime
                    );
                    lateRecordDropSensor.record();
                }
            }

            try (
                    //Get windows to find aggregate to update new left window
                    final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetchAll(
                            timestamp - windows.size() - windows.size(),
                            timestamp - windows.size()
                    )
            ) {
                Agg storedAgg = initializer.apply();
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next = iterator.next();
                    storedAgg = getValueOrNull(next.value);
                }
                //We've found an updated value for the new window
                if (leftWindow.end() > closeTime) {
                    if (storedAgg != initializer.apply()) {
                        windowStore.put(key,
                                ValueAndTimestamp.make(storedAgg, leftWindow.start()),
                                leftWindow.start());
                        tupleForwarder.maybeForward(
                                new Windowed<K>(key, leftWindow),
                                storedAgg,
                                sendOldValues ? null : null,
                                leftWindow.start());
                    }
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
                            rightWindow.start(), rightWindow.start(),
                            closeTime,
                            observedStreamTime
                    );
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
                return new String[] {storeName};
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
        public void close() {}
    }
}
