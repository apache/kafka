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
import org.apache.kafka.streams.kstream.SlidingWindows;
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
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrLateRecordDropSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;
import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KStreamSlidingWindowAggregate<K, V, Agg> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final SlidingWindows windows;
    private final Initializer<Agg> initializer;
    private final Aggregator<? super K, ? super V, Agg> aggregator;

    private boolean sendOldValues = false;

    public KStreamSlidingWindowAggregate(final SlidingWindows windows,
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

    public SlidingWindows windows() {
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
            if (key == null || value == null) {
                log.warn(
                    "Skipping record due to null key or value. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            final long timestamp = context().timestamp();

            processInOrder(key, value, timestamp);
        }

        public void processInOrder(final K key, final V value, final long timestamp) {

            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs();

            if (timestamp < windows.timeDifferenceMs()) {
                processEarly(key, value, timestamp, closeTime);
                return;
            }

            final Set<Long> windowStartTimes = new HashSet<>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            //if current record's left/right windows already exist
            boolean leftWinAlreadyCreated = false;
            boolean rightWinAlreadyCreated = false;

            // Store the previous record
            Long previousRecord = null;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(
                    key,
                    key,
                    Math.max(0, timestamp - 2 * windows.timeDifferenceMs()),
                    // to catch the current record's right window, if it exists, without more calls to the store
                    timestamp + 1)
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next = iterator.next();
                    windowStartTimes.add(next.key.window().start());
                    final long startTime = next.key.window().start();
                    final long endTime = startTime + windows.timeDifferenceMs();

                    if (endTime < timestamp) {
                        leftWinAgg = next.value;
                        // update to store the previous record
                        previousRecord = next.value.timestamp();
                    } else if (endTime == timestamp) {
                        leftWinAlreadyCreated = true;
                        if (next.value.timestamp() < timestamp) {
                            previousRecord = next.value.timestamp();
                        }
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                    } else if (endTime > timestamp && startTime <= timestamp) {
                        rightWinAgg = next.value;
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                    } else {
                        rightWinAlreadyCreated = true;
                    }
                }
            }

            //create right window for previous record
            if (previousRecord != null) {
                final long previousRightWinStart = previousRecord + 1;
                if (!windowStartTimes.contains(previousRightWinStart)) {
                    final TimeWindow window = new TimeWindow(previousRightWinStart, previousRightWinStart + windows.timeDifferenceMs());
                    final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                    putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
                }
            }

            //create left window for new record
            if (!leftWinAlreadyCreated) {
                final ValueAndTimestamp<Agg> valueAndTime;
                // if there's a right window that the new record could create && previous record falls within left window -> new record's left window is not empty
                if (previousRecord != null && leftWindowNotEmpty(previousRecord, timestamp)) {
                    valueAndTime = ValueAndTimestamp.make(leftWinAgg.value(), timestamp);
                } else {
                    valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                }
                final TimeWindow window = new TimeWindow(timestamp - windows.timeDifferenceMs(), timestamp);
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
            }
            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, timestamp)) {
                createRightWindow(timestamp, rightWinAgg, key, value, closeTime);
            }
        }

        /**
         * Created to handle records that have a timestamp > 0 but < timeDifference. These records would create
         * windows with negative start times, which is not supported. Instead, they will fall within the [0, timeDifference]
         * window, and we will update their right windows as new records come in later
         */
        private void processEarly(final K key, final V value, final long timestamp, final long closeTime) {
            ValueAndTimestamp<Agg> rightWinAgg = null;
            //window from [0,timeDifference] that holds all early records
            KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> combinedWindow = null;
            boolean rightWinAlreadyCreated = false;
            final Set<Long> windowStartTimes = new HashSet<>();

            Long previousRecordTimestamp = null;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(
                    key,
                    key,
                    Math.max(0, timestamp - 2 * windows.timeDifferenceMs()),
                    // to catch the current record's right window, if it exists, without more calls to the store
                    timestamp + 1)
            ) {
                KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next;
                while (iterator.hasNext()) {
                    next = iterator.next();
                    windowStartTimes.add(next.key.window().start());
                    final long startTime = next.key.window().start();

                    if (startTime == 0) {
                        combinedWindow = next;
                        if (next.value.timestamp() < timestamp) {
                            previousRecordTimestamp = next.value.timestamp();
                        }

                    } else if (startTime <= timestamp) {
                        rightWinAgg = next.value;
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                    } else if (startTime == timestamp + 1) {
                        rightWinAlreadyCreated = true;
                    }
                }
            }

            // if there wasn't a right window agg found and we need a right window for our new record,
            // the current aggregate in the combined window will go in the new record's right window
            if (rightWinAgg == null && combinedWindow != null && combinedWindow.value.timestamp() > timestamp) {
                rightWinAgg = combinedWindow.value;
            }

            if (combinedWindow == null) {
                final TimeWindow window = new TimeWindow(0, windows.timeDifferenceMs());
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);

            } else {
                //create the right window for the previous record if the previous record exists and the window hasn't already been created
                if (previousRecordTimestamp != null && !windowStartTimes.contains(previousRecordTimestamp + 1)) {
                    final TimeWindow window = new TimeWindow(previousRecordTimestamp + 1, previousRecordTimestamp + 1 + windows.timeDifferenceMs());
                    final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                    putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
                }
                //update the combined window with the new aggregate
                putAndForward(combinedWindow.key.window(), combinedWindow.value, key, value, closeTime, timestamp);
            }
            //create right window for new record if needed
            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, timestamp)) {
                createRightWindow(timestamp, rightWinAgg, key, value, closeTime);
            }
        }

        private void createRightWindow(final long timestamp,
                                       final ValueAndTimestamp<Agg> rightWinAgg,
                                       final K key,
                                       final V value,
                                       final long closeTime) {
            final TimeWindow window = new TimeWindow(timestamp + 1, timestamp + 1 + windows.timeDifferenceMs());
            final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(getValueOrNull(rightWinAgg), Math.max(rightWinAgg.timestamp(), timestamp));
            putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
        }

        private boolean leftWindowNotEmpty(final long previousTimestamp, final long currentTimestamp) {
            return currentTimestamp - windows.timeDifferenceMs() <= previousTimestamp;
        }

        // previous record's right window does not already exist and current record falls within previous record's right window
        private boolean rightWindowNecessaryAndPossible(final Set<Long> windowStartTimes,
                                                        final long previousRightWindowStart,
                                                        final long currentRecordTimestamp) {
            return !windowStartTimes.contains(previousRightWindowStart) && previousRightWindowStart + windows.timeDifferenceMs() >= currentRecordTimestamp;
        }

        private boolean rightWindowIsNotEmpty(final ValueAndTimestamp<Agg> rightWinAgg, final long timestamp) {
            return rightWinAgg != null && rightWinAgg.timestamp() > timestamp;
        }

        private void putAndForward(final Window window,
                                   final ValueAndTimestamp<Agg> valueAndTime,
                                   final K key,
                                   final V value,
                                   final long closeTime,
                                   final long timestamp) {
            final long windowStart = window.start();
            final long windowEnd = window.end();
            if (windowEnd > closeTime) {
                //get aggregate from existing window
                final Agg oldAgg = getValueOrNull(valueAndTime);
                final Agg newAgg;
                // keep old aggregate if adding a right window, else add new record's value
                if (windowStart == timestamp + 1) {
                    newAgg = oldAgg;
                } else {
                    newAgg = aggregator.apply(key, value, oldAgg);
                }
                final long newTimestamp = oldAgg == null ? timestamp : Math.max(timestamp, valueAndTime.timestamp());
                windowStore.put(
                    key,
                    ValueAndTimestamp.make(newAgg, newTimestamp),
                    windowStart);
                tupleForwarder.maybeForward(
                    new Windowed<K>(key, window),
                    newAgg,
                    sendOldValues ? oldAgg : null,
                    newTimestamp);
            } else {
                log.warn(
                    "Skipping record for expired window. " +
                        "key=[{}] " +
                        "topic=[{}] " +
                        "partition=[{}] " +
                        "offset=[{}] " +
                        "timestamp=[{}] " +
                        "window=[{},{}] " +
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
            return windowStore.fetch(key, windowedKey.window().start());
        }

        @Override
        public void close() {}
    }
}
