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
        private Boolean reverseIteratorPossible = null;

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

            final long inputRecordTimestamp = context().timestamp();
            observedStreamTime = Math.max(observedStreamTime, inputRecordTimestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs();

            if (inputRecordTimestamp + 1L + windows.timeDifferenceMs() <= closeTime) {
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
                    inputRecordTimestamp - windows.timeDifferenceMs(), inputRecordTimestamp,
                    closeTime,
                    observedStreamTime
                );
                lateRecordDropSensor.record();
                return;
            }

            if (inputRecordTimestamp < windows.timeDifferenceMs()) {
                processEarly(key, value, inputRecordTimestamp, closeTime);
                return;
            }

            if (reverseIteratorPossible == null) {
                try {
                    windowStore.backwardFetch(key, 0L, 0L);
                    reverseIteratorPossible = true;
                    log.debug("Sliding Windows aggregate using a reverse iterator");
                } catch (final UnsupportedOperationException e)  {
                    reverseIteratorPossible = false;
                    log.debug("Sliding Windows aggregate using a forward iterator");
                }
            }

            if (reverseIteratorPossible) {
                processReverse(key, value, inputRecordTimestamp, closeTime);
            } else {
                processInOrder(key, value, inputRecordTimestamp, closeTime);
            }
        }

        public void processInOrder(final K key, final V value, final long inputRecordTimestamp, final long closeTime) {

            final Set<Long> windowStartTimes = new HashSet<>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            //if current record's left/right windows already exist
            boolean leftWinAlreadyCreated = false;
            boolean rightWinAlreadyCreated = false;

            Long previousRecordTimestamp = null;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(
                    key,
                    key,
                    Math.max(0, inputRecordTimestamp - 2 * windows.timeDifferenceMs()),
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    inputRecordTimestamp + 1)
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long endTime = startTime + windows.timeDifferenceMs();
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();

                    if (endTime < inputRecordTimestamp) {
                        leftWinAgg = windowBeingProcessed.value;
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                    } else if (endTime == inputRecordTimestamp) {
                        leftWinAlreadyCreated = true;
                        if (windowMaxRecordTimestamp < inputRecordTimestamp) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        }
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, key, value, closeTime, inputRecordTimestamp);
                    } else if (endTime > inputRecordTimestamp && startTime <= inputRecordTimestamp) {
                        rightWinAgg = windowBeingProcessed.value;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, key, value, closeTime, inputRecordTimestamp);
                    } else if (startTime == inputRecordTimestamp + 1) {
                        rightWinAlreadyCreated = true;
                    } else {
                        log.error(
                            "Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.",
                            startTime, inputRecordTimestamp
                        );
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }
            createWindows(key, value, inputRecordTimestamp, closeTime, windowStartTimes, rightWinAgg, leftWinAgg, leftWinAlreadyCreated, rightWinAlreadyCreated, previousRecordTimestamp);
        }

        public void processReverse(final K key, final V value, final long inputRecordTimestamp, final long closeTime) {

            final Set<Long> windowStartTimes = new HashSet<>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            //if current record's left/right windows already exist
            boolean leftWinAlreadyCreated = false;
            boolean rightWinAlreadyCreated = false;

            Long previousRecordTimestamp = null;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.backwardFetch(
                    key,
                    key,
                    Math.max(0, inputRecordTimestamp - 2 * windows.timeDifferenceMs()),
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    inputRecordTimestamp + 1)
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long endTime = startTime + windows.timeDifferenceMs();
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();
                    if (startTime == inputRecordTimestamp + 1) {
                        rightWinAlreadyCreated = true;
                    } else if (endTime > inputRecordTimestamp) {
                        if (rightWinAgg == null) {
                            rightWinAgg = windowBeingProcessed.value;
                        }
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, key, value, closeTime, inputRecordTimestamp);
                    } else if (endTime == inputRecordTimestamp) {
                        leftWinAlreadyCreated = true;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, key, value, closeTime, inputRecordTimestamp);
                        if (windowMaxRecordTimestamp < inputRecordTimestamp) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        } else {
                            return;
                        }
                    } else if (endTime < inputRecordTimestamp) {
                        leftWinAgg = windowBeingProcessed.value;
                        previousRecordTimestamp = windowMaxRecordTimestamp;
                        break;
                    } else {
                        log.error(
                            "Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.",
                            startTime, inputRecordTimestamp
                        );
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }
            createWindows(key, value, inputRecordTimestamp, closeTime, windowStartTimes, rightWinAgg, leftWinAgg, leftWinAlreadyCreated, rightWinAlreadyCreated, previousRecordTimestamp);
        }

        /**
         * Created to handle records where 0 < inputRecordTimestamp < timeDifferenceMs. These records would create
         * windows with negative start times, which is not supported. Instead, we will put them into the [0, timeDifferenceMs]
         * window as a "workaround", and we will update or create their right windows as new records come in later
         */
        private void processEarly(final K key, final V value, final long inputRecordTimestamp, final long closeTime) {
            if (inputRecordTimestamp < 0 || inputRecordTimestamp >= windows.timeDifferenceMs()) {
                log.error(
                    "Early record for sliding windows must fall between fall between 0 <= inputRecordTimestamp. Timestamp {} does not fall between 0 <= {}",
                    inputRecordTimestamp, windows.timeDifferenceMs()
                );
                throw new IllegalArgumentException("Early record for sliding windows must fall between fall between 0 <= inputRecordTimestamp");
            }

            // A window from [0, timeDifferenceMs] that holds all early records
            KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> combinedWindow = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;
            boolean rightWinAlreadyCreated = false;
            final Set<Long> windowStartTimes = new HashSet<>();

            Long previousRecordTimestamp = null;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(
                    key,
                    key,
                    0,
                    // add 1 to upper bound to catch the current record's right window, if it exists, without more calls to the store
                    inputRecordTimestamp + 1)
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> windowBeingProcessed = iterator.next();
                    final long startTime = windowBeingProcessed.key.window().start();
                    windowStartTimes.add(startTime);
                    final long windowMaxRecordTimestamp = windowBeingProcessed.value.timestamp();

                    if (startTime == 0) {
                        combinedWindow = windowBeingProcessed;
                        // We don't need to store previousRecordTimestamp if maxRecordTimestamp >= timestamp
                        // because the previous record's right window (if there is a previous record)
                        // would have already been created by maxRecordTimestamp
                        if (windowMaxRecordTimestamp < inputRecordTimestamp) {
                            previousRecordTimestamp = windowMaxRecordTimestamp;
                        }

                    } else if (startTime <= inputRecordTimestamp) {
                        rightWinAgg = windowBeingProcessed.value;
                        updateWindowAndForward(windowBeingProcessed.key.window(), windowBeingProcessed.value, key, value, closeTime, inputRecordTimestamp);
                    } else if (startTime == inputRecordTimestamp + 1) {
                        rightWinAlreadyCreated = true;
                    } else {
                        log.error(
                            "Unexpected window with start {} found when processing record at {} in `KStreamSlidingWindowAggregate`.",
                            startTime, inputRecordTimestamp
                        );
                        throw new IllegalStateException("Unexpected window found when processing sliding windows");
                    }
                }
            }

            // If there wasn't a right window agg found and we need a right window for our new record,
            // the current aggregate in the combined window will go in the new record's right window. We can be sure that the combined
            // window only holds records that fall into the current record's right window for two reasons:
            // 1. If there were records earlier than the current record AND later than the current record, there would be a right window found
            // when we looked for right window agg.
            // 2. If there was only a record before the current record, we wouldn't need a right window for the current record and wouldn't update the
            // rightWinAgg value here, as the combinedWindow.value.timestamp() < inputRecordTimestamp
            if (rightWinAgg == null && combinedWindow != null && combinedWindow.value.timestamp() > inputRecordTimestamp) {
                rightWinAgg = combinedWindow.value;
            }

            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, inputRecordTimestamp)) {
                createCurrentRecordRightWindow(inputRecordTimestamp, rightWinAgg, key);
            }

            //create the right window for the previous record if the previous record exists and the window hasn't already been created
            if (previousRecordTimestamp != null && !windowStartTimes.contains(previousRecordTimestamp + 1)) {
                createPreviousRecordRightWindow(previousRecordTimestamp + 1, inputRecordTimestamp, key, value, closeTime);
            }

            if (combinedWindow == null) {
                final TimeWindow window = new TimeWindow(0, windows.timeDifferenceMs());
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), inputRecordTimestamp);
                updateWindowAndForward(window, valueAndTime, key, value, closeTime, inputRecordTimestamp);

            } else {
                //update the combined window with the new aggregate
                updateWindowAndForward(combinedWindow.key.window(), combinedWindow.value, key, value, closeTime, inputRecordTimestamp);
            }

        }

        private void createWindows(final K key,
                                   final V value,
                                   final long inputRecordTimestamp,
                                   final long closeTime,
                                   final Set<Long> windowStartTimes,
                                   final ValueAndTimestamp<Agg> rightWinAgg,
                                   final ValueAndTimestamp<Agg> leftWinAgg,
                                   final boolean leftWinAlreadyCreated,
                                   final boolean rightWinAlreadyCreated,
                                   final Long previousRecordTimestamp) {
            //create right window for previous record
            if (previousRecordTimestamp != null) {
                final long previousRightWinStart = previousRecordTimestamp + 1;
                if (previousRecordRightWindowDoesNotExistAndIsNotEmpty(windowStartTimes, previousRightWinStart, inputRecordTimestamp)) {
                    createPreviousRecordRightWindow(previousRightWinStart, inputRecordTimestamp, key, value, closeTime);
                }
            }

            //create left window for new record
            if (!leftWinAlreadyCreated) {
                final ValueAndTimestamp<Agg> valueAndTime;
                if (leftWindowNotEmpty(previousRecordTimestamp, inputRecordTimestamp)) {
                    valueAndTime = ValueAndTimestamp.make(leftWinAgg.value(), inputRecordTimestamp);
                } else {
                    valueAndTime = ValueAndTimestamp.make(initializer.apply(), inputRecordTimestamp);
                }
                final TimeWindow window = new TimeWindow(inputRecordTimestamp - windows.timeDifferenceMs(), inputRecordTimestamp);
                updateWindowAndForward(window, valueAndTime, key, value, closeTime, inputRecordTimestamp);
            }

            // create right window for new record, if necessary
            if (!rightWinAlreadyCreated && rightWindowIsNotEmpty(rightWinAgg, inputRecordTimestamp)) {
                createCurrentRecordRightWindow(inputRecordTimestamp, rightWinAgg, key);
            }
        }

        private void createCurrentRecordRightWindow(final long inputRecordTimestamp,
                                                    final ValueAndTimestamp<Agg> rightWinAgg,
                                                    final K key) {
            final TimeWindow window = new TimeWindow(inputRecordTimestamp + 1, inputRecordTimestamp + 1 + windows.timeDifferenceMs());
            windowStore.put(
                key,
                rightWinAgg,
                window.start());
            tupleForwarder.maybeForward(
                new Windowed<>(key, window),
                rightWinAgg.value(),
                null,
                rightWinAgg.timestamp());
        }

        private void createPreviousRecordRightWindow(final long windowStart,
                                                     final long inputRecordTimestamp,
                                                     final K key,
                                                     final V value,
                                                     final long closeTime) {
            final TimeWindow window = new TimeWindow(windowStart, windowStart + windows.timeDifferenceMs());
            final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), inputRecordTimestamp);
            updateWindowAndForward(window, valueAndTime, key, value, closeTime, inputRecordTimestamp);
        }

        // checks if the previous record falls into the current records left window; if yes, the left window is not empty, otherwise it is empty
        private boolean leftWindowNotEmpty(final Long previousRecordTimestamp, final long inputRecordTimestamp) {
            return previousRecordTimestamp != null && inputRecordTimestamp - windows.timeDifferenceMs() <= previousRecordTimestamp;
        }

        // checks if the previous record's right window does not already exist and the current record falls within previous record's right window
        private boolean previousRecordRightWindowDoesNotExistAndIsNotEmpty(final Set<Long> windowStartTimes,
                                                                           final long previousRightWindowStart,
                                                                           final long inputRecordTimestamp) {
            return !windowStartTimes.contains(previousRightWindowStart) && previousRightWindowStart + windows.timeDifferenceMs() >= inputRecordTimestamp;
        }

        // checks if the aggregate we found has records that fall into the current record's right window; if yes, the right window is not empty
        private boolean rightWindowIsNotEmpty(final ValueAndTimestamp<Agg> rightWinAgg, final long inputRecordTimestamp) {
            return rightWinAgg != null && rightWinAgg.timestamp() > inputRecordTimestamp;
        }

        private void updateWindowAndForward(final Window window,
                                            final ValueAndTimestamp<Agg> valueAndTime,
                                            final K key,
                                            final V value,
                                            final long closeTime,
                                            final long inputRecordTimestamp) {
            final long windowStart = window.start();
            final long windowEnd = window.end();
            if (windowEnd > closeTime) {
                //get aggregate from existing window
                final Agg oldAgg = getValueOrNull(valueAndTime);
                final Agg newAgg = aggregator.apply(key, value, oldAgg);

                final long newTimestamp = oldAgg == null ? inputRecordTimestamp : Math.max(inputRecordTimestamp, valueAndTime.timestamp());
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
