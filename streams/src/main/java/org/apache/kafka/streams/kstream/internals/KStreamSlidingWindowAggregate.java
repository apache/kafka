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

import java.time.Instant;
import java.util.HashSet;

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
        private boolean reverseIteratorImplemented = false;

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
            //catch unsupported operation error
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
            if (reverseIteratorImplemented) {
                processReverse(key, value);
            } else {
                processInOrder(key, value);
            }
        }

        public void processReverse(final K key, final V value) {
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
            //store start times of windows we find
            final HashSet<Long> windowStartTimes = new HashSet<Long>();
            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            // flag to help determine if out-of-order record’s right window is non-empty
            boolean foundLeftFirst = false;
            //if current record's left/right windows already exist
            boolean leftWinExists = false;
            boolean rightWindowExists = false;
            //to determine if we're creating the previous record's right window, helps with determining empty windows
            boolean prevRightWinExists = false;
            boolean prevRightWinAlreadyCreated = false;

            try (
                    //Fetch all the windows that have a start time between timestamp and timestamp+timeDifference
                    final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(key,
                            key,
                            Instant.ofEpochMilli(timestamp - 2 * windows.timeDifference()),
                            Instant.ofEpochMilli(timestamp + 1))
            ) {
                KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next;
                //if we've already seen the window with the closest start time to the record
                boolean foundFirst = false;
                //if we've already seen the window with the closest end time to the record
                boolean foundFirstEndTime = false;
                while (iterator.hasNext()) {
                    next = iterator.next();
                    windowStartTimes.add(next.key.window().start());

                    //determine if current record's right window exists
                    if (next.key.window().start() == timestamp + 1) {
                        rightWindowExists = true;
                        continue;
                    } else if (next.key.window().end() > timestamp) {
                        if (!foundFirst) {
                            foundFirst = true;
                            if (isLeftWindow(next)) {
                                foundLeftFirst = true;
                            }
                        }
                        rightWinAgg = next.value;
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                        continue;
                    } else if (next.key.window().end() == timestamp) {
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                        leftWinExists = true;
                        continue;
                    } else {
                        if (!foundFirstEndTime) {
                            leftWinAgg = next.value;
                            foundFirstEndTime = true;
                        }
                        if (prevRightWinExists) {
                            break;
                        }
                        if (isLeftWindow(next)) {
                            prevRightWinExists = true;
                            final long rightWinStart = next.key.window().end() + 1;
                            if (windowStartTimes.contains(rightWinStart)) {
                                prevRightWinAlreadyCreated = true;
                            } else {
                                final TimeWindow window = new TimeWindow(rightWinStart, rightWinStart + windows.timeDifference);
                                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
                            }
                        }
                    }
                }
            }

            //create the left window of the current record if it's not created
            if (!leftWinExists) {
                final Agg aggValue;
                final long newTimestamp;
                //confirms that the left window contains more than the current record
                if (prevRightWinExists) {
                    aggValue = aggregator.apply(key, value, getValueOrNull(leftWinAgg));
                    newTimestamp = leftWinAgg.timestamp();
                } else {
                    //left window just contains the current record
                    aggValue = aggregator.apply(key, value, initializer.apply());
                    newTimestamp = timestamp;
                }
                final TimeWindow window = new TimeWindow(timestamp - windows.timeDifference, timestamp);
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(aggValue, Math.max(timestamp, newTimestamp));
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
            }
            //create the right window for
            if (!rightWindowExists && (foundLeftFirst || prevRightWinAlreadyCreated)) {
                final TimeWindow window = new TimeWindow(timestamp + 1, timestamp + 1 + windows.timeDifference);
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(getValueOrNull(rightWinAgg), Math.max(rightWinAgg.timestamp(), timestamp));
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
            }
        }

        public void processInOrder(final K key, final V value) {
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

            //store start times of windows we find
            final HashSet<Long> windowStartTimes = new HashSet<Long>();

            // aggregate that will go in the current record’s left/right window (if needed)
            ValueAndTimestamp<Agg> leftWinAgg = null;
            ValueAndTimestamp<Agg> rightWinAgg = null;

            // flag to help determine if out-of-order record’s right window is non-empty
            boolean foundLeftFirst = false;
            //if current record's left/right windows already exist
            boolean leftWinExists = false;
            boolean rightWindowExists = false;
            //to determine if we're creating the previous record's right window, helps with determining empty windows
            boolean prevRightWinAlreadyCreated = false;


            //to keep find the left type window closest to the record
            Window latestLeftTypeWindow = null;

            try (
                    //Fetch all the windows that have a start time between timestamp and timestamp+timeDifference
                    //potentially need to change long to instant
                    final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = windowStore.fetch(key,
                            key,
                            Instant.ofEpochMilli(timestamp - 2 * windows.timeDifference()),
                            Instant.ofEpochMilli(timestamp + 1))
            ) {

                KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next = iterator.next();

                while (iterator.hasNext()) {
                    next = iterator.next();
                    windowStartTimes.add(next.key.window().start());

                    final long endTime = next.key.window().end();
                    final long startTime = next.key.window().start();

                    if (endTime < timestamp) {
                        leftWinAgg = next.value;
                        if (isLeftWindow(next)) {
                            latestLeftTypeWindow = next.key.window();
                        }
                        continue;
                    } else if (endTime == timestamp) {
                        leftWinExists = true;
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                        continue;
                    } else if (endTime > timestamp && startTime <= timestamp) {
                        rightWinAgg = next.value;
                        foundLeftFirst = isLeftWindow(next) ? true : false;
                        putAndForward(next.key.window(), next.value, key, value, closeTime, timestamp);
                        continue;
                    } else {
                        rightWindowExists = true;
                    }
                }
            }

            //create right window for previous record
            if (latestLeftTypeWindow != null) {
                final long rightWinStart = latestLeftTypeWindow.end() + 1;
                if (windowStartTimes.contains(rightWinStart)) {
                    prevRightWinAlreadyCreated = true;
                } else {
                    final TimeWindow window = new TimeWindow(rightWinStart, rightWinStart + windows.timeDifference);
                    final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(initializer.apply(), timestamp);
                    putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
                }
            }

            //create left window for new record
            if (!leftWinExists) {
                final Agg aggValue;
                final long newTimestamp;
                if (latestLeftTypeWindow != null) {
                    aggValue = aggregator.apply(key, value, getValueOrNull(leftWinAgg));
                    newTimestamp = leftWinAgg.timestamp();
                } else {
                    aggValue = aggregator.apply(key, value, initializer.apply());
                    newTimestamp = timestamp;
                }
                final TimeWindow window = new TimeWindow(timestamp - windows.timeDifference, timestamp);
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(aggValue, Math.max(timestamp, newTimestamp));
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
            }
            //create right window for new record
            if (!rightWindowExists && (foundLeftFirst || prevRightWinAlreadyCreated)) {
                final TimeWindow window = new TimeWindow(timestamp + 1, timestamp + 1 + windows.timeDifference);
                final ValueAndTimestamp<Agg> valueAndTime = ValueAndTimestamp.make(getValueOrNull(rightWinAgg), Math.max(rightWinAgg.timestamp(), timestamp));
                putAndForward(window, valueAndTime, key, value, closeTime, timestamp);
            }

        }

        private boolean isLeftWindow(final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> window) {
            return window.key.window().end() == window.value.timestamp();
        }

        private void putAndForward(final Window window, final ValueAndTimestamp<Agg> valueAndTime, final K key,
                                   final V value, final long closeTime, final long timestamp) {
            final long windowStart = window.start();
            final long windowEnd = window.end();
            if (windowEnd > closeTime) {
                //get aggregate from existing window
                final Agg oldAgg = getValueOrNull(valueAndTime);
                //add record's value to existing aggregate
                final Agg newAgg = aggregator.apply(key, value, oldAgg);

                windowStore.put(key,
                        ValueAndTimestamp.make(newAgg, Math.max(timestamp, valueAndTime.timestamp())),
                        windowStart);
                tupleForwarder.maybeForward(
                        new Windowed<K>(key, window),
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
