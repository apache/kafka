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
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrLateRecordDropSensor;
import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor;

public class KStreamSessionWindowAggregate<K, V, Agg> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamSessionWindowAggregate.class);

    private final String storeName;
    private final SessionWindows windows;
    private final Initializer<Agg> initializer;
    private final Aggregator<? super K, ? super V, Agg> aggregator;
    private final Merger<? super K, Agg> sessionMerger;

    private boolean sendOldValues = false;

    public KStreamSessionWindowAggregate(final SessionWindows windows,
                                         final String storeName,
                                         final Initializer<Agg> initializer,
                                         final Aggregator<? super K, ? super V, Agg> aggregator,
                                         final Merger<? super K, Agg> sessionMerger) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
        this.sessionMerger = sessionMerger;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamSessionWindowAggregateProcessor();
    }

    public SessionWindows windows() {
        return windows;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamSessionWindowAggregateProcessor extends AbstractProcessor<K, V> {

        private SessionStore<K, Agg> store;
        private SessionTupleForwarder<K, Agg> tupleForwarder;
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
            metrics = (StreamsMetricsImpl) context.metrics();
            final String threadId = Thread.currentThread().getName();
            lateRecordDropSensor = droppedRecordsSensorOrLateRecordDropSensor(
                threadId,
                context.taskId().toString(),
                internalProcessorContext.currentNode().name(),
                metrics
            );
            droppedRecordsSensor = droppedRecordsSensorOrSkippedRecordsSensor(threadId, context.taskId().toString(), metrics);
            store = (SessionStore<K, Agg>) context.getStateStore(storeName);
            tupleForwarder = new SessionTupleForwarder<>(store, context, new SessionCacheFlushListener<>(context), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (key == null) {
                LOG.warn(
                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context().topic(), context().partition(), context().offset()
                );
                droppedRecordsSensor.record();
                return;
            }

            final long timestamp = context().timestamp();
            observedStreamTime = Math.max(observedStreamTime, timestamp);
            final long closeTime = observedStreamTime - windows.gracePeriodMs();

            final List<KeyValue<Windowed<K>, Agg>> merged = new ArrayList<>();
            final SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            Agg agg = initializer.apply();

            try (
                final KeyValueIterator<Windowed<K>, Agg> iterator = store.findSessions(
                    key,
                    timestamp - windows.inactivityGap(),
                    timestamp + windows.inactivityGap()
                )
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, Agg> next = iterator.next();
                    merged.add(next);
                    agg = sessionMerger.apply(key, agg, next.value);
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow) next.key.window());
                }
            }

            if (mergedWindow.end() < closeTime) {
                LOG.warn(
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
                    timestamp,
                    mergedWindow.start(),
                    mergedWindow.end(),
                    closeTime,
                    observedStreamTime
                );
                lateRecordDropSensor.record();
            } else {
                if (!mergedWindow.equals(newSessionWindow)) {
                    for (final KeyValue<Windowed<K>, Agg> session : merged) {
                        store.remove(session.key);
                        tupleForwarder.maybeForward(session.key, null, sendOldValues ? session.value : null);
                    }
                }

                agg = aggregator.apply(key, value, agg);
                final Windowed<K> sessionKey = new Windowed<>(key, mergedWindow);
                store.put(sessionKey, agg);
                tupleForwarder.maybeForward(sessionKey, agg, null);
            }
        }
    }

    private SessionWindow mergeSessionWindow(final SessionWindow one, final SessionWindow two) {
        final long start = one.start() < two.start() ? one.start() : two.start();
        final long end = one.end() > two.end() ? one.end() : two.end();
        return new SessionWindow(start, end);
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, Agg> view() {
        return new KTableValueGetterSupplier<Windowed<K>, Agg>() {
            @Override
            public KTableValueGetter<Windowed<K>, Agg> get() {
                return new KTableSessionWindowValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[] {storeName};
            }
        };
    }

    private class KTableSessionWindowValueGetter implements KTableValueGetter<Windowed<K>, Agg> {
        private SessionStore<K, Agg> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (SessionStore<K, Agg>) context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<Agg> get(final Windowed<K> key) {
            return ValueAndTimestamp.make(
                store.fetchSession(key.key(), key.window().start(), key.window().end()),
                key.window().end());
        }
    }

}
