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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.metrics.Sensors;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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

        private SessionStore<K, ValueAndTimestamp<Agg>> store;
        private TupleForwarder<Windowed<K>, Agg> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private InternalProcessorContext internalProcessorContext;
        private Sensor lateRecordDropSensor;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            internalProcessorContext = (InternalProcessorContext) context;
            metrics = (StreamsMetricsImpl) context.metrics();
            lateRecordDropSensor = Sensors.lateRecordDropSensor(internalProcessorContext);

            StateStore store = context.getStateStore(storeName);
            if (store instanceof WrappedStateStore) {
                store = ((WrappedStateStore) store).wrappedStore();
            }
            this.store = ((SessionWindowedKStreamImpl.SessionStoreFacade) store).inner;
            tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context), sendOldValues);
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
                metrics.skippedRecordsSensor().record();
                return;
            }

            final long closeTime = internalProcessorContext.streamTime() - windows.gracePeriodMs();

            final long timestamp = context().timestamp();
            final List<KeyValue<Windowed<K>, ValueAndTimestamp<Agg>>> merged = new ArrayList<>();
            final SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            Agg agg = initializer.apply();
            long resultTimestamp = timestamp;

            try (
                final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iterator = store.findSessions(
                    key,
                    timestamp - windows.inactivityGap(),
                    timestamp + windows.inactivityGap()
                )
            ) {
                while (iterator.hasNext()) {
                    final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> next = iterator.next();
                    merged.add(next);
                    agg = sessionMerger.apply(key, agg, next.value.value());
                    resultTimestamp = Math.max(resultTimestamp, next.value.timestamp());
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow) next.key.window());
                }
            }

            if (mergedWindow.end() > closeTime) {
                if (!mergedWindow.equals(newSessionWindow)) {
                    for (final KeyValue<Windowed<K>, ValueAndTimestamp<Agg>> session : merged) {
                        store.remove(session.key);
                        tupleForwarder.maybeForward(
                            session.key,
                            null,
                            sendOldValues ? session.value.value() : null,
                            resultTimestamp);
                    }
                }

                agg = aggregator.apply(key, value, agg);
                final Windowed<K> sessionKey = new Windowed<>(key, mergedWindow);
                store.put(sessionKey, ValueAndTimestamp.make(agg, resultTimestamp));
                tupleForwarder.maybeForward(sessionKey, agg, null, resultTimestamp);
            } else {
                LOG.debug(
                    "Skipping record for expired window. key=[{}] topic=[{}] partition=[{}] offset=[{}] timestamp=[{}] window=[{},{}) expiration=[{}]",
                    key, context().topic(), context().partition(), context().offset(), context().timestamp(), mergedWindow.start(), mergedWindow.end(), closeTime
                );
                lateRecordDropSensor.record();
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
        private SessionStore<K, ValueAndTimestamp<Agg>> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            StateStore store = context.getStateStore(storeName);
            if (store instanceof WrappedStateStore) {
                store = ((WrappedStateStore) store).wrappedStore();
            }
            this.store = ((SessionWindowedKStreamImpl.SessionStoreFacade) store).inner;
        }

        @Override
        public ValueAndTimestamp<Agg> get(final Windowed<K> key) {
            try (final KeyValueIterator<Windowed<K>, ValueAndTimestamp<Agg>> iter
                     = store.findSessions(key.key(), key.window().end(), key.window().end())) {
                if (!iter.hasNext()) {
                    return null;
                }
                final ValueAndTimestamp<Agg> valueAndTimestamp = iter.next().value;
                if (iter.hasNext()) {
                    throw new ProcessorStateException(String.format("Iterator for key [%s] on session store has more than one value", key));
                }
                return valueAndTimestamp;
            }
        }

        @Override
        public void close() {
        }
    }

}
