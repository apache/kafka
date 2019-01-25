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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.metrics.Sensors;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KStreamWindowAggregate<K, V, Agg, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<Agg> initializer;
    private final Aggregator<? super K, ? super V, Agg> aggregator;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(final Windows<W> windows,
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

        private WindowStore<K, ValueAndTimestamp<Agg>> windowStore;
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
            windowStore = ((KStreamImpl.WindowStoreFacade) store).inner;
            tupleForwarder = new TupleForwarder<>(windowStore, context, new ForwardingCacheFlushListener<Windowed<K>, V>(context), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            if (key == null) {
                log.warn(
                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            // first get the matching windows
            final long timestamp = context().timestamp();
            final long closeTime = internalProcessorContext.streamTime() - windows.gracePeriodMs();

            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                final Long windowStart = entry.getKey();
                final long windowEnd = entry.getValue().end();
                if (windowEnd > closeTime) {
                    final ValueAndTimestamp<Agg> oldAggWithTimestamp = windowStore.fetch(key, windowStart);

                    final Agg oldAgg;
                    final long resultTimestamp;
                    if (oldAggWithTimestamp == null) {
                        oldAgg = initializer.apply();
                        resultTimestamp = context().timestamp();
                    } else {
                        oldAgg = oldAggWithTimestamp.value();
                        resultTimestamp = Math.max(context().timestamp(), oldAggWithTimestamp.timestamp());
                    }

                    final Agg newAgg = aggregator.apply(key, value, oldAgg);

                    // update the store with the new value
                    windowStore.put(key, ValueAndTimestamp.make(newAgg, resultTimestamp), windowStart);
                    tupleForwarder.maybeForward(new Windowed<>(key, entry.getValue()), newAgg, sendOldValues ? oldAgg : null, resultTimestamp);
                } else {
                    log.debug(
                        "Skipping record for expired window. key=[{}] topic=[{}] partition=[{}] offset=[{}] timestamp=[{}] window=[{},{}) expiration=[{}]",
                        key, context().topic(), context().partition(), context().offset(), context().timestamp(), windowStart, windowEnd, closeTime
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

        private ReadOnlyWindowStore<K, ValueAndTimestamp<Agg>> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            StateStore store = context.getStateStore(storeName);
            if (store instanceof WrappedStateStore) {
                store = ((WrappedStateStore) store).wrappedStore();
            }
            windowStore = ((KStreamImpl.WindowStoreFacade) store).inner;
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
