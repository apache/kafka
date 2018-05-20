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

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KStreamWindowAggregate<K, V, T, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, T> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamWindowAggregate.class);

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<T> initializer;
    private final Aggregator<? super K, ? super V, T> aggregator;

    private boolean sendOldValues = false;

    KStreamWindowAggregate(final Windows<W> windows,
                           final String storeName,
                           final Initializer<T> initializer,
                           final Aggregator<? super K, ? super V, T> aggregator) {
        this.windows = windows;
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamWindowAggregateProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamWindowAggregateProcessor extends AbstractProcessor<K, V> {

        private WindowStore<K, T> windowStore;
        private TupleForwarder<Windowed<K>, T> tupleForwarder;
        private StreamsMetricsImpl metrics;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();

            windowStore = (WindowStore<K, T>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(windowStore, context, new ForwardingCacheFlushListener<Windowed<K>, V>(context, sendOldValues), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, we do not need proceed aggregating the record
            // the record with the table
            if (key == null) {
                LOG.warn(
                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            // first get the matching windows
            final long timestamp = context().timestamp();
            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                T oldAgg = windowStore.fetch(key, entry.getKey());

                if (oldAgg == null) {
                    oldAgg = initializer.apply();
                }

                final T newAgg = aggregator.apply(key, value, oldAgg);

                // update the store with the new value
                windowStore.put(key, newAgg, entry.getKey());
                tupleForwarder.maybeForward(new Windowed<>(key, entry.getValue()), newAgg, oldAgg);
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, T> view() {

        return new KTableValueGetterSupplier<Windowed<K>, T>() {

            public KTableValueGetter<Windowed<K>, T> get() {
                return new KStreamWindowAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamWindowAggregateValueGetter implements KTableValueGetter<Windowed<K>, T> {

        private WindowStore<K, T> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            windowStore = (WindowStore<K, T>) context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(final Windowed<K> windowedKey) {
            final K key = windowedKey.key();
            final W window = (W) windowedKey.window();

            return windowStore.fetch(key, window.start());
        }

        @Override
        public void close() {
        }
    }
}
