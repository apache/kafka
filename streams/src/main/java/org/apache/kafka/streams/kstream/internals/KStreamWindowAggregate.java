/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Iterator;
import java.util.Map;

public class KStreamWindowAggregate<K, V, T, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, T> {

    private final String storeName;
    private final Windows<W> windows;
    private final Initializer<T> initializer;
    private final Aggregator<K, V, T> aggregator;

    private boolean sendOldValues = false;

    public KStreamWindowAggregate(Windows<W> windows, String storeName, Initializer<T> initializer, Aggregator<K, V, T> aggregator) {
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

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            windowStore = (WindowStore<K, T>) context.getStateStore(storeName);
        }

        @Override
        public void process(K key, V value) {
            // if the key is null, we do not need proceed aggregating the record
            // the record with the table
            if (key == null)
                return;

            // first get the matching windows
            long timestamp = context().timestamp();
            Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            long timeFrom = Long.MAX_VALUE;
            long timeTo = Long.MIN_VALUE;

            // use range query on window store for efficient reads
            for (long windowStartMs : matchedWindows.keySet()) {
                timeFrom = windowStartMs < timeFrom ? windowStartMs : timeFrom;
                timeTo = windowStartMs > timeTo ? windowStartMs : timeTo;
            }

            WindowStoreIterator<T> iter = windowStore.fetch(key, timeFrom, timeTo);

            // for each matching window, try to update the corresponding key and send to the downstream
            while (iter.hasNext()) {
                KeyValue<Long, T> entry = iter.next();
                W window = matchedWindows.get(entry.key);

                if (window != null) {

                    T oldAgg = entry.value;

                    if (oldAgg == null)
                        oldAgg = initializer.apply();

                    // try to add the new new value (there will never be old value)
                    T newAgg = aggregator.apply(key, value, oldAgg);

                    // update the store with the new value
                    windowStore.put(key, newAgg, window.start());

                    // forward the aggregated change pair
                    if (sendOldValues)
                        context().forward(new Windowed<>(key, window), new Change<>(newAgg, oldAgg));
                    else
                        context().forward(new Windowed<>(key, window), new Change<>(newAgg, null));

                    matchedWindows.remove(entry.key);
                }
            }

            iter.close();

            // create the new window for the rest of unmatched window that do not exist yet
            for (long windowStartMs : matchedWindows.keySet()) {
                T oldAgg = initializer.apply();
                T newAgg = aggregator.apply(key, value, oldAgg);

                windowStore.put(key, newAgg, windowStartMs);

                // send the new aggregate pair
                if (sendOldValues)
                    context().forward(new Windowed<>(key, matchedWindows.get(windowStartMs)), new Change<>(newAgg, oldAgg));
                else
                    context().forward(new Windowed<>(key, matchedWindows.get(windowStartMs)), new Change<>(newAgg, null));
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, T> view() {

        return new KTableValueGetterSupplier<Windowed<K>, T>() {

            public KTableValueGetter<Windowed<K>, T> get() {
                return new KStreamWindowAggregateValueGetter();
            }

        };
    }

    private class KStreamWindowAggregateValueGetter implements KTableValueGetter<Windowed<K>, T> {

        private WindowStore<K, T> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            windowStore = (WindowStore<K, T>) context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(Windowed<K> windowedKey) {
            K key = windowedKey.key();
            W window = (W) windowedKey.window();

            // this iterator should contain at most one element
            Iterator<KeyValue<Long, T>> iter = windowStore.fetch(key, window.start(), window.start());

            return iter.hasNext() ? iter.next().value : null;
        }

    }
}
