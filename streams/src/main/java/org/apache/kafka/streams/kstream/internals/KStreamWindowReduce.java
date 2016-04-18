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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Reducer;
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

public class KStreamWindowReduce<K, V, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, V> {

    private final String storeName;
    private final Windows<W> windows;
    private final Reducer<V> reducer;

    private boolean sendOldValues = false;

    public KStreamWindowReduce(Windows<W> windows, String storeName, Reducer<V> reducer) {
        this.windows = windows;
        this.storeName = storeName;
        this.reducer = reducer;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamWindowReduceProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KStreamWindowReduceProcessor extends AbstractProcessor<K, V> {

        private WindowStore<K, V> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            windowStore = (WindowStore<K, V>) context.getStateStore(storeName);
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

            WindowStoreIterator<V> iter = windowStore.fetch(key, timeFrom, timeTo);

            // for each matching window, try to update the corresponding key and send to the downstream
            while (iter.hasNext()) {
                KeyValue<Long, V> entry = iter.next();
                W window = matchedWindows.get(entry.key);

                if (window != null) {

                    V oldAgg = entry.value;
                    V newAgg = oldAgg;

                    // try to add the new new value (there will never be old value)
                    if (newAgg == null) {
                        newAgg = value;
                    } else {
                        newAgg = reducer.apply(newAgg, value);
                    }

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
                windowStore.put(key, value, windowStartMs);

                // send the new aggregate pair (there will be no old value)
                context().forward(new Windowed<>(key, matchedWindows.get(windowStartMs)), new Change<>(value, null));
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, V> view() {

        return new KTableValueGetterSupplier<Windowed<K>, V>() {

            public KTableValueGetter<Windowed<K>, V> get() {
                return new KStreamAggregateValueGetter();
            }

        };
    }

    private class KStreamAggregateValueGetter implements KTableValueGetter<Windowed<K>, V> {

        private WindowStore<K, V> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            windowStore = (WindowStore<K, V>) context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(Windowed<K> windowedKey) {
            K key = windowedKey.key();
            W window = (W) windowedKey.window();

            // this iterator should only contain one element
            Iterator<KeyValue<Long, V>> iter = windowStore.fetch(key, window.start(), window.start());

            return iter.next().value;
        }

    }
}
