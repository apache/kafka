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

import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Map;

public class KStreamWindowReduce<K, V, W extends Window> implements KStreamAggProcessorSupplier<K, Windowed<K>, V, V> {

    private final String storeName;
    private final Windows<W> windows;
    private final Reducer<V> reducer;

    private boolean sendOldValues = false;

    KStreamWindowReduce(final Windows<W> windows,
                        final String storeName,
                        final Reducer<V> reducer) {
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
        private TupleForwarder<Windowed<K>, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            windowStore = (WindowStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(windowStore, context, new ForwardingCacheFlushListener<Windowed<K>, V>(context, sendOldValues), sendOldValues);
        }

        @Override
        public void process(final K key, final V value) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (key == null)
                return;

            // first get the matching windows
            final long timestamp = context().timestamp();
            final Map<Long, W> matchedWindows = windows.windowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            for (final Map.Entry<Long, W> entry : matchedWindows.entrySet()) {
                final V oldAgg = windowStore.fetch(key, entry.getKey());

                V newAgg;
                if (oldAgg == null) {
                    newAgg = value;
                } else {
                    newAgg = reducer.apply(oldAgg, value);
                }

                // update the store with the new value
                windowStore.put(key, newAgg, entry.getKey());
                tupleForwarder.maybeForward(new Windowed<>(key, entry.getValue()), newAgg, oldAgg);
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<Windowed<K>, V> view() {

        return new KTableValueGetterSupplier<Windowed<K>, V>() {

            public KTableValueGetter<Windowed<K>, V> get() {
                return new KStreamWindowReduceValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamWindowReduceValueGetter implements KTableValueGetter<Windowed<K>, V> {

        private WindowStore<K, V> windowStore;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            windowStore = (WindowStore<K, V>) context.getStateStore(storeName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(final Windowed<K> windowedKey) {
            K key = windowedKey.key();
            W window = (W) windowedKey.window();

            return windowStore.fetch(key, window.start());
        }
    }
}
