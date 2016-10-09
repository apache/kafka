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

import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;

public class KStreamReduce<K, V> implements KStreamAggProcessorSupplier<K, K, V, V> {

    private final String storeName;
    private final Reducer<V> reducer;

    private boolean sendOldValues = false;
    private boolean forwardImmediately = false;

    public KStreamReduce(String storeName, Reducer<V> reducer) {
        this.storeName = storeName;
        this.reducer = reducer;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamReduceProcessor();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    @Override
    public void enableForwardImmediately() {
        forwardImmediately = true;
    }

    private class KStreamReduceProcessor extends AbstractProcessor<K, V> {

        private ProcessorContext context;
        private KeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            this.context = context;
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);

            if (!forwardImmediately) {
                ((CachedStateStore) store).setFlushListener(new ForwardingCacheFlushListener<K, V>(context, sendOldValues));
            }
        }


        @Override
        public void process(K key, V value) {
            // If the key is null we don't need to proceed
            if (key == null)
                return;

            V oldAgg = store.get(key);
            V newAgg = oldAgg;

            // try to add the new new value
            if (value != null) {
                if (newAgg == null) {
                    newAgg = value;
                } else {
                    newAgg = reducer.apply(newAgg, value);
                }
            }

            // update the store with the new value
            store.put(key, newAgg);

            if (forwardImmediately) {
                if (sendOldValues) {
                    context.forward(key, new Change<>(newAgg, oldAgg));
                } else {
                    context.forward(key, new Change<>(newAgg, null));
                }
            }
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KStreamReduceValueGetter();
            }

        };
    }

    private class KStreamReduceValueGetter implements KTableValueGetter<K, V> {

        private KeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
        }

        @Override
        public V get(K key) {
            return store.get(key);
        }
    }
}

