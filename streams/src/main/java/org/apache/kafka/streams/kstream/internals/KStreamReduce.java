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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KStreamReduce<K, V> implements KStreamAggProcessorSupplier<K, K, V, V> {
    private static final Logger LOG = LoggerFactory.getLogger(KStreamReduce.class);

    private final String storeName;
    private final Reducer<V> reducer;

    private boolean sendOldValues = false;

    KStreamReduce(final String storeName, final Reducer<V> reducer) {
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

    private class KStreamReduceProcessor extends AbstractProcessor<K, V> {

        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();

            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);
        }


        @Override
        public void process(final K key, final V value) {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                metrics.skippedRecordsSensor().record();
                return;
            }

            final V oldAgg = store.get(key);
            V newAgg = oldAgg;

            // try to add the new value
            if (newAgg == null) {
                newAgg = value;
            } else {
                newAgg = reducer.apply(newAgg, value);
            }

            // update the store with the new value
            store.put(key, newAgg);
            tupleForwarder.maybeForward(key, newAgg, oldAgg);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KStreamReduceValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KStreamReduceValueGetter implements KTableValueGetter<K, V> {

        private KeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
        }

        @Override
        public V get(final K key) {
            return store.get(key);
        }
    }
}

