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

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class KTableAggregate<K, V, T> implements KTableProcessorSupplier<K, V, T> {

    private final String storeName;
    private final Initializer<T> initializer;
    private final Aggregator<K, V, T> add;
    private final Aggregator<K, V, T> remove;

    private boolean sendOldValues = false;

    public KTableAggregate(String storeName, Initializer<T> initializer, Aggregator<K, V, T> add, Aggregator<K, V, T> remove) {
        this.storeName = storeName;
        this.initializer = initializer;
        this.add = add;
        this.remove = remove;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableAggregateProcessor();
    }

    private class KTableAggregateProcessor extends AbstractProcessor<K, Change<V>> {

        private KeyValueStore<K, T> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);

            store = (KeyValueStore<K, T>) context.getStateStore(storeName);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(K key, Change<V> value) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable aggregate operator with state " + storeName + " should not be null.");

            T oldAgg = store.get(key);

            if (oldAgg == null)
                oldAgg = initializer.apply();

            T newAgg = oldAgg;

            // first try to remove the old value
            if (value.oldValue != null) {
                newAgg = remove.apply(key, value.oldValue, newAgg);
            }

            // then try to add the new new value
            if (value.newValue != null) {
                newAgg = add.apply(key, value.newValue, newAgg);
            }

            // update the store with the new value
            store.put(key, newAgg);

            // send the old / new pair
            if (sendOldValues)
                context().forward(key, new Change<>(newAgg, oldAgg));
            else
                context().forward(key, new Change<>(newAgg, null));
        }
    }

    @Override
    public KTableValueGetterSupplier<K, T> view() {

        return new KTableValueGetterSupplier<K, T>() {

            public KTableValueGetter<K, T> get() {
                return new KTableAggregateValueGetter();
            }

        };
    }

    private class KTableAggregateValueGetter implements KTableValueGetter<K, T> {

        private KeyValueStore<K, T> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<K, T>) context.getStateStore(storeName);
        }

        @Override
        public T get(K key) {
            return store.get(key);
        }

    }
}
