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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class KTableAggregate<K, V, T> implements ProcessorSupplier<K, Change<V>> {

    private final String storeName;
    private final Aggregator<K, V, T> aggregator;

    KTableAggregate(String storeName, Aggregator<K, V, T> aggregator) {
        this.storeName = storeName;
        this.aggregator = aggregator;
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

        @Override
        public void process(K key, Change<V> value) {
            T newAgg = null;

            // old value
            if (value.oldValue != null) {
                newAgg = aggregator.remove(key, value.newValue, store.get(key));
                store.put(key, newAgg);
            }

            // new value
            if (value.newValue != null) {
                newAgg = aggregator.add(key, value.newValue, store.get(key));
                store.put(key, newAgg);
            }

            if (newAgg != null) {
                context().forward(key, newAgg);
            }
        }
    }
}
