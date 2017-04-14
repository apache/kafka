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

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class KStreamCogroup<K, V> implements KStreamAggProcessorSupplier<K, K, V, V> {

    private final String storeName;
    private final List<KStreamAggregate> aggregates;

    public KStreamCogroup(String storeName, KStreamAggregate... aggregates) {
        this.storeName = storeName;
        this.aggregates = Arrays.asList(aggregates);
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamPassThroughProcessor<>();
    }
    
    private static final class KStreamPassThroughProcessor<K, V> extends AbstractProcessor<K, V> {
        @Override
        public void process(K key, V value) {
            context().forward(key, value);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KStreamAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }
    
    private class KStreamAggregateValueGetter implements KTableValueGetter<K, V> {

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

    @Override
    public void enableSendingOldValues() {
        for (KStreamAggregate aggregate : aggregates) {
            aggregate.enableSendingOldValues();
        }
    }

}
