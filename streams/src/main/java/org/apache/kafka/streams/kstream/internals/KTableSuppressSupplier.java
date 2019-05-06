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

import org.apache.kafka.streams.kstream.internals.suppress.KTableSuppressProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class KTableSuppressSupplier<K, V> implements KTableProcessorSupplier<K, V, V>, ProcessorSupplier<K, Change<V>> {

    private final String storeName;
    private final KTableSuppressProcessor<K, V> suppressProcessor;

    public KTableSuppressSupplier(final KTableSuppressProcessor<K, V> suppressProcessor, final String storeName) {
        this.storeName = storeName;
        this.suppressProcessor = suppressProcessor;
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTableSuppressValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    @Override
    public void enableSendingOldValues() {
        throw new UnsupportedOperationException("enableSendingOldValues is not supported by KTableSuppressSupplier");
    }

    @Override
    public Processor<K, Change<V>> get() {
        return suppressProcessor;
    }

    private class KTableSuppressValueGetter implements KTableValueGetter<K, V> {

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

        @Override
        public void close() {
        }
    }
}