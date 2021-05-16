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

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;

public class KTablePassThrough<K, V> implements KTableProcessorSupplier<K, V, V> {
    private final Collection<KStreamAggProcessorSupplier> parents;
    private final String storeName;


    KTablePassThrough(final Collection<KStreamAggProcessorSupplier> parents, final String storeName) {
        this.parents = parents;
        this.storeName = storeName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTablePassThroughProcessor();
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Aggregation requires materialization so we will always enable sending old values
        for (final KStreamAggProcessorSupplier parent : parents) {
            parent.enableSendingOldValues();
        }
        return true;
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTablePassThroughValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTablePassThroughProcessor extends AbstractProcessor<K, Change<V>> {
        @Override
        public void process(final K key, final Change<V> value) {
            context().forward(key, value);
        }
    }

    private class KTablePassThroughValueGetter implements KTableValueGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return store.get(key);
        }

    }
}
