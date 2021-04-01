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

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Collection;

public class KTablePassThrough<K, V> implements KTableChangeProcessorSupplier<K, V, V, K, V> {
    private final Collection<KStreamAggProcessorSupplier> parents; //TODO change to aggregationprocessor
    private final String storeName;


    KTablePassThrough(final Collection<KStreamAggProcessorSupplier> parents, final String storeName) {
        this.parents = parents;
        this.storeName = storeName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
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
    public KTableValueAndTimestampGetterSupplier<K, V> view() {

        return new KTableValueAndTimestampGetterSupplier<K, V>() {

            public KTableValueAndTimestampGetter<K, V> get() {
                return new KTablePassThroughValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTablePassThroughProcessor extends ContextualProcessor<K, Change<V>, K, Change<V>> {
        @Override
        public void process(final Record<K, Change<V>> record) {
            context().forward(record);
        }
    }

    private class KTablePassThroughValueGetter implements KTableValueAndTimestampGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store;

        @Override
        public <KParent, VParent> void init(final ProcessorContext<KParent, VParent> context) {
            store = context.getStateStore(storeName);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return store.get(key);
        }

    }
}
