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
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class KTableKTableJoinMerger<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableProcessorSupplier<K, ?, V> parent1;
    private final KTableProcessorSupplier<K, ?, V> parent2;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableKTableJoinMerger(final KTableProcessorSupplier<K, ?, V> parent1,
                           final KTableProcessorSupplier<K, ?, V> parent2,
                           final String queryableName) {
        this.parent1 = parent1;
        this.parent2 = parent2;
        this.queryableName = queryableName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableKTableJoinMergeProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        // if the result KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply join on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueGetterSupplier<>(queryableName);
        } else {
            return new KTableValueGetterSupplier<K, V>() {

                public KTableValueGetter<K, V> get() {
                    return parent1.view().get();
                }

                @Override
                public String[] storeNames() {
                    final String[] storeNames1 = parent1.view().storeNames();
                    final String[] storeNames2 = parent2.view().storeNames();
                    final Set<String> stores = new HashSet<>(storeNames1.length + storeNames2.length);
                    Collections.addAll(stores, storeNames1);
                    Collections.addAll(stores, storeNames2);
                    return stores.toArray(new String[stores.size()]);
                }
            };
        }
    }

    @Override
    public void enableSendingOldValues() {
        parent1.enableSendingOldValues();
        parent2.enableSendingOldValues();
        sendOldValues = true;
    }

    private class KTableKTableJoinMergeProcessor extends AbstractProcessor<K, Change<V>> {
        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            if (queryableName != null) {
                store = (KeyValueStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TupleForwarder<>(store, context,
                    new ForwardingCacheFlushListener<K, V>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final K key, final Change<V> value) {
            if (queryableName != null) {
                store.put(key, value.newValue);
                tupleForwarder.maybeForward(key, value.newValue, sendOldValues ? value.oldValue : null);
            } else {
                if (sendOldValues) {
                    context().forward(key, value);
                } else {
                    context().forward(key, new Change<>(value.newValue, null));
                }
            }
        }
    }
}
