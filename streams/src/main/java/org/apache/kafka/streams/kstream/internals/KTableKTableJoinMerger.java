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
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreWrapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_NOT_PUT;
import static org.apache.kafka.streams.state.internals.KeyValueStoreWrapper.PUT_RETURN_CODE_IS_LATEST;

public class KTableKTableJoinMerger<K, V> implements KTableProcessorSupplier<K, V, K, V> {

    private final KTableProcessorSupplier<K, ?, K, V> parent1;
    private final KTableProcessorSupplier<K, ?, K, V> parent2;
    private final String queryableName;
    private final StoreFactory storeFactory;
    private boolean sendOldValues = false;

    KTableKTableJoinMerger(final KTableProcessorSupplier<K, ?, K, V> parent1,
                           final KTableProcessorSupplier<K, ?, K, V> parent2,
                           final String queryableName,
                           final StoreFactory storeFactory) {
        this.parent1 = parent1;
        this.parent2 = parent2;
        this.queryableName = queryableName;
        this.storeFactory = storeFactory;
    }

    public String queryableName() {
        return queryableName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
        return new KTableKTableJoinMergeProcessor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return storeFactory == null
                ? null
                : Set.of(new StoreFactory.FactoryWrappingStoreBuilder<>(storeFactory));
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
                    return stores.toArray(new String[0]);
                }
            };
        }
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Table-table joins require upstream materialization:
        parent1.enableSendingOldValues(true);
        parent2.enableSendingOldValues(true);
        sendOldValues = true;
        return true;
    }

    public static <K, V> KTableKTableJoinMerger<K, V> of(final KTableProcessorSupplier<K, ?, K, V> parent1,
                                                         final KTableProcessorSupplier<K, ?, K, V> parent2) {
        return of(parent1, parent2, null, null);
    }

    public static <K, V> KTableKTableJoinMerger<K, V> of(final KTableProcessorSupplier<K, ?, K, V> parent1,
                                                         final KTableProcessorSupplier<K, ?, K, V> parent2,
                                                         final String queryableName,
                                                         final StoreFactory stores) {
        return new KTableKTableJoinMerger<>(parent1, parent2, queryableName, stores);
    }

    private class KTableKTableJoinMergeProcessor extends ContextualProcessor<K, Change<V>, K, Change<V>> {
        private KeyValueStoreWrapper<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            if (queryableName != null) {
                store = new KeyValueStoreWrapper<>(context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store.store(),
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            if (queryableName != null) {
                final long putReturnCode = store.put(record.key(), record.value().newValue, record.timestamp());
                // if not put to store, do not forward downstream either
                if (putReturnCode != PUT_RETURN_CODE_NOT_PUT) {
                    tupleForwarder.maybeForward(record.withValue(new Change<>(record.value().newValue, record.value().oldValue, putReturnCode == PUT_RETURN_CODE_IS_LATEST)));
                }
            } else {
                if (sendOldValues) {
                    context().forward(record);
                } else {
                    context().forward(record.withValue(new Change<>(record.value().newValue, null, record.value().isLatest)));
                }
            }
        }
    }
}
