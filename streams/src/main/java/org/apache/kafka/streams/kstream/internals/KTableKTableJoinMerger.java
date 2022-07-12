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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KTableKTableJoinMerger<K, V> implements KTableProcessorSupplier<K, V, K, V> {

    private final KTableProcessorSupplier<K, ?, K, V> parent1;
    private final KTableProcessorSupplier<K, ?, K, V> parent2;
    private final String queryableName;
    private boolean sendOldValues = false;

    KTableKTableJoinMerger(final KTableProcessorSupplier<K, ?, K, V> parent1,
                           final KTableProcessorSupplier<K, ?, K, V> parent2,
                           final String queryableName) {
        this.parent1 = parent1;
        this.parent2 = parent2;
        this.queryableName = queryableName;
    }

    public String getQueryableName() {
        return queryableName;
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
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
        return of(parent1, parent2, null);
    }

    public static <K, V> KTableKTableJoinMerger<K, V> of(final KTableProcessorSupplier<K, ?, K, V> parent1,
                                                         final KTableProcessorSupplier<K, ?, K, V> parent2,
                                                         final String queryableName) {
        return new KTableKTableJoinMerger<>(parent1, parent2, queryableName);
    }

    private class KTableKTableJoinMergeProcessor extends ContextualProcessor<K, Change<V>, K, Change<V>> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            if (queryableName != null) {
                store = (TimestampedKeyValueStore<K, V>) context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            if (queryableName != null) {
                store.put(record.key(), ValueAndTimestamp.make(record.value().newValue, record.timestamp()));
                tupleForwarder.maybeForward(record);
            } else {
                if (sendOldValues) {
                    context().forward(record);
                } else {
                    context().forward(record.withValue(new Change<>(record.value().newValue, null)));
                }
            }
        }
    }
}
