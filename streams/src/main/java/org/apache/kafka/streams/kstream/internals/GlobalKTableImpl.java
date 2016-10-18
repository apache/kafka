/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Objects;


public class GlobalKTableImpl<K, V> implements GlobalKTable<K, V> {

    private final KTableValueGetterSupplier<K, V> valueGetterSupplier;
    private final ReadOnlyKeyValueStore<K, V> store;
    private final KStreamBuilder topologyBuilder;

    public GlobalKTableImpl(final KTableValueGetterSupplier<K, V> valueGetterSupplier,
                            final ReadOnlyKeyValueStore<K, V> store,
                            final KStreamBuilder topologyBuilder) {
        this.valueGetterSupplier = valueGetterSupplier;
        this.store = store;
        this.topologyBuilder = topologyBuilder;
    }

    @Override
    public <K1, V1, R> GlobalKTable<K, R> join(final GlobalKTable<K1, V1> otherTable,
                                               final KeyValueMapper<K, V, K1> keyMapper,
                                               final ValueJoiner<V, V1, R> joiner,
                                               final String queryableViewName) {

        return createView((GlobalKTableImpl<K1, V1>) otherTable, keyMapper, joiner, queryableViewName, false);
    }

    @Override
    public <K1, V1, R> GlobalKTable<K, R> leftJoin(final GlobalKTable<K1, V1> otherTable,
                                                   final KeyValueMapper<K, V, K1> keyMapper,
                                                   final ValueJoiner<V, V1, R> joiner,
                                                   final String queryableViewName) {

        return createView((GlobalKTableImpl<K1, V1>) otherTable, keyMapper, joiner, queryableViewName, true);
    }

    private <K1, V1, R> GlobalKTable<K, R> createView(final GlobalKTableImpl<K1, V1> otherTable,
                                                      final KeyValueMapper<K, V, K1> keyMapper,
                                                      final ValueJoiner<V, V1, R> joiner,
                                                      final String queryableViewName,
                                                      final boolean leftJoin) {
        Objects.requireNonNull(otherTable, "otherTable can't be null");
        Objects.requireNonNull(keyMapper, "keyMapper can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(queryableViewName, "queryableViewName can't be null");
        final KeyValueStoreJoinView<K, V, K1, V1, R> view = new KeyValueStoreJoinView<>(queryableViewName,
                                                                                        this.store,
                                                                                        otherTable.valueGetterSupplier.get(),
                                                                                        keyMapper,
                                                                                        joiner,
                                                                                        leftJoin);
        topologyBuilder.addGlobalStore(view);

        return new GlobalKTableImpl<>(new KTableSourceValueGetterSupplier<K, R>(queryableViewName),
                                      view,
                                      topologyBuilder);
    }


    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        return valueGetterSupplier;
    }


}
