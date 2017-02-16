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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractStream<K> {

    protected final KStreamBuilder topology;
    protected final String name;
    protected final Set<String> sourceNodes;

    AbstractStream(final KStreamBuilder topology, String name, final Set<String> sourceNodes) {
        if (sourceNodes == null || sourceNodes.isEmpty()) {
            throw new IllegalArgumentException("parameter <sourceNodes> must not be null or empty");
        }

        this.topology = topology;
        this.name = name;
        this.sourceNodes = sourceNodes;
    }

    Set<String> ensureJoinableWith(final AbstractStream<K> other) {
        Set<String> allSourceNodes = new HashSet<>();
        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(other.sourceNodes);

        topology.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    static <T2, T1, R> ValueJoiner<T2, T1, R> reverseJoiner(final ValueJoiner<T1, T2, R> joiner) {
        return new ValueJoiner<T2, T1, R>() {
            @Override
            public R apply(T2 value2, T1 value1) {
                return joiner.apply(value1, value2);
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <T, K>  StateStoreSupplier<KeyValueStore> keyValueStore(final Serde<K> keySerde,
                                                                   final Serde<T> aggValueSerde,
                                                                   final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        return storeFactory(keySerde, aggValueSerde, storeName).build();
    }

    @SuppressWarnings("unchecked")
    static  <W extends Window, T, K> StateStoreSupplier<WindowStore> windowedStore(final Serde<K> keySerde,
                                                                                   final Serde<T> aggValSerde,
                                                                                   final Windows<W> windows,
                                                                                   final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        return storeFactory(keySerde, aggValSerde, storeName)
                .windowed(windows.size(), windows.maintainMs(), windows.segments, false)
                .build();
    }

    static  <T, K> Stores.PersistentKeyValueFactory<K, T> storeFactory(final Serde<K> keySerde,
                                                                       final Serde<T> aggValueSerde,
                                                                       final String storeName) {
        return Stores.create(storeName)
                .withKeys(keySerde)
                .withValues(aggValueSerde)
                .persistent()
                .enableCaching();
    }


}
