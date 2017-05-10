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

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.RichValueJoiner;
import org.apache.kafka.streams.kstream.RichInitializer;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.RichAggregator;
import org.apache.kafka.streams.kstream.Aggregator;
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

    String getOrCreateName(final String queryableStoreName, final String prefix) {
        final String returnName = queryableStoreName != null ? queryableStoreName : topology.newStoreName(prefix);
        Topic.validate(returnName);
        return returnName;
    }

    static <K, T2, T1, R> ValueJoinerWithKey<K, T2, T1, R> reverseJoiner(final ValueJoinerWithKey<K, T1, T2, R> valueJoinerWithKey) {
        return new ValueJoinerWithKey<K, T2, T1, R>() {
            @Override
            public R apply(K key, T2 value2, T1 value1) {
                return valueJoinerWithKey.apply(key, value1, value2);
            }
        };
    }

    static <K, T1, T2, R> RichValueJoiner<K, T1, T2, R> convertToRichValueJoiner(final ValueJoinerWithKey<K, T1, T2, R> valueJoinerWithKey) {
        Objects.requireNonNull(valueJoinerWithKey, "valueJoiner can't be null");
        if (valueJoinerWithKey instanceof RichValueJoiner) {
            return (RichValueJoiner<K, T1, T2, R>) valueJoinerWithKey;
        } else {
            return new RichValueJoiner<K, T1, T2, R>() {
                @Override
                public void init() {}

                @Override
                public void close() {}

                @Override
                public R apply(K key, T1 value1, T2 value2) {
                    return valueJoinerWithKey.apply(key, value1, value2);
                }
            };
        }
    }

    static <K, T1, T2, R> ValueJoinerWithKey<K, T1, T2, R> convertToValueJoinerWithKey(final ValueJoiner<T1, T2, R> valueJoiner) {
        Objects.requireNonNull(valueJoiner, "valueJoiner can't be null");
        return new ValueJoinerWithKey<K, T1, T2, R>() {
            @Override
            public R apply(K key, T1 value1, T2 value2) {
                return valueJoiner.apply(value1, value2);
            }
        };
    }

    static <VA> RichInitializer<VA> checkAndMaybeConvertToRichInitializer(final Initializer<VA> initializer) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        if (initializer instanceof RichInitializer) {
            return (RichInitializer<VA>) initializer;
        } else {
            return new RichInitializer<VA>() {
                @Override
                public VA apply() {
                    return initializer.apply();
                }

                @Override
                public void init() {}

                @Override
                public void close() {}
            };
        }
    }

    static <K, V, VA> RichAggregator<K, V, VA> checkAndMaybeConvertToRichAggregator(final Aggregator<K, V, VA> aggregator) {
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        if (aggregator instanceof RichAggregator) {
            return (RichAggregator<K, V, VA>) aggregator;
        } else {
            return new RichAggregator<K, V, VA>() {
                @Override
                public VA apply(K key, V value, VA aggregate) {
                    return aggregator.apply(key, value, aggregate);
                }

                @Override
                public void init() {}

                @Override
                public void close() {}
            };
        }
    }


    @SuppressWarnings("unchecked")
    static <T, K>  StateStoreSupplier<KeyValueStore> keyValueStore(final Serde<K> keySerde,
                                                                   final Serde<T> aggValueSerde,
                                                                   final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Topic.validate(storeName);
        return storeFactory(keySerde, aggValueSerde, storeName).build();
    }

    @SuppressWarnings("unchecked")
    static  <W extends Window, T, K> StateStoreSupplier<WindowStore> windowedStore(final Serde<K> keySerde,
                                                                                   final Serde<T> aggValSerde,
                                                                                   final Windows<W> windows,
                                                                                   final String storeName) {
        Objects.requireNonNull(storeName, "storeName can't be null");
        Topic.validate(storeName);
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
