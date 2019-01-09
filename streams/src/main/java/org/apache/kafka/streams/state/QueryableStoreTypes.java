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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

/**
 * Provides access to the {@link QueryableStoreType}s provided with {@link KafkaStreams}.
 * These can be used with {@link KafkaStreams#store(String, QueryableStoreType)}.
 * To access and query the {@link StateStore}s that are part of a {@link Topology}.
 */
public final class QueryableStoreTypes {

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.KeyValueStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyKeyValueStore<K, V>> keyValueStore() {
        return new KeyValueStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.WindowStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStore() {
        return new WindowStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlySessionStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.SessionStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlySessionStore<K, V>> sessionStore() {
        return new SessionStoreType<>();
    }

    private static abstract class QueryableStoreTypeMatcher<T> implements QueryableStoreType<T> {

        private final Class matchTo;

        QueryableStoreTypeMatcher(final Class matchTo) {
            this.matchTo = matchTo;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean accepts(final StateStore stateStore) {
            return matchTo.isAssignableFrom(stateStore.getClass());
        }
    }

    static class KeyValueStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>> {
        KeyValueStoreType() {
            super(ReadOnlyKeyValueStore.class);
        }

        @Override
        public ReadOnlyKeyValueStore<K, V> create(final StateStoreProvider storeProvider,
                                                  final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }

    }

    static class WindowStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, V>> {
        WindowStoreType() {
            super(ReadOnlyWindowStore.class);
        }

        @Override
        public ReadOnlyWindowStore<K, V> create(final StateStoreProvider storeProvider,
                                                final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    static class SessionStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlySessionStore<K, V>> {
        SessionStoreType() {
            super(ReadOnlySessionStore.class);
        }
        @Override
        public ReadOnlySessionStore<K, V> create(final StateStoreProvider storeProvider,
                                                 final String storeName) {
            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }
}
