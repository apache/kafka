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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlySessionStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides access to the {@link QueryableStoreType}s provided with {@link KafkaStreams}.
 * These can be used with {@link KafkaStreams#store(StoreQueryParameters)}.
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
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.TimestampedKeyValueStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> timestampedKeyValueStore() {
        return new TimestampedKeyValueStoreType<>();
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
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore ReadOnlyWindowStore<K, ValueAndTimestamp<V>>}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.TimestampedWindowStoreType}
     */
    public static <K, V> QueryableStoreType<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>> timestampedWindowStore() {
        return new TimestampedWindowStoreType<>();
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

        private final Set<Class> matchTo;

        QueryableStoreTypeMatcher(final Set<Class> matchTo) {
            this.matchTo = matchTo;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean accepts(final StateStore stateStore) {
            for (final Class matchToClass : matchTo) {
                if (!matchToClass.isAssignableFrom(stateStore.getClass())) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class KeyValueStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>> {

        KeyValueStoreType() {
            super(Collections.singleton(ReadOnlyKeyValueStore.class));
        }

        @Override
        public ReadOnlyKeyValueStore<K, V> create(final StateStoreProvider storeProvider,
                                                  final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }

    }

    private static class TimestampedKeyValueStoreType<K, V>
        extends QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> {

        TimestampedKeyValueStoreType() {
            super(new HashSet<>(Arrays.asList(
                TimestampedKeyValueStore.class,
                ReadOnlyKeyValueStore.class)));
        }

        @Override
        public ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> create(final StateStoreProvider storeProvider,
                                                                     final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }
    }

    public static class WindowStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, V>> {

        WindowStoreType() {
            super(Collections.singleton(ReadOnlyWindowStore.class));
        }

        @Override
        public ReadOnlyWindowStore<K, V> create(final StateStoreProvider storeProvider,
                                                final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    private static class TimestampedWindowStoreType<K, V>
        extends QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>> {

        TimestampedWindowStoreType() {
            super(new HashSet<>(Arrays.asList(
                TimestampedWindowStore.class,
                ReadOnlyWindowStore.class)));
        }

        @Override
        public ReadOnlyWindowStore<K, ValueAndTimestamp<V>> create(final StateStoreProvider storeProvider,
                                                                   final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    public static class SessionStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlySessionStore<K, V>> {

        SessionStoreType() {
            super(Collections.singleton(ReadOnlySessionStore.class));
        }

        @Override
        public ReadOnlySessionStore<K, V> create(final StateStoreProvider storeProvider,
                                                 final String storeName) {
            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }

}