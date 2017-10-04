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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Used to describe how a {@link StateStore} should be materialized.
 * You can either provide a custom {@link StateStore} backend through one of the provided methods accepting a supplier
 * or use the default RocksDB backends by providing just a store name.
 * <p>
 * For example, you can read a topic as {@link KTable} and force a state store materialization to access the content
 * via Interactive Queries API:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KTable<Integer, Integer> table = builder.table(
 *   "topicName",
 *   Materialized.as("queryable-store-name"));
 * }</pre>
 *
 * @param <K> type of record key
 * @param <V> type of record value
 * @param <S> type of state store (note: state stores always have key/value types {@code <Bytes,byte[]>}
 *
 * @see org.apache.kafka.streams.state.Stores
 */
public class Materialized<K, V, S extends StateStore> {
    protected StoreSupplier<S> storeSupplier;
    protected String storeName;
    protected Serde<V> valueSerde;
    protected Serde<K> keySerde;
    protected boolean loggingEnabled = true;
    protected boolean cachingEnabled = true;
    protected Map<String, String> topicConfig = new HashMap<>();

    private Materialized(final StoreSupplier<S> storeSupplier) {
        this.storeSupplier = storeSupplier;
    }

    private Materialized(final String storeName) {
        this.storeName = storeName;
    }

    /**
     * Copy constructor.
     * @param materialized  the {@link Materialized} instance to copy.
     */
    protected Materialized(final Materialized<K, V, S> materialized) {
        this.storeSupplier = materialized.storeSupplier;
        this.storeName = materialized.storeName;
        this.keySerde = materialized.keySerde;
        this.valueSerde = materialized.valueSerde;
        this.loggingEnabled = materialized.loggingEnabled;
        this.cachingEnabled = materialized.cachingEnabled;
        this.topicConfig = materialized.topicConfig;
    }

    /**
     * Materialize a {@link StateStore} with the given name.
     *
     * @param storeName  the name of the underlying {@link KTable} state store; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'.
     * @param <K>       key type of the store
     * @param <V>       value type of the store
     * @param <S>       type of the {@link StateStore}
     * @return a new {@link Materialized} instance with the given storeName
     */
    public static <K, V, S extends StateStore> Materialized<K, V, S> as(final String storeName) {
        Topic.validate(storeName);
        return new Materialized<>(storeName);
    }

    /**
     * Materialize a {@link WindowStore} using the provided {@link WindowBytesStoreSupplier}.
     *
     * @param supplier the {@link WindowBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link Materialized} instance with the given supplier
     */
    public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> as(final WindowBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new Materialized<>(supplier);
    }

    /**
     * Materialize a {@link SessionStore} using the provided {@link SessionBytesStoreSupplier}.
     *
     * @param supplier the {@link SessionBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link Materialized} instance with the given sup
     * plier
     */
    public static <K, V> Materialized<K, V, SessionStore<Bytes, byte[]>> as(final SessionBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new Materialized<>(supplier);
    }

    /**
     * Materialize a {@link KeyValueStore} using the provided {@link KeyValueBytesStoreSupplier}.
     *
     * @param supplier the {@link KeyValueBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link Materialized} instance with the given supplier
     */
    public static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> as(final KeyValueBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new Materialized<>(supplier);
    }

    /**
     * Materialize a {@link StateStore} with the provided key and value {@link Serde}s.
     * An internal name will be used for the store.
     *
     * @param keySerde      the key {@link Serde} to use. If the {@link Serde} is null, then the default key
     *                      serde from configs will be used
     * @param valueSerde    the value {@link Serde} to use. If the {@link Serde} is null, then the default value
     *                      serde from configs will be used
     * @param <K>           key type
     * @param <V>           value type
     * @param <S>           store type
     * @return a new {@link Materialized} instance with the given key and value serdes
     */
    public static <K, V, S extends StateStore> Materialized<K, V, S> with(final Serde<K> keySerde,
                                                                          final Serde<V> valueSerde) {
        return new Materialized<K, V, S>((String) null).withKeySerde(keySerde).withValueSerde(valueSerde);
    }

    /**
     * Set the valueSerde the materialized {@link StateStore} will use.
     *
     * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
     *                   serde from configs will be used
     * @return itself
     */
    public Materialized<K, V, S> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    /**
     * Set the keySerde the materialize {@link StateStore} will use.
     * @param keySerde  the key {@link Serde} to use. If the {@link Serde} is null, then the default key
     *                  serde from configs will be used
     * @return itself
     */
    public Materialized<K, V, S> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    /**
     * Indicates that a changelog should be created for the store. The changelog will be created
     * with the provided configs.
     * <p>
     * Note: Any unrecognized configs will be ignored.
     * @param config    any configs that should be applied to the changelog
     * @return itself
     */
    public Materialized<K, V, S> withLoggingEnabled(final Map<String, String> config) {
        loggingEnabled = true;
        this.topicConfig = config;
        return this;
    }

    /**
     * Disable change logging for the materialized {@link StateStore}.
     * @return itself
     */
    public Materialized<K, V, S> withLoggingDisabled() {
        loggingEnabled = false;
        this.topicConfig.clear();
        return this;
    }

    /**
     * Enable caching for the materialized {@link StateStore}.
     * @return itself
     */
    public Materialized<K, V, S> withCachingEnabled() {
        cachingEnabled = true;
        return this;
    }

    /**
     * Disable caching for the materialized {@link StateStore}.
     * @return itself
     */
    public Materialized<K, V, S> withCachingDisabled() {
        cachingEnabled = false;
        return this;
    }

}
