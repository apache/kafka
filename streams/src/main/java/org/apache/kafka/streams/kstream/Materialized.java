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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.DslSessionParams;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.DslWindowParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

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
 * <p>
 * <b>Correct Usage When Providing Serde:</b>
 * <p>
 * To configure both the name of the store and the {@link Serde} for the key and value,
 * you should use the following pattern:
 * <pre>{@code
 * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
 *     .withKeySerde(keySerde)
 *     .withValueSerde(valueSerde);
 * }</pre>
 * This ensures that the store name is retained while configuring the key and value serde.
 * <p>
 * <b>Warning:</b> If you use the ({@code static}) {@link Materialized#with(Serde, Serde)} method after calling
 * {@link Materialized#as(String)}, the instance created by {@link Materialized#as(String)} will be replaced by a new
 * {@link Materialized} instance, and any configuration set on the first instance (e.g., store name, logging settings)
 * will be lost.
 * <p>
 * For example, the following code is incorrect because it discards the configuration of the store name (calling {@code static}) methods on an object/instance is an anti-pattern):
 * <pre>{@code
 * // This will not work as expected:
 * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
 *     .with(keySerde, valueSerde);  // The store name "MyStoreName" is lost
 * }</pre>
 * Instead, use the proper pattern of chaining {@code withKeySerde} and {@code withValueSerde}.
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
    protected Duration retention;
    protected DslStoreSuppliers dslStoreSuppliers;

    // the built-in state store types
    public enum StoreType implements DslStoreSuppliers {
        ROCKS_DB(BuiltInDslStoreSuppliers.ROCKS_DB),
        IN_MEMORY(BuiltInDslStoreSuppliers.IN_MEMORY);

        private final DslStoreSuppliers delegate;

        StoreType(final DslStoreSuppliers delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            delegate.configure(configs);
        }

        @Override
        public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params) {
            return delegate.keyValueStore(params);
        }

        @Override
        public WindowBytesStoreSupplier windowStore(final DslWindowParams params) {
            return delegate.windowStore(params);
        }

        @Override
        public SessionBytesStoreSupplier sessionStore(final DslSessionParams params) {
            return delegate.sessionStore(params);
        }
    }

    private Materialized(final StoreSupplier<S> storeSupplier) {
        this.storeSupplier = storeSupplier;
    }

    private Materialized(final String storeName) {
        this.storeName = storeName;
    }

    private Materialized(final DslStoreSuppliers storeSuppliers) {
        this.dslStoreSuppliers = storeSuppliers;
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
        this.retention = materialized.retention;
        this.dslStoreSuppliers = materialized.dslStoreSuppliers;
    }

    /**
     * Materialize a {@link StateStore} with the given {@link DslStoreSuppliers}.
     *
     * @param storeSuppliers  the type of the state store
     * @param <K>             key type of the store
     * @param <V>             value type of the store
     * @param <S>             type of the {@link StateStore}
     * @return a new {@link Materialized} instance with the given storeName
     */
    public static <K, V, S extends StateStore> Materialized<K, V, S> as(final DslStoreSuppliers storeSuppliers) {
        Objects.requireNonNull(storeSuppliers, "store type can't be null");
        return new Materialized<>(storeSuppliers);
    }

    /**
     * Materialize a {@link StateStore} with the given name.
     * <p>
     * This method sets the name of the state store to be used during materialization. You can provide additional
     * configurations like key and value {@link Serde}s using {@link #withKeySerde(Serde)} and
     * {@link #withValueSerde(Serde)}.
     * <p>
     * <b>Warning:</b> Do not use {@link #with(Serde, Serde)} after calling this method, as it creates a new
     * {@link Materialized} instance, which discards the store name and any other configurations set earlier.
     * <p>
     * Correct usage:
     * <pre>{@code
     * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
     *     .withKeySerde(keySerde)
     *     .withValueSerde(valueSerde);
     * }</pre>
     * <p>
     * Incorrect usage (store name is lost):
     * <pre>{@code
     * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
     *     .with(keySerde, valueSerde);  // Store name is lost
     * }</pre>
     *
     * @param storeName  the name of the underlying {@link KTable} state store; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'.
     * @param <K>       key type of the store
     * @param <V>       value type of the store
     * @param <S>       type of the {@link StateStore}
     * @return a new {@link Materialized} instance with the given storeName
     */
    public static <K, V, S extends StateStore> Materialized<K, V, S> as(final String storeName) {
        Named.validate(storeName);
        return new Materialized<>(storeName);
    }

    /**
     * Materialize a {@link WindowStore} using the provided {@link WindowBytesStoreSupplier}.
     *
     * Important: Custom subclasses are allowed here, but they should respect the retention contract:
     * Window stores are required to retain windows at least as long as (window size + window grace period).
     * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
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
     * Important: Custom subclasses are allowed here, but they should respect the retention contract:
     * Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
     * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
     *
     * @param supplier the {@link SessionBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link Materialized} instance with the given supplier
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
     * <p>
     * Note: If this method is used after {@link #as(String)}, the original {@link Materialized} instance will be
     * replaced with a new instance, and any configuration on the first instance (e.g., store name) will be lost. To
     * configure both a store name and key/value serde, use {@link #withKeySerde(Serde)} and
     * {@link #withValueSerde(Serde)} instead.
     * <p>
     * Correct usage with {@link Serde}:
     * <pre>{@code
     * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
     *     .withKeySerde(keySerde)
     *     .withValueSerde(valueSerde);
     * }</pre>
     * <p>
     * Incorrect usage (store name will be lost):
     * <pre>{@code
     * Materialized.<KeyType, ValueType, StateStore>as("MyStoreName")
     *     .with(keySerde, valueSerde);  // Store name is lost
     * }</pre>
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
     *                   serde from configs will be used. If the serialized bytes is null for put operations,
     *                   it is treated as delete operation
     * @return itself
     */
    public Materialized<K, V, S> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    /**
     * Set the keySerde the materialized {@link StateStore} will use.
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

    /**
     * Configure retention period for window and session stores. Ignored for key/value stores.
     *
     * Overridden by pre-configured store suppliers
     * ({@link Materialized#as(SessionBytesStoreSupplier)} or {@link Materialized#as(WindowBytesStoreSupplier)}).
     *
     * Note that the retention period must be at least long enough to contain the windowed data's entire life cycle,
     * from window-start through window-end, and for the entire grace period. If not specified, the retention
     * period would be set as the window length (from window-start through window-end) plus the grace period.
     *
     * @param retention the retention time
     * @return itself
     * @throws IllegalArgumentException if retention is negative or can't be represented as {@code long milliseconds}
     */
    public Materialized<K, V, S> withRetention(final Duration retention) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retention, "retention");
        final long retentionMs = validateMillisecondDuration(retention, msgPrefix);

        if (retentionMs < 0) {
            throw new IllegalArgumentException("Retention must not be negative.");
        }
        this.retention = retention;
        return this;
    }

    /**
     * Set the type of the materialized {@link StateStore}.
     *
     * @param storeSuppliers  the store type {@link StoreType} to use.
     * @return itself
     * @throws IllegalArgumentException if store supplier is also pre-configured
     */
    public Materialized<K, V, S> withStoreType(final DslStoreSuppliers storeSuppliers) throws IllegalArgumentException {
        Objects.requireNonNull(storeSuppliers, "store type can't be null");
        if (storeSupplier != null) {
            throw new IllegalArgumentException("Cannot set store type when store supplier is pre-configured.");
        }
        this.dslStoreSuppliers = storeSuppliers;
        return this;
    }
}
