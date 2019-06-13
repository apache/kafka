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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

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
 * @see org.apache.kafka.streams.state.Stores
 */
public class MaterializedKV<K, V> {
    protected KeyValueBytesStoreSupplier storeSupplier;
    protected String storeName;
    protected Serde<V> valueSerde;
    protected Serde<K> keySerde;
    protected boolean loggingEnabled = true;
    protected boolean cachingEnabled = true;
    protected Map<String, String> topicConfig = new HashMap<>();
    protected Duration retention;

    private MaterializedKV(final KeyValueBytesStoreSupplier storeSupplier) {
        this.storeSupplier = storeSupplier;
    }

    private MaterializedKV(final WindowBytesStoreSupplier storeSupplier) {
        this.storeSupplier = adapt(storeSupplier);
    }

    private MaterializedKV(final SessionBytesStoreSupplier storeSupplier) {
        this.storeSupplier = adapt(storeSupplier);
    }

    private KeyValueBytesStoreSupplier adapt(SessionBytesStoreSupplier storeSupplier) {
        throw new UnsupportedOperationException("TODO: implement this, too");
    }

    private static KeyValueBytesStoreSupplier adapt(final WindowBytesStoreSupplier windowBytesStoreSupplier) {
        final Serde<Windowed<Bytes>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(Bytes.class);
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return windowBytesStoreSupplier.name();
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                final WindowStore<Bytes, byte[]> bytesWindowStore = windowBytesStoreSupplier.get();
                return new KeyValueStore<Bytes, byte[]>() {
                    @Override
                    public String name() {
                        return bytesWindowStore.name();
                    }

                    @Override
                    public boolean persistent() {
                        return bytesWindowStore.persistent();
                    }

                    @Override
                    public void init(final ProcessorContext context, final StateStore root) {
                        bytesWindowStore.init(context, root);
                    }

                    @Override
                    public void flush() {
                        bytesWindowStore.flush();
                    }

                    @Override
                    public void close() {
                        bytesWindowStore.close();
                    }

                    @Override
                    public boolean isOpen() {
                        return bytesWindowStore.isOpen();
                    }

                    @Override
                    public void put(final Bytes key, final byte[] value) {
                        final Windowed<Bytes> windowedBytes = windowedSerde.deserializer().deserialize(null, key.get());
                        bytesWindowStore.put(windowedBytes.key(), value, windowedBytes.window().start());
                    }

                    @Override
                    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
                        final Windowed<Bytes> windowedBytes = windowedSerde.deserializer().deserialize(null, key.get());
                        final byte[] fetched = bytesWindowStore.fetch(windowedBytes.key(), windowedBytes.window().start());
                        if (fetched != null) {
                            return fetched;
                        } else {
                            bytesWindowStore.put(windowedBytes.key(), value, windowedBytes.window().start());
                            return value;
                        }
                    }

                    @Override
                    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
                        for (final KeyValue<Bytes, byte[]> entry : entries) {
                            put(entry.key, entry.value);
                        }
                    }

                    @Override
                    public byte[] delete(final Bytes key) {
                        return putIfAbsent(key, null);
                    }

                    @Override
                    public byte[] get(final Bytes key) {
                        final Windowed<Bytes> windowedBytes = windowedSerde.deserializer().deserialize(null, key.get());
                        return bytesWindowStore.fetch(windowedBytes.key(), windowedBytes.window().start());
                    }

                    @Override
                    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
                        final Windowed<Bytes> windowedBytesFrom = windowedSerde.deserializer().deserialize(null, from.get());
                        final Windowed<Bytes> windowedBytesTo = windowedSerde.deserializer().deserialize(null, to.get());
                        final KeyValueIterator<Windowed<Bytes>, byte[]> fetch = bytesWindowStore.fetch(windowedBytesFrom.key(), windowedBytesTo.key(), windowedBytesFrom.window().start(), windowedBytesTo.window().start());
                        return adapt(fetch);
                    }

                    @Override
                    public KeyValueIterator<Bytes, byte[]> all() {
                        final KeyValueIterator<Windowed<Bytes>, byte[]> all = bytesWindowStore.all();
                        return adapt(all);
                    }

                    @Override
                    public long approximateNumEntries() {
                        throw new UnsupportedOperationException("Needs to be added to the WindowStore interface");
                    }

                    private KeyValueIterator<Bytes, byte[]> adapt(final KeyValueIterator<Windowed<Bytes>, byte[]> fetch) {
                        return new KeyValueIterator<Bytes, byte[]>() {
                            @Override
                            public void close() {
                                fetch.close();
                            }

                            @Override
                            public Bytes peekNextKey() {
                                final Windowed<Bytes> bytesWindowed = fetch.peekNextKey();
                                final byte[] serialized = windowedSerde.serializer().serialize(null, bytesWindowed);
                                return Bytes.wrap(serialized);
                            }

                            @Override
                            public boolean hasNext() {
                                return fetch.hasNext();
                            }

                            @Override
                            public KeyValue<Bytes, byte[]> next() {
                                final KeyValue<Windowed<Bytes>, byte[]> next = fetch.next();
                                final byte[] serializedKey = windowedSerde.serializer().serialize(null, next.key);
                                return new KeyValue<>(Bytes.wrap(serializedKey), next.value);
                            }
                        };
                    }
                };
            }

            @Override
            public String metricsScope() {
                return windowBytesStoreSupplier.metricsScope();
            }
        };
    }

    private MaterializedKV(final String storeName) {
        this.storeName = storeName;
    }

    /**
     * Copy constructor.
     *
     * @param materialized the {@link MaterializedKV} instance to copy.
     */
    protected MaterializedKV(final MaterializedKV<K, V> materialized) {
        storeSupplier = materialized.storeSupplier;
        storeName = materialized.storeName;
        keySerde = materialized.keySerde;
        valueSerde = materialized.valueSerde;
        loggingEnabled = materialized.loggingEnabled;
        cachingEnabled = materialized.cachingEnabled;
        topicConfig = materialized.topicConfig;
        retention = materialized.retention;
    }

    /**
     * Materialize a {@link StateStore} with the given name.
     *
     * @param storeName the name of the underlying {@link KTable} state store; valid characters are ASCII
     *                  alphanumerics, '.', '_' and '-'.
     * @param <K>       key type of the store
     * @param <V>       value type of the store
     * @param <S>       type of the {@link StateStore}
     * @return a new {@link MaterializedKV} instance with the given storeName
     */
    public static <K, V> MaterializedKV<K, V> as(final String storeName) {
        Named.validate(storeName);
        return new MaterializedKV<>(storeName);
    }

    /**
     * Materialize a {@link WindowStore} using the provided {@link WindowBytesStoreSupplier}.
     * <p>
     * Important: Custom subclasses are allowed here, but they should respect the retention contract:
     * Window stores are required to retain windows at least as long as (window size + window grace period).
     * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
     *
     * @param supplier the {@link WindowBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link MaterializedKV} instance with the given supplier
     */
    public static <K, V> MaterializedKV<K, V> as(final WindowBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new MaterializedKV<>(supplier);
    }

    /**
     * Materialize a {@link SessionStore} using the provided {@link SessionBytesStoreSupplier}.
     * <p>
     * Important: Custom subclasses are allowed here, but they should respect the retention contract:
     * Session stores are required to retain windows at least as long as (session inactivity gap + session grace period).
     * Stores constructed via {@link org.apache.kafka.streams.state.Stores} already satisfy this contract.
     *
     * @param supplier the {@link SessionBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link MaterializedKV} instance with the given sup
     * plier
     */
    public static <K, V> MaterializedKV<K, V> as(final SessionBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new MaterializedKV<>(supplier);
    }

    /**
     * Materialize a {@link KeyValueStore} using the provided {@link KeyValueBytesStoreSupplier}.
     *
     * @param supplier the {@link KeyValueBytesStoreSupplier} used to materialize the store
     * @param <K>      key type of the store
     * @param <V>      value type of the store
     * @return a new {@link MaterializedKV} instance with the given supplier
     */
    public static <K, V> MaterializedKV<K, V> as(final KeyValueBytesStoreSupplier supplier) {
        Objects.requireNonNull(supplier, "supplier can't be null");
        return new MaterializedKV<>(supplier);
    }

    /**
     * Materialize a {@link StateStore} with the provided key and value {@link Serde}s.
     * An internal name will be used for the store.
     *
     * @param keySerde   the key {@link Serde} to use. If the {@link Serde} is null, then the default key
     *                   serde from configs will be used
     * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
     *                   serde from configs will be used
     * @param <K>        key type
     * @param <V>        value type
     * @param <S>        store type
     * @return a new {@link MaterializedKV} instance with the given key and value serdes
     */
    public static <K, V> MaterializedKV<K, V> with(final Serde<K> keySerde,
                                                   final Serde<V> valueSerde) {
        return new MaterializedKV<K, V>((String) null).withKeySerde(keySerde).withValueSerde(valueSerde);
    }

    /**
     * Set the valueSerde the materialized {@link StateStore} will use.
     *
     * @param valueSerde the value {@link Serde} to use. If the {@link Serde} is null, then the default value
     *                   serde from configs will be used. If the serialized bytes is null for put operations,
     *                   it is treated as delete operation
     * @return itself
     */
    public MaterializedKV<K, V> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    /**
     * Set the keySerde the materialize {@link StateStore} will use.
     *
     * @param keySerde the key {@link Serde} to use. If the {@link Serde} is null, then the default key
     *                 serde from configs will be used
     * @return itself
     */
    public MaterializedKV<K, V> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    /**
     * Indicates that a changelog should be created for the store. The changelog will be created
     * with the provided configs.
     * <p>
     * Note: Any unrecognized configs will be ignored.
     *
     * @param config any configs that should be applied to the changelog
     * @return itself
     */
    public MaterializedKV<K, V> withLoggingEnabled(final Map<String, String> config) {
        loggingEnabled = true;
        topicConfig = config;
        return this;
    }

    /**
     * Disable change logging for the materialized {@link StateStore}.
     *
     * @return itself
     */
    public MaterializedKV<K, V> withLoggingDisabled() {
        loggingEnabled = false;
        topicConfig.clear();
        return this;
    }

    /**
     * Enable caching for the materialized {@link StateStore}.
     *
     * @return itself
     */
    public MaterializedKV<K, V> withCachingEnabled() {
        cachingEnabled = true;
        return this;
    }

    /**
     * Disable caching for the materialized {@link StateStore}.
     *
     * @return itself
     */
    public MaterializedKV<K, V> withCachingDisabled() {
        cachingEnabled = false;
        return this;
    }

    /**
     * Configure retention period for window and session stores. Ignored for key/value stores.
     * <p>
     * Overridden by pre-configured store suppliers
     * ({@link MaterializedKV#as(SessionBytesStoreSupplier)} or {@link MaterializedKV#as(WindowBytesStoreSupplier)}).
     * <p>
     * Note that the retention period must be at least long enough to contain the windowed data's entire life cycle,
     * from window-start through window-end, and for the entire grace period.
     *
     * @param retention the retention time
     * @return itself
     * @throws IllegalArgumentException if retention is negative or can't be represented as {@code long milliseconds}
     */
    public MaterializedKV<K, V> withRetention(final Duration retention) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retention, "retention");
        final long retenationMs = ApiUtils.validateMillisecondDuration(retention, msgPrefix);

        if (retenationMs < 0) {
            throw new IllegalArgumentException("Retention must not be negative.");
        }
        this.retention = retention;
        return this;
    }
}
