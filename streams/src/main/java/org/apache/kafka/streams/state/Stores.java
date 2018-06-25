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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbSessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Factory for creating state stores in Kafka Streams.
 * <p>
 * When using the high-level DSL, i.e., {@link org.apache.kafka.streams.StreamsBuilder StreamsBuilder}, users create
 * {@link StoreSupplier}s that can be further customized via
 * {@link org.apache.kafka.streams.kstream.Materialized Materialized}.
 * For example, a topic read as {@link org.apache.kafka.streams.kstream.KTable KTable} can be materialized into an
 * in-memory store with custom key/value serdes and caching disabled:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("queryable-store-name");
 * KTable<Long,String> table = builder.table(
 *   "topicName",
 *   Materialized.<Long,String>as(storeSupplier)
 *               .withKeySerde(Serdes.Long())
 *               .withValueSerde(Serdes.String())
 *               .withCachingDisabled());
 * }</pre>
 * When using the Processor API, i.e., {@link org.apache.kafka.streams.Topology Topology}, users create
 * {@link StoreBuilder}s that can be attached to {@link org.apache.kafka.streams.processor.Processor Processor}s.
 * For example, you can create a {@link org.apache.kafka.streams.kstream.Windowed windowed} RocksDB store with custom
 * changelog topic configuration like:
 * <pre>{@code
 * Topology topology = new Topology();
 * topology.addProcessor("processorName", ...);
 *
 * Map<String,String> topicConfig = new HashMap<>();
 * StoreBuilder<WindowStore<Integer, Long>> storeBuilder = Stores
 *   .windowStoreBuilder(
 *     Stores.persistentWindowStore("queryable-store-name", ...),
 *     Serdes.Integer(),
 *     Serdes.Long())
 *   .withLoggingEnabled(topicConfig);
 *
 * topology.addStateStore(storeBuilder, "processorName");
 * }</pre>
 */
@InterfaceStability.Evolving
public class Stores {

    private static final Logger log = LoggerFactory.getLogger(Stores.class);

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * @param name  name of the store (cannot be {@code null})
     * @return  an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent store
     */
    public static KeyValueBytesStoreSupplier persistentKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDbKeyValueBytesStoreSupplier(name);
    }

    /**
     * Create an in-memory {@link KeyValueBytesStoreSupplier}.
     * @param name  name of the store (cannot be {@code null})
     * @return  an instance of a {@link KeyValueBytesStoreSupplier} than can be used to
     * build an in-memory store
     */
    public static KeyValueBytesStoreSupplier inMemoryKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new InMemoryKeyValueStore<>(name, Serdes.Bytes(), Serdes.ByteArray());
            }

            @Override
            public String metricsScope() {
                return "in-memory-state";
            }
        };
    }

    /**
     * Create a LRU Map {@link KeyValueBytesStoreSupplier}.
     * @param name          name of the store (cannot be {@code null})
     * @param maxCacheSize  maximum number of items in the LRU (cannot be negative)
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used to build
     * an LRU Map based store
     */
    public static KeyValueBytesStoreSupplier lruMap(final String name, final int maxCacheSize) {
        Objects.requireNonNull(name, "name cannot be null");
        if (maxCacheSize < 0) {
            throw new IllegalArgumentException("maxCacheSize cannot be negative");
        }
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new MemoryNavigableLRUCache<>(name, maxCacheSize, Serdes.Bytes(), Serdes.ByteArray());
            }

            @Override
            public String metricsScope() {
                return "in-memory-lru-state";
            }
        };
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     * @param numSegments           number of db segments (cannot be zero or negative)
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates.
     * @return an instance of {@link WindowBytesStoreSupplier}
     */
    public static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                 final long retentionPeriod,
                                                                 final int numSegments,
                                                                 final long windowSize,
                                                                 final boolean retainDuplicates) {
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriod < 0) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        if (numSegments < 2) {
            throw new IllegalArgumentException("numSegments cannot must smaller than 2");
        }
        if (windowSize < 0) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }
        final long segmentIntervalMs = Math.max(retentionPeriod / (numSegments - 1), 60_000L);

        return new RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentIntervalMs, windowSize, retainDuplicates);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length ot time to retain data in the store (cannot be negative)
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    public static SessionBytesStoreSupplier persistentSessionStore(final String name,
                                                                   final long retentionPeriod) {
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriod < 0) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        return new RocksDbSessionBytesStoreSupplier(name, retentionPeriod);
    }


    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link WindowStore}.
     * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is null for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} than can build a {@link WindowStore}
     */
    public static <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(final WindowBytesStoreSupplier supplier,
                                                                            final Serde<K> keySerde,
                                                                            final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new WindowStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} than can be used to build a {@link KeyValueStore}.
     * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is null for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
     */
    public static <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                final Serde<K> keySerde,
                                                                                final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new KeyValueStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link SessionStore}.
     * @param supplier      a {@link SessionBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is null for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} than can build a {@link SessionStore}
     * */
    public static <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(final SessionBytesStoreSupplier supplier,
                                                                              final Serde<K> keySerde,
                                                                              final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new SessionStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }
}

