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
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbSessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

import java.time.Duration;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

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

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent store
     */
    public static KeyValueBytesStoreSupplier persistentKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDbKeyValueBytesStoreSupplier(name, false);
    }

    /**
     * Create an in-memory {@link KeyValueBytesStoreSupplier}.
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} than can be used to
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
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative).
     *                              Note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period.
     * @param numSegments           number of db segments (cannot be zero or negative)
     * @param windowSize            size of the windows that are stored (cannot be negative). Note: the window size
     *                              is not stored with the records, so this value is used to compute the keys that
     *                              the store returns. No effort is made to validate this parameter, so you must be
     *                              careful to set it the same as the windowed keys you're actually storing.
     * @param retainDuplicates      whether or not to retain duplicates.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @deprecated since 2.1 Use {@link Stores#persistentWindowStore(String, Duration, Duration, boolean)} instead
     */
    @Deprecated
    public static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                 final long retentionPeriod,
                                                                 final int numSegments,
                                                                 final long windowSize,
                                                                 final boolean retainDuplicates) {
        if (numSegments < 2) {
            throw new IllegalArgumentException("numSegments cannot must smaller than 2");
        }

        final long legacySegmentInterval = Math.max(retentionPeriod / (numSegments - 1), 60_000L);

        return persistentWindowStore(
            name,
            retentionPeriod,
            windowSize,
            retainDuplicates,
            legacySegmentInterval
        );
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              Note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period.
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     */
    public static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                 final Duration retentionPeriod,
                                                                 final Duration windowSize,
                                                                 final boolean retainDuplicates) throws IllegalArgumentException {
        Objects.requireNonNull(name, "name cannot be null");
        final String rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = ApiUtils.validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        final String wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = ApiUtils.validateMillisecondDuration(windowSize, wsMsgPrefix);

        final long defaultSegmentInterval = Math.max(retentionMs / 2, 60_000L);

        return persistentWindowStore(name, retentionMs, windowSizeMs, retainDuplicates, defaultSegmentInterval);
    }

    private static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                  final long retentionPeriod,
                                                                  final long windowSize,
                                                                  final boolean retainDuplicates,
                                                                  final long segmentInterval) {
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriod < 0L) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        if (windowSize < 0L) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }
        if (segmentInterval < 1L) {
            throw new IllegalArgumentException("segmentInterval cannot be zero or negative");
        }
        if (windowSize > retentionPeriod) {
            throw new IllegalArgumentException("The retention period of the window store "
                                                   + name + " must be no smaller than its window size. Got size=["
                                                   + windowSize + "], retention=[" + retentionPeriod + "]");
        }

        return new RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length ot time to retain data in the store (cannot be negative)
     *                          Note that the retention period must be at least long enough to contain the
     *                          windowed data's entire life cycle, from window-start through window-end,
     *                          and for the entire grace period.
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     * @deprecated since 2.1 Use {@link Stores#persistentSessionStore(String, Duration)} instead
     */
    @Deprecated
    public static SessionBytesStoreSupplier persistentSessionStore(final String name,
                                                                   final long retentionPeriod) {
        Objects.requireNonNull(name, "name cannot be null");
        if (retentionPeriod < 0) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        return new RocksDbSessionBytesStoreSupplier(name, retentionPeriod);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length ot time to retain data in the store (cannot be negative)
     *                          Note that the retention period must be at least long enough to contain the
     *                          windowed data's entire life cycle, from window-start through window-end,
     *                          and for the entire grace period.
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    @SuppressWarnings("deprecation")
    public static SessionBytesStoreSupplier persistentSessionStore(final String name,
                                                                   final Duration retentionPeriod) {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        return persistentSessionStore(name, ApiUtils.validateMillisecondDuration(retentionPeriod, msgPrefix));
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

