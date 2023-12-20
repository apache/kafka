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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemorySessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.MemoryNavigableLRUCache;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbSessionBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbVersionedKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.TimestampedWindowStoreBuilder;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.WindowStoreBuilder;

import java.time.Duration;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

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
 * {@link StoreBuilder}s that can be attached to {@link org.apache.kafka.streams.processor.api.Processor Processor}s.
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
public final class Stores {

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link TimestampedKeyValueStore} or {@link VersionedKeyValueStore}
     * you should use {@link #persistentTimestampedKeyValueStore(String)} or
     * {@link #persistentVersionedKeyValueStore(String, Duration)}, respectively,
     * to create a store supplier instead.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent key-value store
     */
    public static KeyValueBytesStoreSupplier persistentKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDBKeyValueBytesStoreSupplier(name, false);
    }

    /**
     * Create a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link KeyValueStore} or a {@link VersionedKeyValueStore}
     * you should use {@link #persistentKeyValueStore(String)} or
     * {@link #persistentVersionedKeyValueStore(String, Duration)}, respectively,
     * to create a store supplier instead.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     * to build a persistent key-(timestamp/value) store
     */
    public static KeyValueBytesStoreSupplier persistentTimestampedKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new RocksDBKeyValueBytesStoreSupplier(name, true);
    }

    /**
     * Create a persistent versioned key-value store {@link VersionedBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #versionedKeyValueStoreBuilder(VersionedBytesStoreSupplier, Serde, Serde)}.
     * <p>
     * Note that it is not safe to change the value of {@code historyRetention} between
     * application restarts without clearing local state from application instances,
     * as this may cause incorrect values to be read from the state store if it impacts
     * the underlying storage format.
     *
     * @param name             name of the store (cannot be {@code null})
     * @param historyRetention length of time that old record versions are available for query
     *                         (cannot be negative). If a timestamp bound provided to
     *                         {@link VersionedKeyValueStore#get(Object, long)} is older than this
     *                         specified history retention, then the get operation will not return data.
     *                         This parameter also determines the "grace period" after which
     *                         out-of-order writes will no longer be accepted.
     * @return an instance of {@link VersionedBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code historyRetention} can't be represented as {@code long milliseconds}
     */
    public static VersionedBytesStoreSupplier persistentVersionedKeyValueStore(final String name,
                                                                               final Duration historyRetention) {
        Objects.requireNonNull(name, "name cannot be null");
        final String hrMsgPrefix = prepareMillisCheckFailMsgPrefix(historyRetention, "historyRetention");
        final long historyRetentionMs = validateMillisecondDuration(historyRetention, hrMsgPrefix);
        if (historyRetentionMs < 0L) {
            throw new IllegalArgumentException("historyRetention cannot be negative");
        }
        return new RocksDbVersionedKeyValueBytesStoreSupplier(name, historyRetentionMs);
    }

    /**
     * Create a persistent versioned key-value store {@link VersionedBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #versionedKeyValueStoreBuilder(VersionedBytesStoreSupplier, Serde, Serde)}.
     * <p>
     * Note that it is not safe to change the value of {@code segmentInterval} between
     * application restarts without clearing local state from application instances,
     * as this may cause incorrect values to be read from the state store otherwise.
     *
     * @param name             name of the store (cannot be {@code null})
     * @param historyRetention length of time that old record versions are available for query
     *                         (cannot be negative). If a timestamp bound provided to
     *                         {@link VersionedKeyValueStore#get(Object, long)} is older than this
     *                         specified history retention, then the get operation will not return data.
     *                         This parameter also determines the "grace period" after which
     *                         out-of-order writes will no longer be accepted.
     * @param segmentInterval  size of segments for storing old record versions (must be positive). Old record versions
     *                         for the same key in a single segment are stored (updated and accessed) together.
     *                         The only impact of this parameter is performance. If segments are large
     *                         and a workload results in many record versions for the same key being collected
     *                         in a single segment, performance may degrade as a result. On the other hand,
     *                         historical reads (which access older segments) and out-of-order writes may
     *                         slow down if there are too many segments.
     * @return an instance of {@link VersionedBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code historyRetention} or {@code segmentInterval} can't be represented as {@code long milliseconds}
     */
    public static VersionedBytesStoreSupplier persistentVersionedKeyValueStore(final String name,
                                                                               final Duration historyRetention,
                                                                               final Duration segmentInterval) {
        Objects.requireNonNull(name, "name cannot be null");
        final String hrMsgPrefix = prepareMillisCheckFailMsgPrefix(historyRetention, "historyRetention");
        final long historyRetentionMs = validateMillisecondDuration(historyRetention, hrMsgPrefix);
        if (historyRetentionMs < 0L) {
            throw new IllegalArgumentException("historyRetention cannot be negative");
        }
        final String siMsgPrefix = prepareMillisCheckFailMsgPrefix(segmentInterval, "segmentInterval");
        final long segmentIntervalMs = validateMillisecondDuration(segmentInterval, siMsgPrefix);
        if (segmentIntervalMs < 1L) {
            throw new IllegalArgumentException("segmentInterval cannot be zero or negative");
        }
        return new RocksDbVersionedKeyValueBytesStoreSupplier(name, historyRetentionMs, segmentIntervalMs);
    }

    /**
     * Create an in-memory {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
     * or {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name  name of the store (cannot be {@code null})
     * @return an instance of a {@link KeyValueBytesStoreSupplier} than can be used to
     * build an in-memory store
     */
    public static KeyValueBytesStoreSupplier inMemoryKeyValueStore(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new InMemoryKeyValueBytesStoreSupplier(name);
    }

    /**
     * Create a LRU Map {@link KeyValueBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}
     * or {@link #timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name          name of the store (cannot be {@code null})
     * @param maxCacheSize  maximum number of items in the LRU (cannot be negative)
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used to build
     * an LRU Map based store
     * @throws IllegalArgumentException if {@code maxCacheSize} is negative
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
                return new MemoryNavigableLRUCache(name, maxCacheSize);
            }

            @Override
            public String metricsScope() {
                return "in-memory-lru";
            }
        };
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link TimestampedWindowStore} you should use
     * {@link #persistentTimestampedWindowStore(String, Duration, Duration, boolean)} to create a store supplier instead.
     * <p>
     * Note that it is not safe to change the value of {@code retentionPeriod} between
     * application restarts without clearing local state from application instances,
     * as this may cause incorrect values to be read from the state store if it impacts
     * the underlying storage format.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              (note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period)
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates. Turning this on will automatically disable
     *                              caching and means that null values will be ignored.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     * @throws IllegalArgumentException if {@code retentionPeriod} is smaller than {@code windowSize}
     */
    public static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                 final Duration retentionPeriod,
                                                                 final Duration windowSize,
                                                                 final boolean retainDuplicates) throws IllegalArgumentException {
        return persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates, false);
    }

    /**
     * Create a persistent {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a
     * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     * If you want to create a {@link WindowStore} you should use
     * {@link #persistentWindowStore(String, Duration, Duration, boolean)} to create a store supplier instead.
     * <p>
     * Note that it is not safe to change the value of {@code retentionPeriod} between
     * application restarts without clearing local state from application instances,
     * as this may cause incorrect values to be read from the state store if it impacts
     * the underlying storage format.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              (note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period)
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates. Turning this on will automatically disable
     *                              caching and means that null values will be ignored.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     * @throws IllegalArgumentException if {@code retentionPeriod} is smaller than {@code windowSize}
     */
    public static WindowBytesStoreSupplier persistentTimestampedWindowStore(final String name,
                                                                            final Duration retentionPeriod,
                                                                            final Duration windowSize,
                                                                            final boolean retainDuplicates) throws IllegalArgumentException {
        return persistentWindowStore(name, retentionPeriod, windowSize, retainDuplicates, true);
    }

    private static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                  final Duration retentionPeriod,
                                                                  final Duration windowSize,
                                                                  final boolean retainDuplicates,
                                                                  final boolean timestampedStore) {
        Objects.requireNonNull(name, "name cannot be null");
        final String rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        final String wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = validateMillisecondDuration(windowSize, wsMsgPrefix);

        final long defaultSegmentInterval = Math.max(retentionMs / 2, 60_000L);

        return persistentWindowStore(name, retentionMs, windowSizeMs, retainDuplicates, defaultSegmentInterval, timestampedStore);
    }

    private static WindowBytesStoreSupplier persistentWindowStore(final String name,
                                                                  final long retentionPeriod,
                                                                  final long windowSize,
                                                                  final boolean retainDuplicates,
                                                                  final long segmentInterval,
                                                                  final boolean timestampedStore) {
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

        return new RocksDbWindowBytesStoreSupplier(
            name,
            retentionPeriod,
            segmentInterval,
            windowSize,
            retainDuplicates,
            timestampedStore);
    }

    /**
     * Create an in-memory {@link WindowBytesStoreSupplier}.
     * <p>
     * This store supplier can be passed into a {@link #windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)} or
     * {@link #timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)}.
     *
     * @param name                  name of the store (cannot be {@code null})
     * @param retentionPeriod       length of time to retain data in the store (cannot be negative)
     *                              Note that the retention period must be at least long enough to contain the
     *                              windowed data's entire life cycle, from window-start through window-end,
     *                              and for the entire grace period.
     * @param windowSize            size of the windows (cannot be negative)
     * @param retainDuplicates      whether or not to retain duplicates. Turning this on will automatically disable
     *                              caching and means that null values will be ignored.
     * @return an instance of {@link WindowBytesStoreSupplier}
     * @throws IllegalArgumentException if {@code retentionPeriod} or {@code windowSize} can't be represented as {@code long milliseconds}
     * @throws IllegalArgumentException if {@code retentionPeriod} is smaller than {@code windowSize}
     */
    public static WindowBytesStoreSupplier inMemoryWindowStore(final String name,
                                                               final Duration retentionPeriod,
                                                               final Duration windowSize,
                                                               final boolean retainDuplicates) throws IllegalArgumentException {
        Objects.requireNonNull(name, "name cannot be null");

        final String repartitionPeriodErrorMessagePrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = validateMillisecondDuration(retentionPeriod, repartitionPeriodErrorMessagePrefix);
        if (retentionMs < 0L) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }

        final String windowSizeErrorMessagePrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = validateMillisecondDuration(windowSize, windowSizeErrorMessagePrefix);
        if (windowSizeMs < 0L) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }

        if (windowSizeMs > retentionMs) {
            throw new IllegalArgumentException("The retention period of the window store "
                + name + " must be no smaller than its window size. Got size=["
                + windowSize + "], retention=[" + retentionPeriod + "]");
        }

        return new InMemoryWindowBytesStoreSupplier(name, retentionMs, windowSizeMs, retainDuplicates);
    }

    /**
     * Create a persistent {@link SessionBytesStoreSupplier}.
     * <p>
     * Note that it is not safe to change the value of {@code retentionPeriod} between
     * application restarts without clearing local state from application instances,
     * as this may cause incorrect values to be read from the state store if it impacts
     * the underlying storage format.
     *
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length of time to retain data in the store (cannot be negative)
     *                          (note that the retention period must be at least as long enough to
     *                          contain the inactivity gap of the session and the entire grace period.)
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    public static SessionBytesStoreSupplier persistentSessionStore(final String name,
                                                                   final Duration retentionPeriod) {
        Objects.requireNonNull(name, "name cannot be null");
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionPeriodMs = validateMillisecondDuration(retentionPeriod, msgPrefix);
        if (retentionPeriodMs < 0) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        return new RocksDbSessionBytesStoreSupplier(name, retentionPeriodMs);
    }

    /**
     * Create an in-memory {@link SessionBytesStoreSupplier}.
     *
     * @param name              name of the store (cannot be {@code null})
     * @param retentionPeriod   length of time to retain data in the store (cannot be negative)
     *                          (note that the retention period must be at least as long enough to
     *                          contain the inactivity gap of the session and the entire grace period.)
     * @return an instance of a {@link  SessionBytesStoreSupplier}
     */
    public static SessionBytesStoreSupplier inMemorySessionStore(final String name, final Duration retentionPeriod) {
        Objects.requireNonNull(name, "name cannot be null");

        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionPeriodMs = validateMillisecondDuration(retentionPeriod, msgPrefix);
        if (retentionPeriodMs < 0) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        return new InMemorySessionBytesStoreSupplier(name, retentionPeriodMs);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link KeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     *
     * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
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
     * Creates a {@link StoreBuilder} that can be used to build a {@link TimestampedKeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link KeyValueStore KeyValueStores}. For this case, passed in timestamps will be dropped and not stored in the
     * key-value-store. On read, no valid timestamp but a dummy timestamp will be returned.
     *
     * @param supplier      a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of a {@link StoreBuilder} that can build a {@link KeyValueStore}
     */
    public static <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                                      final Serde<K> keySerde,
                                                                                                      final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new TimestampedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link VersionedKeyValueStore}.
     *
     * @param supplier   a {@link VersionedBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as a deletion
     * @param <K>        key type
     * @param <V>        value type
     * @return an instance of a {@link StoreBuilder} that can build a {@link VersionedKeyValueStore}
     */
    public static <K, V> StoreBuilder<VersionedKeyValueStore<K, V>> versionedKeyValueStoreBuilder(final VersionedBytesStoreSupplier supplier,
                                                                                                  final Serde<K> keySerde,
                                                                                                  final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new VersionedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link WindowStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedWindowStore TimestampedWindowStores}.
     *
     * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
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
     * Creates a {@link StoreBuilder} that can be used to build a {@link TimestampedWindowStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link WindowStore WindowStores}. For this case, passed in timestamps will be dropped and not stored in the
     * window-store. On read, no valid timestamp but a dummy timestamp will be returned.
     *
     * @param supplier      a {@link WindowBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} that can build a {@link TimestampedWindowStore}
     */
    public static <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(final WindowBytesStoreSupplier supplier,
                                                                                                  final Serde<K> keySerde,
                                                                                                  final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new TimestampedWindowStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }

    /**
     * Creates a {@link StoreBuilder} that can be used to build a {@link SessionStore}.
     *
     * @param supplier      a {@link SessionBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde      the key serde to use
     * @param valueSerde    the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                      it is treated as delete
     * @param <K>           key type
     * @param <V>           value type
     * @return an instance of {@link StoreBuilder} than can build a {@link SessionStore}
     */
    public static <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(final SessionBytesStoreSupplier supplier,
                                                                              final Serde<K> keySerde,
                                                                              final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new SessionStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }
}