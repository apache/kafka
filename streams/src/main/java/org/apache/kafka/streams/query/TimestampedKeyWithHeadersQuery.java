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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.processor.api.ReadOnlyRecord;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;

import java.util.Objects;

/**
 * Interactive query for retrieving a single record, including its record headers, based on its key
 * from a {@link TimestampedKeyValueStoreWithHeaders}.
 *
 * <p>This is the headers-aware parallel of {@link TimestampedKeyQuery}: it returns a
 * {@link ReadOnlyRecord} carrying the key, value, timestamp, and headers, whereas
 * {@link TimestampedKeyQuery} returns a {@link org.apache.kafka.streams.state.ValueAndTimestamp}
 * (value and timestamp only, no key or headers).
 *
 * <p>Headers are persisted and returned only when the store is backed by a native headers store,
 * i.e. built with a KIP-1271 {@code WithHeaders} byte-store supplier (e.g.
 * {@code Stores.persistentTimestampedKeyValueStoreWithHeaders}). A {@code WithHeaders} store built
 * over a legacy (non-headers) supplier cannot persist headers, so the result depends on who serves
 * the read: with caching enabled, a not-yet-evicted entry is served from the record cache and
 * returns the written value, timestamp, and headers (read-your-writes); once served from the
 * underlying store, the headers are gone. Over a timestamped legacy supplier (e.g.
 * {@code Stores.persistentTimestampedKeyValueStore}) the store-served read succeeds with empty
 * {@code headers()}; over a plain legacy supplier (e.g. {@code Stores.persistentKeyValueStore}),
 * which keeps no timestamp either, the store-served read has no representable timestamp and the
 * query fails with {@link FailureReason#STORE_EXCEPTION}.
 *
 * <p>Against a plain store not built with the {@code WithHeaders} builder at all, this query type is
 * unsupported and fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
@InterfaceAudience.Public
public final class TimestampedKeyWithHeadersQuery<K, V> implements Query<ReadOnlyRecord<K, V>> {

    private final K key;
    private final boolean skipCache;

    private TimestampedKeyWithHeadersQuery(final K key, final boolean skipCache) {
        this.key = key;
        this.skipCache = skipCache;
    }

    /**
     * Creates a query that will retrieve the record (value, timestamp, and headers) identified by
     * {@code key} if it exists (or {@code null} otherwise).
     * @param key The key to retrieve
     * @param <K> The type of the key
     * @param <V> The type of the value that will be retrieved
     */
    public static <K, V> TimestampedKeyWithHeadersQuery<K, V> withKey(final K key) {
        Objects.requireNonNull(key, "the key should not be null");
        return new TimestampedKeyWithHeadersQuery<>(key, false);
    }

    /**
     * Specifies that the cache should be skipped during query evaluation. This means, that the query will always
     * get forwarded to the underlying store.
     */
    public TimestampedKeyWithHeadersQuery<K, V> skipCache() {
        return new TimestampedKeyWithHeadersQuery<>(key, true);
    }

    /**
     * Return the key that was specified for this query.
     *
     * @return The key that was specified for this query.
     */
    public K key() {
        return key;
    }

    /**
     * The flag whether to skip the cache or not during query evaluation.
     */
    public boolean isSkipCache() {
        return skipCache;
    }
}
