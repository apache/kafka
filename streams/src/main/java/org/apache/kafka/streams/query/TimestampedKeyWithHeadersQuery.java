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
 * <p>Headers are only returned when the queried store was built with a KIP-1271
 * {@code WithHeaders} supplier. Against a plain (non-headers) store, this query type is
 * unsupported and fails with {@link FailureReason#UNKNOWN_QUERY_TYPE}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
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
