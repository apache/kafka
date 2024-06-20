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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

/**
 * Interactive query for retrieving a single record based on its key from {@link TimestampedKeyValueStore}
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@Evolving
public final class TimestampedKeyQuery<K, V> implements Query<ValueAndTimestamp<V>> {

    private final K key;
    private final boolean skipCache;

    private TimestampedKeyQuery(final K key, final boolean skipCache) {
        this.key = key;
        this.skipCache = skipCache;
    }

    /**
     * Creates a query that will retrieve the record identified by {@code key} if it exists
     * (or {@code null} otherwise).
     * @param key The key to retrieve
     * @param <K> The type of the key
     * @param <V> The type of the value that will be retrieved
     */
    public static <K, V> TimestampedKeyQuery<K, V> withKey(final K key) {
        Objects.requireNonNull(key, "the key should not be null");
        return new TimestampedKeyQuery<>(key, false);
    }

    /**
     * Specifies that the cache should be skipped during query evaluation. This means, that the query will always
     * get forwarded to the underlying store.
     */
    public TimestampedKeyQuery<K, V> skipCache() {
        return new TimestampedKeyQuery<>(key, true);
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
