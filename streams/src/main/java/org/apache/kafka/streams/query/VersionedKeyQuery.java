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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Interactive query for retrieving a single record from a versioned state store based on its key and timestamp.
 */
@Evolving
public final class VersionedKeyQuery<K, V> implements Query<VersionedRecord<V>> {

    private final K key;
    private final Optional<Instant> asOfTimestamp;

    private VersionedKeyQuery(final K key, final Optional<Instant> asOfTimestamp) {
        this.key = Objects.requireNonNull(key);
        this.asOfTimestamp = asOfTimestamp;
    }

    /**
     * Creates a query that will retrieve the record from a versioned state store identified by {@code key} if it exists
     * (or {@code null} otherwise).
     * @param key The key to retrieve
     * @param <K> The type of the key
     * @param <V> The type of the value that will be retrieved
     * @throws NullPointerException if @param key is null
     */
    public static <K, V> VersionedKeyQuery<K, V> withKey(final K key) {
        return new VersionedKeyQuery<>(key, Optional.empty());
    }

    /**
     * Specifies the timestamp for the key query. The key query returns the record version for the specified timestamp.
     * (To be more precise: The key query returns the record with the greatest timestamp <= asOfTimestamp)
     * if @param asOfTimestamp is null, it will be considered as Optional.empty()
     * @param asOfTimestamp The as of timestamp for timestamp
     */
    public VersionedKeyQuery<K, V> asOf(final Instant asOfTimestamp) {
        if (asOfTimestamp == null) {
            return new VersionedKeyQuery<>(key, Optional.empty());
        }
        return new VersionedKeyQuery<>(key, Optional.of(asOfTimestamp));
    }

    /**
     * The key that was specified for this query.
     */
    public K key() {
        return key;
    }

    /**
     * The timestamp of the query, if specified
     */
    public Optional<Instant> asOfTimestamp() {
        return asOfTimestamp;
    }
}
