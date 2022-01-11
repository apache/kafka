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

import java.util.Objects;

/**
 * Interactive query for retrieving a single record based on its key.
 */
@Evolving
public final class KeyQuery<K, V> implements Query<V> {

    private final K key;

    private KeyQuery(final K key) {
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Creates a query that will retrieve the record identified by {@code key} if it exists
     * (or {@code null} otherwise).
     * @param key The key to retrieve
     * @param <K> The type of the key
     * @param <V> The type of the value that will be retrieved
     */
    public static <K, V> KeyQuery<K, V> withKey(final K key) {
        return new KeyQuery<>(key);
    }

    /**
     * The key that was specified for this query.
     */
    public K getKey() {
        return key;
    }
}
