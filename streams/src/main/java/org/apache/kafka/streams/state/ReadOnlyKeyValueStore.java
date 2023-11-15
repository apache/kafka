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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * A key-value store that only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes are expected.
 * <p>
 * Please note that this contract defines the thread-safe read functionality only; it does not
 * guarantee anything about whether the actual instance is writable by another thread, or
 * whether it uses some locking mechanism under the hood. For this reason, making dependencies
 * between the read and write operations on different StateStore instances can cause concurrency
 * problems like deadlock.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface ReadOnlyKeyValueStore<K, V> {

    /**
     * Get the value corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    V get(K key);

    /**
     * Get an iterator over a given range of keys. This iterator must be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values.
     * Order is based on the serialized byte[] of the keys, not the 'logical' key order.
     *
     * @param from The first key that could be in the range, where iteration starts from.
     *             A null value indicates that the range starts with the first element in the store.
     * @param to   The last key that could be in the range, where iteration ends.
     *             A null value indicates that the range ends with the last element in the store.
     * @return The iterator for this range, from key with the smallest bytes to the key with the largest bytes of keys.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    KeyValueIterator<K, V> range(K from, K to);

    /**
     * Get a reverse iterator over a given range of keys. This iterator must be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values.
     * Order is based on the serialized byte[] of the keys, not the 'logical' key order.
     *
     * @param from The first key that could be in the range, where iteration ends.
     *             A null value indicates that the range starts with the first element in the store.
     * @param to   The last key that could be in the range, where iteration starts from.
     *             A null value indicates that the range ends with the last element in the store.
     * @return The iterator for this range, from key with the smallest bytes to the key with the largest bytes of keys.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    default KeyValueIterator<K, V> reverseRange(K from, K to) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return an iterator over all keys in this store. This iterator must be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values.
     * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
     *
     * @return An iterator of all key/value pairs in the store, from smallest to largest bytes.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    KeyValueIterator<K, V> all();

    /**
     * Return a reverse iterator over all keys in this store. This iterator must be closed after use.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values.
     * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
     *
     * @return An reverse iterator of all key/value pairs in the store, from largest to smallest key bytes.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    default KeyValueIterator<K, V> reverseAll() {
        throw new UnsupportedOperationException();
    }

    /**
     * Return an iterator over all keys with the specified prefix.
     * Since the type of the prefix can be different from that of the key, a serializer to convert the
     * prefix into the format in which the keys are stored in the stores needs to be passed to this method.
     * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
     * and must not return null values.
     * Since {@code prefixScan()} relies on byte lexicographical ordering and not on the ordering of the key type, results for some types might be unexpected.
     * For example, if the key type is {@code Integer}, and the store contains keys [1, 2, 11, 13],
     * then running {@code store.prefixScan(1, new IntegerSerializer())} will return [1] and not [1,11,13]. 
     * In contrast, if the key type is {@code String} the keys will be sorted [1, 11, 13, 2] in the store and {@code store.prefixScan(1, new StringSerializer())} will return [1,11,13]. 
     * In both cases {@code prefixScan()} starts the scan at 1 and stops at 2.
     *
     * @param prefix The prefix.
     * @param prefixKeySerializer Serializer for the Prefix key type
     * @param <PS> Prefix Serializer type
     * @param <P> Prefix Type.
     * @return The iterator for keys having the specified prefix.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    default <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(P prefix, PS prefixKeySerializer) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return an approximate count of key-value mappings in this store.
     * <p>
     * The count is not guaranteed to be exact in order to accommodate stores
     * where an exact count is expensive to calculate.
     *
     * @return an approximate count of key-value mappings in the store.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    long approximateNumEntries();
}
