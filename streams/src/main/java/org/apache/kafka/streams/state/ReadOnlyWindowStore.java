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

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;

/**
 * A window store that only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes are expected.
 *
 * <p>Note: The current implementation of either forward or backward fetches on range-key-range-time does not
 * obey the ordering when there are multiple local stores hosted on that instance. For example,
 * if there are two stores from two tasks hosting keys {1,3} and {2,4}, then a range query of key [1,4]
 * would return in the order of [1,3,2,4] but not [1,2,3,4] since it is just looping over the stores only.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface ReadOnlyWindowStore<K, V> {

    /**
     * Get the value of key from a window.
     *
     * @param key  the key to fetch
     * @param time start timestamp (inclusive) of the window
     * @return The value or {@code null} if no value is found in the window
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if {@code null} is used for any key.
     */
    V fetch(K key, long time);

    /**
     * Get all the key-value pairs with the given key and the time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     * <p>
     * The time range is inclusive and applies to the starting timestamp of the window.
     * For example, if we have the following windows:
     * <pre>
     * +-------------------------------+
     * |  key  | start time | end time |
     * +-------+------------+----------+
     * |   A   |     10     |    20    |
     * +-------+------------+----------+
     * |   A   |     15     |    25    |
     * +-------+------------+----------+
     * |   A   |     20     |    30    |
     * +-------+------------+----------+
     * |   A   |     25     |    35    |
     * +--------------------------------
     * </pre>
     * And we call {@code store.fetch("A", Instant.ofEpochMilli(10), Instant.ofEpochMilli(20))} then the results will contain the first
     * three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
     * <p>
     * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
     * available window to the newest/latest window.
     *
     * @param key  the key to fetch
     * @param timeFrom time range start (inclusive), where iteration starts.
     * @param timeTo   time range end (inclusive), where iteration ends.
     * @return an iterator over key-value pairs {@code <timestamp, value>}, from beginning to end of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if {@code null} is used for key.
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    WindowStoreIterator<V> fetch(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException;

    /**
     * Get all the key-value pairs with the given key and the time range from all the existing windows
     * in backward order with respect to time (from end to beginning of time).
     * <p>
     * This iterator must be closed after use.
     * <p>
     * The time range is inclusive and applies to the starting timestamp of the window.
     * For example, if we have the following windows:
     * <pre>
     * +-------------------------------+
     * |  key  | start time | end time |
     * +-------+------------+----------+
     * |   A   |     10     |    20    |
     * +-------+------------+----------+
     * |   A   |     15     |    25    |
     * +-------+------------+----------+
     * |   A   |     20     |    30    |
     * +-------+------------+----------+
     * |   A   |     25     |    35    |
     * +--------------------------------
     * </pre>
     * And we call {@code store.backwardFetch("A", Instant.ofEpochMilli(10), Instant.ofEpochMilli(20))} then the
     * results will contain the first three windows from the table above in backward order,
     * i.e., all those where 10 &lt;= start time &lt;= 20.
     * <p>
     * For each key, the iterator guarantees ordering of windows, starting from the newest/latest
     * available window to the oldest/earliest window.
     *
     * @param key  the key to fetch
     * @param timeFrom time range start (inclusive), where iteration ends.
     * @param timeTo   time range end (inclusive), where iteration starts.
     * @return an iterator over key-value pairs {@code <timestamp, value>}, from end to beginning of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if {@code null} is used for key.
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    default WindowStoreIterator<V> backwardFetch(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException  {
        throw new UnsupportedOperationException();
    }

    /**
     * Get all the key-value pairs in the given key range and time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom     the first key in the range
     *                    A null value indicates a starting position from the first element in the store.
     * @param keyTo       the last key in the range
     *                    A null value indicates that the range ends with the last element in the store.
     * @param timeFrom time range start (inclusive), where iteration starts.
     * @param timeTo   time range end (inclusive), where iteration ends.
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from beginning to end of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    KeyValueIterator<Windowed<K>, V> fetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo)
        throws IllegalArgumentException;

    /**
     * Get all the key-value pairs in the given key range and time range from all the existing windows
     * in backward order with respect to time (from end to beginning of time).
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom     the first key in the range
     *                    A null value indicates a starting position from the first element in the store.
     * @param keyTo       the last key in the range
     *                    A null value indicates that the range ends with the last element in the store.
     * @param timeFrom time range start (inclusive), where iteration ends.
     * @param timeTo   time range end (inclusive), where iteration starts.
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from end to beginning of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    default KeyValueIterator<Windowed<K>, V> backwardFetch(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo)
        throws IllegalArgumentException  {
        throw new UnsupportedOperationException();
    }


    /**
     * Gets all the key-value pairs in the existing windows.
     *
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from beginning to end of time.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    KeyValueIterator<Windowed<K>, V> all();

    /**
     * Gets all the key-value pairs in the existing windows in backward order
     * with respect to time (from end to beginning of time).
     *
     * @return an backward iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from the end to beginning of time.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    default KeyValueIterator<Windowed<K>, V> backwardAll() {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets all the key-value pairs that belong to the windows within in the given time range.
     *
     * @param timeFrom the beginning of the time slot from which to search (inclusive), where iteration starts.
     * @param timeTo   the end of the time slot from which to search (inclusive), where iteration ends.
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from beginning to end of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if {@code null} is used for any key
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    KeyValueIterator<Windowed<K>, V> fetchAll(Instant timeFrom, Instant timeTo) throws IllegalArgumentException;

    /**
     * Gets all the key-value pairs that belong to the windows within in the given time range in backward order
     * with respect to time (from end to beginning of time).
     *
     * @param timeFrom the beginning of the time slot from which to search (inclusive), where iteration ends.
     * @param timeTo   the end of the time slot from which to search (inclusive), where iteration starts.
     * @return an backward iterator over windowed key-value pairs {@code <Windowed<K>, value>}, from end to beginning of time.
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if {@code null} is used for any key
     * @throws IllegalArgumentException   if duration is negative or can't be represented as {@code long milliseconds}
     */
    default KeyValueIterator<Windowed<K>, V> backwardFetchAll(Instant timeFrom, Instant timeTo) throws IllegalArgumentException  {
        throw new UnsupportedOperationException();
    }
}
