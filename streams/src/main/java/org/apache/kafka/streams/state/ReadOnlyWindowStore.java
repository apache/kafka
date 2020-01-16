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
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface ReadOnlyWindowStore<K, V> {

    /**
     * Get the value of key from a window.
     *
     * @param key       the key to fetch
     * @param time      start timestamp (inclusive) of the window
     * @return The value or {@code null} if no value is found in the window
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If {@code null} is used for any key.
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
     * And we call {@code store.fetch("A", 10, 20)} then the results will contain the first
     * three windows from the table above, i.e., all those where 10 &lt;= start time &lt;= 20.
     * <p>
     * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
     * available window to the newest/latest window.
     *
     * @param key       the key to fetch
     * @param timeFrom  time range start (inclusive)
     * @param timeTo    time range end (inclusive)
     * @return an iterator over key-value pairs {@code <timestamp, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If {@code null} is used for key.
     * @deprecated Use {@link #fetch(Object, Instant, Instant)} instead
     */
    @Deprecated
    WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo);

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
     * @param key       the key to fetch
     * @param from      time range start (inclusive)
     * @param to        time range end (inclusive)
     * @return an iterator over key-value pairs {@code <timestamp, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If {@code null} is used for key.
     * @throws IllegalArgumentException if duration is negative or can't be represented as {@code long milliseconds}
     */
    WindowStoreIterator<V> fetch(K key, Instant from, Instant to) throws IllegalArgumentException;

    /**
     * Get all the key-value pairs in the given key range and time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     *
     * @param from      the first key in the range
     * @param to        the last key in the range
     * @param timeFrom  time range start (inclusive)
     * @param timeTo    time range end (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If {@code null} is used for any key.
     * @deprecated Use {@link #fetch(Object, Object, Instant, Instant)} instead
     */
    @Deprecated
    KeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo);

    /**
     * Get all the key-value pairs in the given key range and time range from all the existing windows.
     * <p>
     * This iterator must be closed after use.
     *
     * @param from      the first key in the range
     * @param to        the last key in the range
     * @param fromTime  time range start (inclusive)
     * @param toTime    time range end (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If {@code null} is used for any key.
     * @throws IllegalArgumentException if duration is negative or can't be represented as {@code long milliseconds}
     */
    KeyValueIterator<Windowed<K>, V> fetch(K from, K to, Instant fromTime, Instant toTime)
        throws IllegalArgumentException;

    /**
    * Gets all the key-value pairs in the existing windows.
    *
    * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
    * @throws InvalidStateStoreException if the store is not initialized
    */
    KeyValueIterator<Windowed<K>, V> all();
    
    /**
     * Gets all the key-value pairs that belong to the windows within in the given time range.
     *
     * @param timeFrom the beginning of the time slot from which to search (inclusive)
     * @param timeTo   the end of the time slot from which to search (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException if {@code null} is used for any key
     * @deprecated Use {@link #fetchAll(Instant, Instant)} instead
     */
    @Deprecated
    KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom, long timeTo);

    /**
     * Gets all the key-value pairs that belong to the windows within in the given time range.
     *
     * @param from the beginning of the time slot from which to search (inclusive)
     * @param to   the end of the time slot from which to search (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException if {@code null} is used for any key
     * @throws IllegalArgumentException if duration is negative or can't be represented as {@code long milliseconds}
     */
    KeyValueIterator<Windowed<K>, V> fetchAll(Instant from, Instant to) throws IllegalArgumentException;
}
