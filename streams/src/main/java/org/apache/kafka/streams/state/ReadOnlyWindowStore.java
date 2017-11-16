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

/**
 * A window store that only supports read operations
 * Implementations should be thread-safe as concurrent reads and writes
 * are expected.
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface ReadOnlyWindowStore<K, V> {

    /**
     * Get all the key-value pairs with the given key and the time range from all
     * the existing windows.
     *
     * This iterator must be closed after use.
     * <p>
     * The time range is inclusive and applies to the starting timestamp of the window.
     * For example, if we have the following windows:
     * <p>
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
     * three windows from the table above, i.e., all those where 10 <= start time <= 20.
     * <p>
     * For each key, the iterator guarantees ordering of windows, starting from the oldest/earliest
     * available window to the newest/latest window.
     *
     * @return an iterator over key-value pairs {@code <timestamp, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If null is used for key.
     */
    WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo);

    /**
     * Get all the key-value pairs in the given key range and time range from all
     * the existing windows.
     *
     * This iterator must be closed after use.
     *
     * @param from      the first key in the range
     * @param to        the last key in the range
     * @param timeFrom  time range start (inclusive)
     * @param timeTo    time range end (inclusive)
     * @return an iterator over windowed key-value pairs {@code <Windowed<K>, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException If null is used for any key.
     */
    KeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo);
}
