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

import org.apache.kafka.common.header.Headers;

/**
 * Interface for storing the aggregated values of fixed-size time windows with headers support.
 * <p>
 * In contrast to a {@link TimestampedWindowStore} that stores windowedKey-(value/timestamp) pairs,
 * a {@code TimestampedWindowStoreWithHeaders} stores windowedKey-(value/timestamp/headers) tuples.
 * <p>
 * While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
 * to store the last update timestamp of the corresponding window, and headers preserve the record
 * metadata (e.g., schema registry information).
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface TimestampedWindowStoreWithHeaders<K, V> extends WindowStore<K, ValueTimestampHeaders<V>> {

    /**
     * Convenience method to put a key-value-timestamp pair with headers into the window store.
     * <p>
     * This is a convenience wrapper around {@link #put(Object, Object, long)} that constructs
     * the {@link ValueTimestampHeaders} instance for you.
     *
     * @param key                  The key to associate the value to
     * @param value                The value; can be null
     * @param windowStartTimestamp The timestamp of the beginning of the window
     * @param timestamp            The record timestamp
     * @param headers              The Kafka headers associated with the record
     */
    default void put(final K key, final V value, final long windowStartTimestamp, final long timestamp, final Headers headers) {
        put(key, ValueTimestampHeaders.make(value, timestamp, headers), windowStartTimestamp);
    }
}
