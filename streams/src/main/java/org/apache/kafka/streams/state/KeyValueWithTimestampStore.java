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

/**
 * A key-(value/timestamp) store that supports put/get/delete and range queries.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface KeyValueWithTimestampStore<K, V> extends KeyValueStore<K, ValueAndTimestamp<V>> {

    /**
     * Update the value and timestamp associated with this key.
     *
     * @param key The key to associate the value to
     * @param value The value to update, it can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @param timestamp the timestamp to be stored next to the value; ignored if {@code value} is {@code null}
     * @throws NullPointerException If {@code null} is used for key.
     */
    void put(K key, V value, long timestamp);

    /**
     * Update the value associated with this key, unless a value is already associated with the key.
     *
     * @param key The key to associate the value to
     * @param value The value to update, it can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @param timestamp the timestamp to be stored next to the value; ignored if {@code value} is {@code null}
     * @return The old value and timestamp or {@code null} if there is no such key.
     * @throws NullPointerException If {@code null} is used for key.
     */
    ValueAndTimestamp<V> putIfAbsent(K key, V value, long timestamp);

}