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

import org.apache.kafka.streams.processor.StateStore;

/**
 * A windowed store interface extending {@link StateStore}.
 *
 * In contrast to {@link WindowStore} that stores plain key-value pairs,
 * {@code WindowWithTimestampStore} stores key-(value/timestamp) pairs.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface WindowWithTimestampStore<K, V> extends WindowStore<K, ValueAndTimestamp<V>> {

    /**
     * Put a key-value pair and its timestamp into the window with given window start timestamp
     * @param key The key to associate the value to
     * @param value The value, can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @param timestamp the timestamp of the key-value pair; ignored if {@code value} is {@code null}
     * @param windowStartTimestamp The timestamp of the beginning of the window to put the key/value/timestamp into
     * @throws NullPointerException if the given key is {@code null}
     */
    void put(K key, V value, long timestamp, long windowStartTimestamp);
}
