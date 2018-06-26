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
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface WindowStore<K, V> extends StateStore, ReadOnlyWindowStore<K, V> {

    /**
     * Put a key-value pair with the current record time as the timestamp
     * into the corresponding window
     * @param key The key to associate the value to
     * @param value The value to update, it can be null;
     *              if the serialized bytes are also null it is interpreted as deletes
     * @throws NullPointerException If null is used for key.
     */
    void put(K key, V value);

    /**
     * Put a key-value pair with the given timestamp into the corresponding window
     * @param key The key to associate the value to
     * @param value The value; can be null
     * @throws NullPointerException If null is used for key.
     */
    void put(K key, V value, long timestamp);
}
