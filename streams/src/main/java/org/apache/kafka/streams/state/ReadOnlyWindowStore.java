/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * A window store that only supports read operations
 * Implementations should be thread-safe as concurrent reads and writes
 * are expected.
 * @param <K> Type of keys
 * @param <V> Type of values
 */
@InterfaceStability.Unstable
public interface ReadOnlyWindowStore<K, V> {

    /**
     * Get all the key-value pairs with the given key and the time range from all
     * the existing windows.
     *
     * @return an iterator over key-value pairs {@code <timestamp, value>}
     * @throws InvalidStateStoreException if the store is not initialized
     */
    WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo);
}
