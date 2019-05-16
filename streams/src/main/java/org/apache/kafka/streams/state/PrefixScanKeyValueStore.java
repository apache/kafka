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
 * A key-value store that only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes are expected.
 *
 * Please note that this contract defines the thread-safe read functionality only; it does not
 * guarantee anything about whether the actual instance is writable by another thread, or
 * whether it uses some locking mechanism under the hood. For this reason, making dependencies
 * between the read and write operations on different StateStore instances can cause concurrency
 * problems like deadlock.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface PrefixScanKeyValueStore<K, V> {
    /**
     * Return an iterator over all keys that share the same prefix byte[].
     *
     * @param prefix The byte[] prefix to perform the prefixScan for
     * @throws NullPointerException If {@code null} is used for prefix.
     */
    KeyValueIterator<K, V> prefixScan(K prefix);
}
