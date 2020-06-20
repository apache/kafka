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


import org.apache.kafka.streams.kstream.Windowed;

/**
 * A session store that only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes
 * are expected.
 *
 * @param <K> the key type
 * @param <AGG> the aggregated value type
 */
public interface ReadOnlySessionStore<K, AGG> {
    /**
     * Retrieve all aggregated sessions for the provided key.
     * This iterator must be closed after use.
     *
     * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
     * available session to the newest/latest session.
     *
     * @param    key record key to find aggregated session values for
     * @return   KeyValueIterator containing all sessions for the provided key.
     * @throws   NullPointerException If null is used for key.
     *
     */
    KeyValueIterator<Windowed<K>, AGG> fetch(final K key);

    /**
     * Retrieve all aggregated sessions for the given range of keys.
     * This iterator must be closed after use.
     *
     * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
     * available session to the newest/latest session.
     *
     * @param    from first key in the range to find aggregated session values for
     * @param    to last key in the range to find aggregated session values for
     * @return   KeyValueIterator containing all sessions for the provided key.
     * @throws   NullPointerException If null is used for any of the keys.
     */
    KeyValueIterator<Windowed<K>, AGG> fetch(final K from, final K to);
}
