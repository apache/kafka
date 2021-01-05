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
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime and the sessions
     * start is &le; latestSessionStartTime iterating from earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for, where iteration starts.
     * @param latestSessionStartTime the end timestamp of the latest session to search for, where iteration ends.
     * @return iterator of sessions with the matching key and aggregated values, from earliest to latest session time.
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                            final long earliestSessionEndTime,
                                                            final long latestSessionStartTime) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }

    /**
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime and the sessions
     * start is &le; latestSessionStartTime iterating from latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for, where iteration ends.
     * @param latestSessionStartTime the end timestamp of the latest session to search for, where iteration starts.
     * @return backward iterator of sessions with the matching key and aggregated values, from latest to earliest session time.
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(final K key,
                                                                    final long earliestSessionEndTime,
                                                                    final long latestSessionStartTime) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }

    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge; earliestSessionEndTime and the sessions
     * start is &le; latestSessionStartTime iterating from earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for, where iteration starts.
     * @param latestSessionStartTime the end timestamp of the latest session to search for, where iteration ends.
     * @return iterator of sessions with the matching keys and aggregated values, from earliest to latest session time.
     * @throws NullPointerException If null is used for any key.
     */
    default KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                            final K keyTo,
                                                            final long earliestSessionEndTime,
                                                            final long latestSessionStartTime) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }


    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge; earliestSessionEndTime and the sessions
     * start is &le; latestSessionStartTime iterating from latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for, where iteration ends.
     * @param latestSessionStartTime the end timestamp of the latest session to search for, where iteration starts.
     * @return backward iterator of sessions with the matching keys and aggregated values, from latest to earliest session time.
     * @throws NullPointerException If null is used for any key.
     */
    default KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(final K keyFrom,
                                                                    final K keyTo,
                                                                    final long earliestSessionEndTime,
                                                                    final long latestSessionStartTime) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }

    /**
     * Get the value of key from a single session.
     *
     * @param key       the key to fetch
     * @param startTime start timestamp of the session
     * @param endTime   end timestamp of the session
     * @return The value or {@code null} if no session associated with the key can be found
     * @throws NullPointerException If {@code null} is used for any key.
     */
    default AGG fetchSession(final K key, final long startTime, final long endTime) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }

    /**
     * Retrieve all aggregated sessions for the provided key.
     * This iterator must be closed after use.
     * <p>
     * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
     * available session to the newest/latest session.
     *
     * @param    key record key to find aggregated session values for
     * @return   KeyValueIterator containing all sessions for the provided key, from oldest to newest session.
     * @throws   NullPointerException If null is used for key.
     *
     */
    KeyValueIterator<Windowed<K>, AGG> fetch(final K key);

    /**
     * Retrieve all aggregated sessions for the provided key.
     * This iterator must be closed after use.
     * <p>
     * For each key, the iterator guarantees ordering of sessions, starting from the newest/latest
     * available session to the oldest/earliest session.
     *
     * @param key record key to find aggregated session values for
     * @return backward KeyValueIterator containing all sessions for the provided key, from newest to oldest session.
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AGG> backwardFetch(final K key) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }

    /**
     * Retrieve all aggregated sessions for the given range of keys.
     * This iterator must be closed after use.
     * <p>
     * For each key, the iterator guarantees ordering of sessions, starting from the oldest/earliest
     * available session to the newest/latest session.
     *
     * @param    from first key in the range to find aggregated session values for
     * @param    to last key in the range to find aggregated session values for
     * @return   KeyValueIterator containing all sessions for the provided key, from oldest to newest session.
     * @throws   NullPointerException If null is used for any of the keys.
     */
    KeyValueIterator<Windowed<K>, AGG> fetch(final K from, final K to);

    /**
     * Retrieve all aggregated sessions for the given range of keys.
     * This iterator must be closed after use.
     * <p>
     * For each key, the iterator guarantees ordering of sessions, starting from the newest/latest
     * available session to the oldest/earliest session.
     *
     * @param from first key in the range to find aggregated session values for
     * @param to   last key in the range to find aggregated session values for
     * @return backward KeyValueIterator containing all sessions for the provided key, from newest to oldest session.
     * @throws NullPointerException If null is used for any of the keys.
     */
    default KeyValueIterator<Windowed<K>, AGG> backwardFetch(final K from, final K to) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of ReadOnlySessionStore.");
    }
}
