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

import java.time.Instant;

/**
 * A session store that supports storing record headers and only supports read operations.
 * Implementations should be thread-safe as concurrent reads and writes are expected.
 *
 * @param <K>   the key type
 * @param <AGG> the aggregated value type
 */
public interface ReadOnlySessionStoreWithHeaders<K, AGG> {

    /**
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime
     * and the sessions start is &le; latestSessionStartTime iterating from earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return iterator of sessions with the matching key and aggregated values with headers
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K key,
                                                                                     final long earliestSessionEndTime,
                                                                                     final long latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime
     * and the sessions start is &le; latestSessionStartTime iterating from earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return iterator of sessions with the matching key and aggregated values with headers
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K key,
                                                                                     final Instant earliestSessionEndTime,
                                                                                     final Instant latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime
     * and the sessions start is &le; latestSessionStartTime iterating from latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return backward iterator of sessions with the matching key and aggregated values with headers
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K key,
                                                                                             final long earliestSessionEndTime,
                                                                                             final long latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions with the matching key and the sessions end is &ge; earliestSessionEndTime
     * and the sessions start is &le; latestSessionStartTime iterating from latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key                    the key to return sessions for
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return backward iterator of sessions with the matching key and aggregated values with headers
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K key,
                                                                                             final Instant earliestSessionEndTime,
                                                                                             final Instant latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge;
     * earliestSessionEndTime and the sessions start is &le; latestSessionStartTime iterating from
     * earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return iterator of sessions with the matching keys and aggregated values with headers
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K keyFrom,
                                                                                     final K keyTo,
                                                                                     final long earliestSessionEndTime,
                                                                                     final long latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge;
     * earliestSessionEndTime and the sessions start is &le; latestSessionStartTime iterating from
     * earliest to latest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return iterator of sessions with the matching keys and aggregated values with headers
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> findSessions(final K keyFrom,
                                                                                     final K keyTo,
                                                                                     final Instant earliestSessionEndTime,
                                                                                     final Instant latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge;
     * earliestSessionEndTime and the sessions start is &le; latestSessionStartTime iterating from
     * latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return backward iterator of sessions with the matching keys and aggregated values with headers
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K keyFrom,
                                                                                             final K keyTo,
                                                                                             final long earliestSessionEndTime,
                                                                                             final long latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Fetch any sessions in the given range of keys and the sessions end is &ge;
     * earliestSessionEndTime and the sessions start is &le; latestSessionStartTime iterating from
     * latest to earliest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom                The first key that could be in the range
     * @param keyTo                  The last key that could be in the range
     * @param earliestSessionEndTime the end timestamp of the earliest session to search for
     * @param latestSessionStartTime the end timestamp of the latest session to search for
     * @return backward iterator of sessions with the matching keys and aggregated values with headers
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFindSessions(final K keyFrom,
                                                                                             final K keyTo,
                                                                                             final Instant earliestSessionEndTime,
                                                                                             final Instant latestSessionStartTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Get the aggregation and headers of key from a single session.
     *
     * @param key              the key to fetch
     * @param sessionStartTime start timestamp of the session
     * @param sessionEndTime   end timestamp of the session
     * @return The aggregation with headers or {@code null} if no session exists
     * @throws NullPointerException If {@code null} is used for any key.
     */
    default AggregationWithHeaders<AGG> fetchSession(final K key,
                                                     final long sessionStartTime,
                                                     final long sessionEndTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Get the aggregation and headers of key from a single session.
     *
     * @param key              the key to fetch
     * @param sessionStartTime start timestamp of the session
     * @param sessionEndTime   end timestamp of the session
     * @return The aggregation with headers or {@code null} if no session exists
     * @throws NullPointerException If {@code null} is used for any key.
     */
    default AggregationWithHeaders<AGG> fetchSession(final K key,
                                                     final Instant sessionStartTime,
                                                     final Instant sessionEndTime) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Retrieve all aggregated sessions for the provided key with headers.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key record key to find aggregated session values for
     * @return KeyValueIterator containing all sessions with headers for the provided key
     * @throws NullPointerException If null is used for key.
     */
    KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K key);

    /**
     * Retrieve all aggregated sessions for the provided key with headers, from newest to oldest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key record key to find aggregated session values for
     * @return backward KeyValueIterator containing all sessions with headers for the provided key
     * @throws NullPointerException If null is used for key.
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K key) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }

    /**
     * Retrieve all aggregated sessions for the given range of keys with headers.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom first key in the range
     * @param keyTo   last key in the range
     * @return KeyValueIterator containing all sessions with headers for the key range
     */
    KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> fetch(final K keyFrom, final K keyTo);

    /**
     * Retrieve all aggregated sessions for the given range of keys with headers, from newest to oldest.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom first key in the range
     * @param keyTo   last key in the range
     * @return backward KeyValueIterator containing all sessions with headers for the key range
     */
    default KeyValueIterator<Windowed<K>, AggregationWithHeaders<AGG>> backwardFetch(final K keyFrom, final K keyTo) {
        throw new UnsupportedOperationException(
            "This API is not supported by this implementation of ReadOnlySessionStoreWithHeaders.");
    }
}