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

import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;

import java.time.Instant;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * Interface for storing the aggregated values of sessions.
 * <p>
 * The key is internally represented as {@link Windowed Windowed&lt;K&gt;} that comprises the plain
 * key and the {@link Window} that represents window start- and end-timestamp.
 * <p>
 * If two sessions are merged, a new session with new start- and end-timestamp must be inserted into
 * the store while the two old sessions must be deleted.
 *
 * @param <K>   type of the record keys
 * @param <AGG> type of the aggregated values
 */
public interface SessionStore<K, AGG> extends StateStore, ReadOnlySessionStore<K, AGG> {

    /**
     * Return all the session window entries that ends between the specified range (both ends are inclusive).
     * This function would be used to retrieve all closed and immutable windows.
     *
     * @param earliestSessionEndTime earliest session end time to search from, inclusive
     * @param latestSessionEndTime latest session end time to search to, inclusive
     */
    default KeyValueIterator<Windowed<K>, AGG> findSessions(final long earliestSessionEndTime,
                                                            final long latestSessionEndTime) {
        throw new UnsupportedOperationException(
                "This API is not supported by this implementation of SessionStore.");
    }

    @Override
    default KeyValueIterator<Windowed<K>, AGG> findSessions(final K key,
                                                            final Instant earliestSessionEndTime,
                                                            final Instant latestSessionStartTime) {
        return findSessions(
            key,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    @Override
    default KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(final K key,
                                                                    final Instant earliestSessionEndTime,
                                                                    final Instant latestSessionStartTime) {
        return backwardFindSessions(
            key,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    default KeyValueIterator<Windowed<K>, AGG> findSessions(final K keyFrom,
                                                            final K keyTo,
                                                            final Instant earliestSessionEndTime,
                                                            final Instant latestSessionStartTime) {
        return findSessions(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    default KeyValueIterator<Windowed<K>, AGG> backwardFindSessions(final K keyFrom,
                                                                    final K keyTo,
                                                                    final Instant earliestSessionEndTime,
                                                                    final Instant latestSessionStartTime) {
        return backwardFindSessions(
            keyFrom,
            keyTo,
            ApiUtils.validateMillisecondInstant(earliestSessionEndTime,
                prepareMillisCheckFailMsgPrefix(earliestSessionEndTime, "earliestSessionEndTime")),
            ApiUtils.validateMillisecondInstant(latestSessionStartTime,
                prepareMillisCheckFailMsgPrefix(latestSessionStartTime, "latestSessionStartTime")));
    }

    default AGG fetchSession(final K key,
                             final Instant sessionStartTime,
                             final Instant sessionEndTime) {
        return fetchSession(key,
            ApiUtils.validateMillisecondInstant(sessionStartTime,
                prepareMillisCheckFailMsgPrefix(sessionStartTime, "sessionStartTime")),
            ApiUtils.validateMillisecondInstant(sessionEndTime,
                prepareMillisCheckFailMsgPrefix(sessionEndTime, "sessionEndTime")));
    }

    /**
     * Remove the session aggregated with provided {@link Windowed} key from the store
     *
     * @param sessionKey key of the session to remove
     * @throws NullPointerException If null is used for sessionKey.
     */
    void remove(final Windowed<K> sessionKey);

    /**
     * Write the aggregated value for the provided key to the store
     *
     * @param sessionKey key of the session to write
     * @param aggregate  the aggregated value for the session, it can be null; if the serialized
     *                   bytes are also null it is interpreted as deletes
     * @throws NullPointerException If null is used for sessionKey.
     */
    void put(final Windowed<K> sessionKey, final AGG aggregate);
}
