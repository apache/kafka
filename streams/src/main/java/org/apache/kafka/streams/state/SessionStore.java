/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;

/**
 * Interface for storing the aggregated values of sessions
 * @param <K>   type of the record keys
 * @param <AGG> type of the aggregated values
 */
public interface SessionStore<K, AGG> extends StateStore, ReadOnlySessionStore<K, AGG> {

    /**
     * Fetch any sessions with the matching key and the sessions end is &le earliestEndTime and the sessions
     * start is &ge latestStartTime
     */
    KeyValueIterator<Windowed<K>, AGG> findSessions(final K key, long earliestSessionEndTime, final long latestSessionStartTime);

    /**
     * Remove the session aggregated with provided {@link Windowed} key from the store
     * @param sessionKey key of the session to remove
     */
    void remove(final Windowed<K> sessionKey);

    /**
     * Write the aggregated value for the provided key to the store
     * @param sessionKey key of the session to write
     * @param aggregate  the aggregated value for the session
     */
    void put(final Windowed<K> sessionKey, final AGG aggregate);
}
