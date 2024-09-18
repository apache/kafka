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

package org.apache.kafka.server.share.session;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.server.share.CachedSharePartition;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Caches share sessions.
 * <p>
 * See tryEvict for an explanation of the cache eviction strategy.
 * <p>
 * The ShareSessionCache is thread-safe because all of its methods are synchronized.
 * Note that individual share sessions have their own locks which are separate from the
 * ShareSessionCache lock.  In order to avoid deadlock, the ShareSessionCache lock
 * must never be acquired while an individual ShareSession lock is already held.
 */
public class ShareSessionCache {
    private final int maxEntries;
    private final long evictionMs;
    private long numPartitions = 0;

    // A map of session key to ShareSession.
    private final Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();

    // Maps last used times to sessions.
    private final TreeMap<LastUsedKey, ShareSession> lastUsed = new TreeMap<>();

    public ShareSessionCache(int maxEntries, long evictionMs) {
        this.maxEntries = maxEntries;
        this.evictionMs = evictionMs;
    }

    /**
     * Get a session by session key.
     *
     * @param key The share session key.
     * @return The session, or None if no such session was found.
     */
    public synchronized ShareSession get(ShareSessionKey key) {
        return sessions.getOrDefault(key, null);
    }

    /**
     * Get the number of entries currently in the share session cache.
     */
    public synchronized int size() {
        return sessions.size();
    }

    public synchronized long totalPartitions() {
        return numPartitions;
    }

    public synchronized ShareSession remove(ShareSessionKey key) {
        ShareSession session = get(key);
        if (session != null)
            return remove(session);
        return null;
    }

    /**
     * Remove an entry from the session cache.
     *
     * @param session The session.
     * @return The removed session, or None if there was no such session.
     */
    public synchronized ShareSession remove(ShareSession session) {
        synchronized (session) {
            lastUsed.remove(session.lastUsedKey());
        }
        ShareSession removeResult = sessions.remove(session.key());
        if (removeResult != null) {
            numPartitions = numPartitions - session.cachedSize();
        }
        return removeResult;
    }

    /**
     * Update a session's position in the lastUsed tree.
     *
     * @param session  The session.
     * @param now      The current time in milliseconds.
     */
    public synchronized void touch(ShareSession session, long now) {
        synchronized (session) {
            // Update the lastUsed map.
            lastUsed.remove(session.lastUsedKey());
            session.lastUsedMs(now);
            lastUsed.put(session.lastUsedKey(), session);

            int oldSize = session.cachedSize();
            if (oldSize != -1) {
                numPartitions = numPartitions - oldSize;
            }
            session.cachedSize(session.size());
            numPartitions = numPartitions + session.cachedSize();
        }
    }

    /**
     * Try to evict an entry from the session cache.
     * <p>
     * A proposed new element A may evict an existing element B if:
     * B is considered "stale" because it has been inactive for a long time.
     *
     * @param now        The current time in milliseconds.
     * @return           True if an entry was evicted; false otherwise.
     */
    public synchronized boolean tryEvict(long now) {
        // Try to evict an entry which is stale.
        Map.Entry<LastUsedKey, ShareSession> lastUsedEntry = lastUsed.firstEntry();
        if (lastUsedEntry == null) {
            return false;
        } else if (now - lastUsedEntry.getKey().lastUsedMs() > evictionMs) {
            ShareSession session = lastUsedEntry.getValue();
            remove(session);
            return true;
        }
        return false;
    }

    /**
     * Maybe create a new session and add it to the cache.
     * @param groupId - The group id in the share fetch request.
     * @param memberId - The member id in the share fetch request.
     * @param now - The current time in milliseconds.
     * @param partitionMap - The topic partitions to be added to the session.
     * @return - The session key if the session was created, or null if the session was not created.
     */
    public synchronized ShareSessionKey maybeCreateSession(String groupId, Uuid memberId, long now, ImplicitLinkedHashCollection<CachedSharePartition> partitionMap) {
        if (sessions.size() < maxEntries || tryEvict(now)) {
            ShareSession session = new ShareSession(new ShareSessionKey(groupId, memberId), partitionMap,
                    now, now, ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH));
            sessions.put(session.key(), session);
            touch(session, now);
            return session.key();
        }
        return null;
    }
}
