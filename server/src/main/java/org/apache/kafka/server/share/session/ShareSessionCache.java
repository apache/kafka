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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.share.CachedSharePartition;

import com.yammer.metrics.core.Meter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    // Visible for testing.
    static final String SHARE_SESSIONS_COUNT = "ShareSessionsCount";
    // Visible for testing.
    static final String SHARE_PARTITIONS_COUNT = "SharePartitionsCount";
    private static final String SHARE_SESSION_EVICTIONS_PER_SEC = "ShareSessionEvictionsPerSec";

    /**
     * Metric for the rate of eviction of share sessions.
     */
    private final Meter evictionsMeter;

    private final int maxEntries;
    private long numPartitions = 0;

    // A map of session key to ShareSession.
    private final Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();

    @SuppressWarnings("this-escape")
    public ShareSessionCache(int maxEntries) {
        this.maxEntries = maxEntries;
        // Register metrics for ShareSessionCache.
        KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "ShareSessionCache");
        metricsGroup.newGauge(SHARE_SESSIONS_COUNT, this::size);
        metricsGroup.newGauge(SHARE_PARTITIONS_COUNT, this::totalPartitions);
        this.evictionsMeter = metricsGroup.newMeter(SHARE_SESSION_EVICTIONS_PER_SEC, "evictions", TimeUnit.SECONDS);
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
        ShareSession removeResult = sessions.remove(session.key());
        if (removeResult != null) {
            numPartitions = numPartitions - session.cachedSize();
        }
        return removeResult;
    }

    /**
     * Update the size of the cache by updating the total number of share partitions.
     *
     * @param session  The session.
     */
    public synchronized void updateNumPartitions(ShareSession session) {
        numPartitions += session.updateCachedSize();
    }

    /**
     * Maybe create a new session and add it to the cache.
     * @param groupId - The group id in the share fetch request.
     * @param memberId - The member id in the share fetch request.
     * @param partitionMap - The topic partitions to be added to the session.
     * @return - The session key if the session was created, or null if the session was not created.
     */
    public synchronized ShareSessionKey maybeCreateSession(String groupId, Uuid memberId, ImplicitLinkedHashCollection<CachedSharePartition> partitionMap) {
        if (sessions.size() < maxEntries) {
            ShareSession session = new ShareSession(new ShareSessionKey(groupId, memberId), partitionMap,
                ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH));
            sessions.put(session.key(), session);
            updateNumPartitions(session);
            return session.key();
        }
        return null;
    }

    // Visible for testing.
    Meter evictionsMeter() {
        return evictionsMeter;
    }
}
