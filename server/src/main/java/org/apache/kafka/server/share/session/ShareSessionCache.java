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

import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.network.ConnectionDisconnectListener;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ShareGroupListener;

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
    /**
     * The listener for connection disconnect events for the client.
     */
    private final ConnectionDisconnectListener connectionDisconnectListener;
    /**
     * Map of session key to ShareSession.
     */
    private final Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();
    /**
     * Map of groupId to number of members in the group.
     */
    private final Map<String, Integer> numMembersPerGroup = new HashMap<>();
    /**
     * The map to store the client connection id to session key. This is used to remove the session
     * from the cache when the respective client disconnects.
     */
    private final Map<String, ShareSessionKey> connectionIdToSessionMap;
    /**
     * The listener for share group events. This is used to notify the listener when the group members
     * change.
     */
    private ShareGroupListener shareGroupListener;

    private final int maxEntries;
    private long numPartitions = 0;

    @SuppressWarnings("this-escape")
    public ShareSessionCache(int maxEntries) {
        this.maxEntries = maxEntries;
        // Register metrics for ShareSessionCache.
        KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "ShareSessionCache");
        metricsGroup.newGauge(SHARE_SESSIONS_COUNT, this::size);
        metricsGroup.newGauge(SHARE_PARTITIONS_COUNT, this::totalPartitions);
        this.connectionIdToSessionMap = new HashMap<>();
        this.connectionDisconnectListener = new ClientConnectionDisconnectListener();
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

    /**
     * Remove all the share sessions from cache.
     */
    public synchronized void removeAllSessions() {
        sessions.clear();
        numMembersPerGroup.clear();
        numPartitions = 0;
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
     * Maybe remove the session and notify listeners. This is called when the connection is disconnected
     * for the client. The session may have already been removed by the client as part of final epoch,
     * hence check if the session is still present in the cache.
     *
     * @param key The share session key.
     */
    public synchronized void maybeRemoveAndNotifyListeners(ShareSessionKey key) {
        ShareSession session = get(key);
        if (session != null) {
            // Notify the share group listener that member has left the group. Notify listener prior
            // removing the session from the cache to ensure that the listener has access to the session
            // while it is still in the cache.
            if (shareGroupListener != null) {
                shareGroupListener.onMemberLeave(key.groupId(), key.memberId());
            }
            // As session is not null hence it's removed as part of connection disconnect. Hence,
            // update the evictions metric.
            evictionsMeter.mark();
            // Try removing session if not already removed. The listener might have removed the session
            // already.
            remove(session);
        }
        // Notify the share group listener if the group is empty. This should be checked regardless
        // session is evicted by connection disconnect or client's final epoch.
        int numMembers = numMembersPerGroup.getOrDefault(key.groupId(), 0);
        if (numMembers == 0) {
            // Remove the group from the map as it is empty.
            numMembersPerGroup.remove(key.groupId());
            if (shareGroupListener != null) {
                shareGroupListener.onGroupEmpty(key.groupId());
            }
        }
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
            numMembersPerGroup.compute(session.key().groupId(), (k, v) -> v != null ? v - 1 : 0);
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
     * @param clientConnectionId - The client connection id.
     * @return - The session key if the session was created, or null if the session was not created.
     */
    public synchronized ShareSessionKey maybeCreateSession(
        String groupId,
        String memberId,
        ImplicitLinkedHashCollection<CachedSharePartition> partitionMap,
        String clientConnectionId
    ) {
        if (sessions.size() < maxEntries) {
            ShareSession session = new ShareSession(new ShareSessionKey(groupId, memberId), partitionMap,
                ShareRequestMetadata.nextEpoch(ShareRequestMetadata.INITIAL_EPOCH));
            sessions.put(session.key(), session);
            updateNumPartitions(session);
            numMembersPerGroup.compute(session.key().groupId(), (k, v) -> v != null ? v + 1 : 1);
            connectionIdToSessionMap.put(clientConnectionId, session.key());
            return session.key();
        }
        return null;
    }

    public ConnectionDisconnectListener connectionDisconnectListener() {
        return connectionDisconnectListener;
    }

    public synchronized void registerShareGroupListener(ShareGroupListener shareGroupListener) {
        this.shareGroupListener = shareGroupListener;
    }

    // Visible for testing.
    Meter evictionsMeter() {
        return evictionsMeter;
    }

    // Visible for testing.
    Integer numMembers(String groupId) {
        return numMembersPerGroup.get(groupId);
    }

    private final class ClientConnectionDisconnectListener implements ConnectionDisconnectListener {

        // When the client disconnects, the corresponding session should be removed from the cache.
        @Override
        public void onDisconnect(String connectionId) {
            ShareSessionKey shareSessionKey = connectionIdToSessionMap.remove(connectionId);
            if (shareSessionKey != null) {
                // Try removing session and notify listeners. The session might already be removed
                // as part of final epoch from client, so we need to check if the session is still
                // present in the cache.
                maybeRemoveAndNotifyListeners(shareSessionKey);
            }
        }
    }
}
