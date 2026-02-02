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
package org.apache.kafka.server;

import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.FetchSession.EvictableKey;
import org.apache.kafka.server.FetchSession.LastUsedKey;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.common.requests.FetchMetadata.INITIAL_EPOCH;
import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * Caches fetch sessions.
 * <p>
 * See {@link #tryEvict} for an explanation of the cache eviction strategy.
 * <p>
 * The FetchSessionCache is thread-safe because all of its methods are synchronized.
 * Note that individual fetch sessions have their own locks which are separate from the
 * FetchSessionCache lock.  In order to avoid deadlock, the FetchSessionCache lock
 * must never be acquired while an individual FetchSession lock is already held.
 */
public class FetchSessionCacheShard {
    private final Logger logger;

    private long numPartitions = 0;

    /**
     * A map of session ID to FetchSession.
     */
    private final Map<Integer, FetchSession> sessions = new HashMap<>();

    /**
     * Maps last used times to sessions.
     */
    private final TreeMap<LastUsedKey, FetchSession> lastUsed = new TreeMap<>();

    /**
     * A map containing sessions which can be evicted by both privileged and unprivileged sessions.
     */
    private final TreeMap<EvictableKey, FetchSession> evictableByAll = new TreeMap<>();

    /**
     * A map containing sessions which can be evicted by privileged sessions.
     */
    private final TreeMap<EvictableKey, FetchSession> evictableByPrivileged = new TreeMap<>();

    /**
     * This metric is shared across all shards because newMeter returns an existing metric
     * if one exists with the same name. It's safe for concurrent use because Meter is thread-safe.
     */
    private final Meter evictionsMeter = FetchSession.FetchSessionCache.METRICS_GROUP.newMeter(
        FetchSession.INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC,
        FetchSession.EVICTIONS,
        TimeUnit.SECONDS
    );

    private final int maxEntries;
    private final long evictionMs;
    private final int sessionIdRange;
    private final int shardNum;

    /**
     * @param maxEntries The maximum number of entries that can be in the cache
     * @param evictionMs The minimum time that an entry must be unused in order to be evictable
     * @param sessionIdRange The number of sessionIds each cache shard handles.
     *                       For a given instance, Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange always holds.
     * @param shardNum Identifier for this shard
     */
    public FetchSessionCacheShard(int maxEntries,
                                  long evictionMs,
                                  int sessionIdRange,
                                  int shardNum) {
        this.maxEntries = maxEntries;
        this.evictionMs = evictionMs;
        this.sessionIdRange = sessionIdRange;
        this.shardNum = shardNum;
        this.logger = new LogContext("[Shard " + shardNum + "] ").logger(FetchSessionCacheShard.class);
    }

    public int sessionIdRange() {
        return sessionIdRange;
    }

    // Only for testing
    public Meter evictionsMeter() {
        return evictionsMeter;
    }

    /**
     * Get a session by session ID.
     *
     * @param sessionId  The session ID.
     * @return           The session, or None if no such session was found.
     */
    public synchronized Optional<FetchSession> get(int sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

    /**
     * Get the number of entries currently in the fetch session cache.
     */
    public synchronized int size() {
        return sessions.size();
    }

    /**
     * Get the total number of cached partitions.
     */
    public synchronized long totalPartitions() {
        return numPartitions;
    }

    /**
     * Creates a new random session ID.  The new session ID will be positive and unique on this broker.
     *
     * @return   The new session ID.
     */
    public synchronized int newSessionId() {
        int id;
        do {
            id = ThreadLocalRandom.current().nextInt(Math.max(1, shardNum * sessionIdRange), (shardNum + 1) * sessionIdRange);
        } while (sessions.containsKey(id) || id == INVALID_SESSION_ID);

        return id;
    }

    /**
     * Try to create a new session.
     *
     * @param now                The current time in milliseconds.
     * @param privileged         True if the new entry we are trying to create is privileged.
     * @param size               The number of cached partitions in the new entry we are trying to create.
     * @param usesTopicIds       True if this session should use topic IDs.
     * @param createPartitions   A callback function which creates the map of cached partitions and the mapping from
     *                           topic name to topic ID for the topics.
     * @return                   If we created a session, the ID; INVALID_SESSION_ID otherwise.
     */
    public synchronized int maybeCreateSession(long now,
                                               boolean privileged,
                                               int size,
                                               boolean usesTopicIds,
                                               Supplier<ImplicitLinkedHashCollection<FetchSession.CachedPartition>> createPartitions) {
        // If there is room, create a new session entry.
        if ((sessions.size() < maxEntries) || tryEvict(privileged, new EvictableKey(privileged, size, 0), now)) {
            ImplicitLinkedHashCollection<FetchSession.CachedPartition> partitionMap = createPartitions.get();
            FetchSession session = new FetchSession(newSessionId(), privileged, partitionMap, usesTopicIds,
                now, now, FetchMetadata.nextEpoch(INITIAL_EPOCH));
            logger.debug("Created fetch session {}", session);
            sessions.put(session.id(), session);
            touch(session, now);

            return session.id();
        } else {
            logger.debug("No fetch session created for privileged={}, size={}.", privileged, size);
            return INVALID_SESSION_ID;
        }
    }

    /**
     * Try to evict an entry from the session cache.
     * <p>
     * A proposed new element A may evict an existing element B if:
     * 1. A is privileged and B is not, or
     * 2. B is considered "stale" because it has been inactive for a long time, or
     * 3. A contains more partitions than B, and B is not recently created.
     * <p>
     * Prior to KAFKA-9401, the session cache was not sharded, and we looked at all
     * entries while considering those eligible for eviction. Now eviction is done
     * by considering entries on a per-shard basis.
     *
     * @param privileged True if the new entry we would like to add is privileged
     * @param key        The EvictableKey for the new entry we would like to add
     * @param now        The current time in milliseconds
     * @return           True if an entry was evicted; false otherwise.
     */
    private synchronized boolean tryEvict(boolean privileged, EvictableKey key, long now) {
        // Try to evict an entry which is stale.
        Map.Entry<LastUsedKey, FetchSession> lastUsedEntry = lastUsed.firstEntry();
        if (lastUsedEntry == null) {
            logger.trace("There are no cache entries to evict.");
            return false;
        } else if (now - lastUsedEntry.getKey().lastUsedMs() > evictionMs) {
            FetchSession session = lastUsedEntry.getValue();
            logger.trace("Evicting stale FetchSession {}.", session.id());
            remove(session);
            evictionsMeter.mark();
            return true;
        } else {
            // If there are no stale entries, check the first evictable entry.
            // If it is less valuable than our proposed entry, evict it.
            TreeMap<EvictableKey, FetchSession> map = privileged ? evictableByPrivileged : evictableByAll;
            Map.Entry<EvictableKey, FetchSession> evictableEntry = map.firstEntry();
            if (evictableEntry == null) {
                logger.trace("No evictable entries found.");
                return false;
            } else if (key.compareTo(evictableEntry.getKey()) < 0) {
                logger.trace("Can't evict {} with {}", evictableEntry.getKey(), key);
                return false;
            } else {
                logger.trace("Evicting {} with {}.", evictableEntry.getKey(), key);
                remove(evictableEntry.getValue());
                evictionsMeter.mark();
                return true;
            }
        }
    }

    public synchronized Optional<FetchSession> remove(int sessionId) {
        Optional<FetchSession> session = get(sessionId);
        return session.isPresent() ? remove(session.get()) : Optional.empty();
    }

    /**
     * Remove an entry from the session cache.
     *
     * @param session  The session.
     *
     * @return         The removed session, or None if there was no such session.
     */
    public synchronized Optional<FetchSession> remove(FetchSession session) {
        EvictableKey evictableKey;
        synchronized (session) {
            lastUsed.remove(session.lastUsedKey());
            evictableKey = session.evictableKey();
        }

        evictableByAll.remove(evictableKey);
        evictableByPrivileged.remove(evictableKey);
        Optional<FetchSession> removeResult = Optional.ofNullable(sessions.remove(session.id()));

        if (removeResult.isPresent())
            numPartitions = numPartitions - session.cachedSize();

        return removeResult;
    }

    /**
     * Update a session's position in the lastUsed and evictable trees.
     *
     * @param session  The session
     * @param now      The current time in milliseconds
     */
    public synchronized void touch(FetchSession session, long now) {
        synchronized (session) {
            // Update the lastUsed map.
            lastUsed.remove(session.lastUsedKey());
            session.setLastUsedMs(now);
            lastUsed.put(session.lastUsedKey(), session);

            int oldSize = session.cachedSize();
            if (oldSize != -1) {
                EvictableKey oldEvictableKey = session.evictableKey();
                evictableByPrivileged.remove(oldEvictableKey);
                evictableByAll.remove(oldEvictableKey);
                numPartitions = numPartitions - oldSize;
            }
            session.setCachedSize(session.size());
            EvictableKey newEvictableKey = session.evictableKey();

            if ((!session.privileged()) || (now - session.creationMs() > evictionMs))
                evictableByPrivileged.put(newEvictableKey, session);

            if (now - session.creationMs() > evictionMs)
                evictableByAll.put(newEvictableKey, session);

            numPartitions = numPartitions + session.cachedSize();
        }
    }
}
