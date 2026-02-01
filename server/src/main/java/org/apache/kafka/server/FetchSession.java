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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The fetch session.
 * <p>
 * Each fetch session is protected by its own lock, which must be taken before mutable
 * fields are read or modified. This includes modification of the session partition map.
 */
public class FetchSession {
    public static final String NUM_INCREMENTAL_FETCH_SESSIONS = "NumIncrementalFetchSessions";
    public static final String NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED = "NumIncrementalFetchPartitionsCached";
    public static final String INCREMENTAL_FETCH_SESSIONS_EVICTIONS_PER_SEC = "IncrementalFetchSessionEvictionsPerSec";
    static final String EVICTIONS = "evictions";

    /**
     * This is used by the FetchSessionCache to store the last known size of this session.
     * If this is -1, the Session is not in the cache.
     */
    private int cachedSize = -1;

    private final int id;
    private final boolean privileged;
    private final ImplicitLinkedHashCollection<CachedPartition> partitionMap;
    private final boolean usesTopicIds;
    private final long creationMs;
    private volatile long lastUsedMs;
    private volatile int epoch;

    /**
     * The fetch session.
     *
     * @param id                 The unique fetch session ID.
     * @param privileged         True if this session is privileged.  Sessions created by followers
     *                           are privileged; session created by consumers are not.
     * @param partitionMap       The CachedPartitionMap.
     * @param usesTopicIds       True if this session is using topic IDs
     * @param creationMs         The time in milliseconds when this session was created.
     * @param lastUsedMs         The last used time in milliseconds.  This should only be updated by
     *                           FetchSessionCache#touch.
     * @param epoch              The fetch session sequence number.
     */
    public FetchSession(int id,
                        boolean privileged,
                        ImplicitLinkedHashCollection<CachedPartition> partitionMap,
                        boolean usesTopicIds,
                        long creationMs,
                        long lastUsedMs,
                        int epoch) {
        this.id = id;
        this.privileged = privileged;
        this.partitionMap = partitionMap;
        this.usesTopicIds = usesTopicIds;
        this.creationMs = creationMs;
        this.lastUsedMs = lastUsedMs;
        this.epoch = epoch;
    }

    public static String partitionsToLogString(Collection<TopicIdPartition> partitions, boolean traceEnabled) {
        return traceEnabled
            ? "(" + String.join(", ", partitions.toString()) + ")"
            : partitions.size() + " partition(s)";
    }

    public synchronized ImplicitLinkedHashCollection<CachedPartition> partitionMap() {
        return partitionMap;
    }

    public int id() {
        return id;
    }

    public boolean privileged() {
        return privileged;
    }

    public boolean usesTopicIds() {
        return usesTopicIds;
    }

    public long creationMs() {
        return creationMs;
    }

    public int epoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public long lastUsedMs() {
        return lastUsedMs;
    }

    public void setLastUsedMs(long lastUsedMs) {
        this.lastUsedMs = lastUsedMs;
    }

    public synchronized int cachedSize() {
        return cachedSize;
    }

    public synchronized void setCachedSize(int cachedSize) {
        this.cachedSize = cachedSize;
    }

    public synchronized int size() {
        return partitionMap.size();
    }

    public synchronized boolean isEmpty() {
        return partitionMap.isEmpty();
    }

    public synchronized LastUsedKey lastUsedKey() {
        return new LastUsedKey(lastUsedMs, id);
    }

    public synchronized EvictableKey evictableKey() {
        return new EvictableKey(privileged, cachedSize, id);
    }

    public synchronized FetchMetadata metadata() {
        return new FetchMetadata(id, epoch);
    }

    public synchronized Optional<Long> getFetchOffset(TopicIdPartition topicIdPartition) {
        return Optional.ofNullable(partitionMap.find(new CachedPartition(topicIdPartition)))
            .map(partition -> partition.fetchOffset);
    }

    // Update the cached partition data based on the request.
    public synchronized List<List<TopicIdPartition>> update(Map<TopicIdPartition, FetchRequest.PartitionData> fetchData,
                                                            List<TopicIdPartition> toForget) {
        List<TopicIdPartition> added = new ArrayList<>();
        List<TopicIdPartition> updated = new ArrayList<>();
        List<TopicIdPartition> removed = new ArrayList<>();

        fetchData.forEach((topicPart, reqData) -> {
            CachedPartition cachedPartitionKey = new CachedPartition(topicPart, reqData);
            CachedPartition cachedPart = partitionMap.find(cachedPartitionKey);
            if (cachedPart == null) {
                partitionMap.mustAdd(cachedPartitionKey);
                added.add(topicPart);
            } else {
                cachedPart.updateRequestParams(reqData);
                updated.add(topicPart);
            }
        });

        toForget.forEach(p -> {
            if (partitionMap.remove(new CachedPartition(p)))
                removed.add(p);
        });

        return List.of(added, updated, removed);
    }

    @Override
    public String toString() {
        synchronized (this) {
            return "FetchSession(id=" + id +
                ", privileged=" + privileged +
                ", partitionMap.size=" + partitionMap.size() +
                ", usesTopicIds=" + usesTopicIds +
                ", creationMs=" + creationMs +
                ", lastUsedMs=" + lastUsedMs +
                ", epoch=" + epoch + ")";
        }
    }

    /**
     * A cached partition.
     * <p>
     * The broker maintains a set of these objects for each incremental fetch session.
     * When an incremental fetch request is made, any partitions which are not explicitly
     * enumerated in the fetch request are loaded from the cache.  Similarly, when an
     * incremental fetch response is being prepared, any partitions that have not changed and
     * do not have errors are left out of the response.
     * <p>
     * We store many of these objects, so it is important for them to be memory-efficient.
     * That is why we store topic and partition separately rather than storing a TopicPartition
     * object.  The TP object takes up more memory because it is a separate JVM object, and
     * because it stores the cached hash code in memory.
     * <p>
     * Note that fetcherLogStartOffset is the LSO of the follower performing the fetch, whereas
     * localLogStartOffset is the log start offset of the partition on this broker.
     */
    public static class CachedPartition implements ImplicitLinkedHashCollection.Element {

        private volatile int cachedNext = ImplicitLinkedHashCollection.INVALID_INDEX;
        private volatile int cachedPrev = ImplicitLinkedHashCollection.INVALID_INDEX;

        private String topic;
        private final Uuid topicId;
        private final int partition;
        private volatile int maxBytes;
        private volatile long fetchOffset;
        private long highWatermark;
        private Optional<Integer> leaderEpoch;
        private volatile long fetcherLogStartOffset;
        private long localLogStartOffset;
        private Optional<Integer> lastFetchedEpoch;

        public CachedPartition(String topic, Uuid topicId, int partition) {
            this(topic, topicId, partition, -1, -1, -1, Optional.empty(), -1, -1, Optional.empty());
        }

        public CachedPartition(TopicIdPartition part) {
            this(part.topic(), part.topicId(), part.partition());
        }

        public CachedPartition(TopicIdPartition part, FetchRequest.PartitionData reqData) {
            this(part.topic(), part.topicId(), part.partition(), reqData.maxBytes, reqData.fetchOffset, -1,
                reqData.currentLeaderEpoch, reqData.logStartOffset, -1, reqData.lastFetchedEpoch);
        }

        public CachedPartition(TopicIdPartition part, FetchRequest.PartitionData reqData, FetchResponseData.PartitionData respData) {
            this(part.topic(), part.topicId(), part.partition(), reqData.maxBytes, reqData.fetchOffset, respData.highWatermark(),
                reqData.currentLeaderEpoch, reqData.logStartOffset, respData.logStartOffset(), reqData.lastFetchedEpoch);
        }

        public CachedPartition(String topic,
                               Uuid topicId,
                               int partition,
                               int maxBytes,
                               long fetchOffset,
                               long highWatermark,
                               Optional<Integer> leaderEpoch,
                               long fetcherLogStartOffset,
                               long localLogStartOffset,
                               Optional<Integer> lastFetchedEpoch) {
            this.topic = topic;
            this.topicId = topicId;
            this.partition = partition;
            this.maxBytes = maxBytes;
            this.fetchOffset = fetchOffset;
            this.highWatermark = highWatermark;
            this.leaderEpoch = leaderEpoch;
            this.fetcherLogStartOffset = fetcherLogStartOffset;
            this.localLogStartOffset = localLogStartOffset;
            this.lastFetchedEpoch = lastFetchedEpoch;
        }

        @Override
        public int next() {
            return cachedNext;
        }

        @Override
        public void setNext(int next) {
            this.cachedNext = next;
        }

        @Override
        public int prev() {
            return cachedPrev;
        }

        @Override
        public void setPrev(int prev) {
            this.cachedPrev = prev;
        }

        public String topic() {
            return topic;
        }

        public Uuid topicId() {
            return topicId;
        }

        public int partition() {
            return partition;
        }

        public FetchRequest.PartitionData reqData() {
            return new FetchRequest.PartitionData(topicId, fetchOffset, fetcherLogStartOffset, maxBytes, leaderEpoch, lastFetchedEpoch);
        }

        public void updateRequestParams(FetchRequest.PartitionData reqData) {
            // Update our cached request parameters.
            maxBytes = reqData.maxBytes;
            fetchOffset = reqData.fetchOffset;
            fetcherLogStartOffset = reqData.logStartOffset;
            leaderEpoch = reqData.currentLeaderEpoch;
            lastFetchedEpoch = reqData.lastFetchedEpoch;
        }

        public void maybeResolveUnknownName(Map<Uuid, String> topicNames) {
            if (topic == null)
                topic = topicNames.get(topicId);
        }

        /**
         * Determine whether the specified cached partition should be included in the FetchResponse we send back to
         * the fetcher and update it if requested.
         * <p>
         * This function should be called while holding the appropriate session lock.
         *
         * @param respData partition data
         * @param updateResponseData if set to true, update this CachedPartition with new request and response data.
         * @return True if this partition should be included in the response; false if it can be omitted.
         */
        public boolean maybeUpdateResponseData(FetchResponseData.PartitionData respData, boolean updateResponseData) {
            // Check the response data.
            boolean mustRespond = false;
            if (FetchResponse.recordsSize(respData) > 0) {
                // Partitions with new data are always included in the response.
                mustRespond = true;
            }
            if (highWatermark != respData.highWatermark()) {
                mustRespond = true;
                if (updateResponseData)
                    highWatermark = respData.highWatermark();
            }
            if (localLogStartOffset != respData.logStartOffset()) {
                mustRespond = true;
                if (updateResponseData)
                    localLogStartOffset = respData.logStartOffset();
            }
            if (FetchResponse.isPreferredReplica(respData)) {
                // If the broker computed a preferred read replica, we need to include it in the response
                mustRespond = true;
            }
            if (respData.errorCode() != Errors.NONE.code()) {
                // Partitions with errors are always included in the response.
                // We also set the cached highWatermark to an invalid offset, -1.
                // This ensures that when the error goes away, we re-send the partition.
                if (updateResponseData)
                    highWatermark = -1;
                mustRespond = true;
            }
            if (FetchResponse.isDivergingEpoch(respData)) {
                // Partitions with diverging epoch are always included in response to trigger truncation.
                mustRespond = true;
            }

            return mustRespond;
        }

        /**
         * We have different equality checks depending on whether topic IDs are used.
         * This means we need a different hash function as well. We use name to calculate the hash if the ID is zero and unused.
         * Otherwise, we use the topic ID in the hash calculation.
         *
         * @return the hash code for the CachedPartition depending on what request version we are using.
         */
        @Override
        public int hashCode() {
            if (topicId != Uuid.ZERO_UUID)
                return (31 * partition) + topicId.hashCode();
            else
                return (31 * partition) + topic.hashCode();
        }

        /**
         * We have different equality checks depending on whether topic IDs are used.
         * <p>
         * This is because when we use topic IDs, a partition with a given ID and an unknown name is the same as a partition with that
         * ID and a known name. This means we can only use topic ID and partition when determining equality.
         * <p>
         * On the other hand, if we are using topic names, all IDs are zero. This means we can only use topic name and partition
         * when determining equality.
         */
        @Override
        public boolean equals(Object that) {
            if (that instanceof CachedPartition part) {
                boolean condition;
                if (this.topicId != Uuid.ZERO_UUID)
                    condition = this.partition == part.partition && this.topicId.equals(part.topicId);
                else
                    condition = this.partition == part.partition && this.topic.equals(part.topic);

                return this == part || condition;
            }

            return false;
        }

        @Override
        public String toString() {
            synchronized (this) {
                return "CachedPartition(topic=" + topic +
                    ", topicId=" + topicId +
                    ", partition=" + partition +
                    ", maxBytes=" + maxBytes +
                    ", fetchOffset=" + fetchOffset +
                    ", highWatermark=" + highWatermark +
                    ", fetcherLogStartOffset=" + fetcherLogStartOffset +
                    ", localLogStartOffset=" + localLogStartOffset  +
                    ")";
            }
        }
    }

    public record LastUsedKey(long lastUsedMs, int id) implements Comparable<LastUsedKey> {
        @Override
        public int compareTo(LastUsedKey other) {
            if (this.lastUsedMs != other.lastUsedMs)
                return Long.compare(this.lastUsedMs, other.lastUsedMs);

            return Integer.compare(this.id, other.id);
        }
    }

    public record EvictableKey(boolean privileged, int size, int id) implements Comparable<EvictableKey> {
        @Override
        public int compareTo(EvictableKey other) {
            if (this.privileged != other.privileged)
                return Boolean.compare(this.privileged, other.privileged);

            if (this.size != other.size)
                return Integer.compare(this.size, other.size);

            return Integer.compare(this.id, other.id);
        }
    }

    public static class FetchSessionCache {
        // Changing the package or class name may cause incompatibility with existing code and metrics configuration
        private static final String METRICS_PACKAGE = "kafka.server";
        private static final String METRICS_CLASS_NAME = "FetchSessionCache";

        public static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup(METRICS_PACKAGE, METRICS_CLASS_NAME);
        public static final AtomicInteger COUNTER = new AtomicInteger(0);

        private final List<FetchSessionCacheShard> cacheShards;

        public FetchSessionCache(List<FetchSessionCacheShard> cacheShards) {
            this.cacheShards = cacheShards;

            // Set up metrics.
            FetchSessionCache.METRICS_GROUP.newGauge(FetchSession.NUM_INCREMENTAL_FETCH_SESSIONS,
                () -> cacheShards.stream().mapToInt(FetchSessionCacheShard::size).sum());
            FetchSessionCache.METRICS_GROUP.newGauge(FetchSession.NUM_INCREMENTAL_FETCH_PARTITIONS_CACHED,
                () -> cacheShards.stream().mapToLong(FetchSessionCacheShard::totalPartitions).sum());
        }

        public FetchSessionCacheShard getCacheShard(int sessionId) {
            int shard = sessionId / cacheShards.get(0).sessionIdRange();
            // This assumes that cacheShards is sorted by shardNum
            return cacheShards.get(shard);
        }

        /**
         * @return The shard in round-robin
         */
        public FetchSessionCacheShard getNextCacheShard() {
            int shardNum = Utils.toPositive(FetchSessionCache.COUNTER.getAndIncrement()) % size();
            return cacheShards.get(shardNum);
        }

        public int size() {
            return cacheShards.size();
        }
    }
}
