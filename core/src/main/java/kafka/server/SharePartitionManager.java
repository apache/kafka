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
package kafka.server;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import org.slf4j.LoggerFactory;

public class SharePartitionManager {

    private final static Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    // TODO: May be use ImplicitLinkedHashCollection.
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;
    private final ReplicaManager replicaManager;
    private final Time time;
    private final ShareFetchSessionCache cache;

    public SharePartitionManager(ReplicaManager replicaManager, Time time, ShareFetchSessionCache cache) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        partitionCacheMap = new ConcurrentHashMap<>();
    }

    // TODO: Move some part in share session context and change method signature to accept share
    //  partition data along TopicIdPartition.
    public CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions, String groupId) {
        log.debug("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
            topicIdPartitions, groupId, fetchParams);
        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();

        synchronized (this) {
            Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new HashMap<>();
            topicIdPartitions.forEach(topicIdPartition -> {
                // TODO: Fetch inflight and delivery count from config.
                SharePartition sharePartition = partitionCacheMap.computeIfAbsent(sharePartitionKey(groupId, topicIdPartition), k -> new SharePartition(100, 5));
                topicPartitionData.put(topicIdPartition, new FetchRequest.PartitionData(
                    topicIdPartition.topicId(),
                    sharePartition.nextFetchOffset(),
                    -1,
                    Integer.MAX_VALUE,
                    Optional.empty()));
            });

            replicaManager.fetchMessages(
                fetchParams,
                CollectionConverters.asScala(
                    topicPartitionData.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue())).collect(
                        Collectors.toList())
                ),
                QuotaFactory.UnboundedQuota$.MODULE$,
                responsePartitionData -> {
                    List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = CollectionConverters.asJava(
                        responsePartitionData);
                    Map<TopicIdPartition, PartitionData> result = new HashMap<>();
                    responseData.forEach(data -> {
                        TopicIdPartition topicIdPartition = data._1;
                        FetchPartitionData fetchPartitionData = data._2;

                        PartitionData partitionData = new PartitionData()
                            .setPartitionIndex(topicIdPartition.partition())
                            .setRecords(fetchPartitionData.records)
                            .setErrorCode(fetchPartitionData.error.code())
//                        .setAcquiredRecords(fetchPartitionData.records)
                            .setAcknowledgeErrorCode(Errors.NONE.code());

                        SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
                        sharePartition.update(fetchPartitionData);

                        result.put(topicIdPartition, partitionData);
                        future.complete(result);
                    });
                    return BoxedUnit.UNIT;
                });
        }
        return future;
    }

    public CompletableFuture<ShareAcknowledgeResponseData> acknowledge(String groupId, TopicIdPartition topicIdPartition,
                                                                       List<ShareAcknowledgeRequestData.AcknowledgementBatch> ackBatches) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    private SharePartition sharePartition(SharePartitionKey sharePartitionKey) {
        return partitionCacheMap.getOrDefault(sharePartitionKey, null);
    }

    // TODO: Function requires an in depth implementation in the future. For now, it returns a new share session everytime
    public ShareSession session(Time time, String memberId, ShareFetchRequest request) {
        return new ShareSession(memberId, new ImplicitLinkedHashCollection<>(),
                time.milliseconds(), time.milliseconds(), ShareFetchMetadata.nextEpoch(ShareFetchMetadata.INITIAL_EPOCH));
    }

    public ShareFetchContext newContext(Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> forgottenTopics,
                                        Map<Uuid, String> topicNames, ShareFetchMetadata reqMetadata) {
        ShareFetchContext context;
        if (reqMetadata.isFull()) {
            String removedFetchSessionStr = "";
            // TODO: We will handle the case of INVALID_MEMBER_ID once we have a clear definition for it
            /*
            if (!Objects.equals(reqMetadata.memberId(), ShareFetchMetadata.INVALID_MEMBER_ID)) {
                // Any session specified in a FULL share fetch request will be closed.
                throw new UnsupportedOperationException("Not implemented yet");
            }
             */
            String suffix = "";
            if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                suffix = " Will not try to create a new session.";
                context = new SessionlessShareFetchContext(shareFetchData);
            } else {
                context = new FullShareFetchContext(time, cache, reqMetadata, shareFetchData);
            }
        } else {
            // TODO: Implement getting ShareFetchContext using ShareFetchSession cache
            throw new UnsupportedOperationException("Not implemented yet");
        }
        return context;
    }

    // TODO: Define share session class.
    public static class ShareSession {

        private final String id;
        private final ImplicitLinkedHashCollection<SharePartitionManager.CachedPartition> partitionMap;
        private final long creationMs;
        private final long lastUsedMs;
        private final int epoch;

        /**
         * The share session.
         * Each share session is protected by its own lock, which must be taken before mutable
         * fields are read or modified.  This includes modification of the share session partition map.
         *
         * @param id                 The unique share session ID.
         * @param partitionMap       The CachedPartitionMap.
         * @param creationMs         The time in milliseconds when this share session was created.
         * @param lastUsedMs         The last used time in milliseconds. This should only be updated by
         *                           ShareSessionCache#touch.
         * @param epoch              The share session sequence number.
         */
        public ShareSession(String id, ImplicitLinkedHashCollection<CachedPartition> partitionMap,
                            long creationMs, long lastUsedMs, int epoch) {
            this.id = id;
            this.partitionMap = partitionMap;
            this.creationMs = creationMs;
            this.lastUsedMs = lastUsedMs;
            this.epoch = epoch;
        }
    }

    /**
     * The share fetch context for a sessionless share fetch request.
     */
    public static class SessionlessShareFetchContext extends ShareFetchContext {
        private final Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        public SessionlessShareFetchContext(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.log = LoggerFactory.getLogger(SessionlessShareFetchContext.class);
            this.shareFetchData = shareFetchData;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Sessionless fetch context returning" + partitionsToLogString(updates.keySet()));
            return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            return shareFetchData;
        }
    }

    /**
     * The fetch context for a full share fetch request.
     */
    // TODO: Implement FullShareFetchContext when you have share sessions available
    public static class FullShareFetchContext extends ShareFetchContext {

        private Time time;
        private ShareFetchSessionCache cache;
        private ShareFetchMetadata reqMetadata;
        private Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        /**
         * @param time               The clock to use.
         * @param cache              The share fetch session cache.
         * @param reqMetadata        The request metadata.
         * @param shareFetchData     The share partition data from the share fetch request.
         */
        public FullShareFetchContext(Time time, ShareFetchSessionCache cache, ShareFetchMetadata reqMetadata,
                                     Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.log = LoggerFactory.getLogger(FullShareFetchContext.class);
            this.time = time;
            this.cache = cache;
            this.reqMetadata = reqMetadata;
            this.shareFetchData = shareFetchData;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    /**
     * The share fetch context for a share fetch request that had a session error.
     */
    // TODO: Implement ShareSessionErrorContext when you have share sessions available
    public static class ShareSessionErrorContext extends ShareFetchContext {

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            return new HashMap<>();
        }
    }

    /**
     * The share fetch context for an incremental share fetch request.
     */
    // TODO: Implement IncrementalFetchContext when you have share sessions available
    public static class IncrementalFetchContext extends ShareFetchContext {

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    // Visible for testing
    static class SharePartitionKey {
        private final String groupId;
        private final TopicIdPartition topicIdPartition;

        public SharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
            this.groupId = Objects.requireNonNull(groupId);
            this.topicIdPartition = Objects.requireNonNull(topicIdPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, topicIdPartition);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            else if (obj == null || getClass() != obj.getClass())
                return false;
            else {
                SharePartitionKey that = (SharePartitionKey) obj;
                return groupId.equals(that.groupId) && Objects.equals(topicIdPartition, that.topicIdPartition);
            }
        }
    }

    /*
     * Caches share fetch sessions.
     *
     * See tryEvict for an explanation of the cache eviction strategy.
     *
     * The ShareFetchSessionCache is thread-safe because all of its methods are synchronized.
     * Note that individual share fetch sessions have their own locks which are separate from the
     * ShareFetchSessionCache lock.  In order to avoid deadlock, the ShareFetchSessionCache lock
     * must never be acquired while an individual ShareFetchSession lock is already held.
     */
    // TODO: Implement ShareFetchSessionCache class
    public static class ShareFetchSessionCache {
        private final int maxEntries;
        private final long evictionMs;

        public ShareFetchSessionCache(int maxEntries, long evictionMs) {
            this.maxEntries = maxEntries;
            this.evictionMs = evictionMs;
        }
    }

    /*
     * A cached partition.
     *
     * The broker maintains a set of these objects for each share fetch session.
     * When a share fetch request is made, any partitions which are not explicitly
     * enumerated in the fetch request are loaded from the cache.  Similarly, when an
     * share fetch response is being prepared, any partitions that have not changed and
     * do not have errors are left out of the response.
     *
     * We store many of these objects, so it is important for them to be memory-efficient.
     * That is why we store topic and partition separately rather than storing a TopicPartition
     * object. The TP object takes up more memory because it is a separate JVM object, and
     * because it stores the cached hash code in memory.
     *
     */
    public static class CachedPartition implements ImplicitLinkedHashCollection.Element {
        private final String topic;
        private final Uuid topicId;
        private final int partition, maxBytes;
        private final Optional<Integer> leaderEpoch;

        private int cachedNext = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int cachedPrev = ImplicitLinkedHashCollection.INVALID_INDEX;

        private CachedPartition(String topic, Uuid topicId, int partition, int maxBytes, Optional<Integer> leaderEpoch) {
            this.topic = topic;
            this.topicId = topicId;
            this.partition = partition;
            this.maxBytes = maxBytes;
            this.leaderEpoch = leaderEpoch;
        }

        private CachedPartition(String topic, Uuid topicId, int partition) {
            this(topic, topicId, partition, -1, Optional.empty());
        }

        public CachedPartition(TopicIdPartition topicIdPartition) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition());
        }

        public CachedPartition(TopicIdPartition topicIdPartition, ShareFetchRequest.SharePartitionData reqData) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), reqData.maxBytes,
                    reqData.currentLeaderEpoch);
        }

        @Override
        public int prev() {
            return cachedPrev;
        }

        @Override
        public void setPrev(int prev) {
            cachedPrev = prev;
        }

        @Override
        public int next() {
            return cachedNext;
        }

        @Override
        public void setNext(int next) {
            cachedNext = next;
        }
    }
}
