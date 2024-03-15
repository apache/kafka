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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.TreeMap;

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
    private final ShareSessionCache cache;
    private final ConcurrentLinkedQueue<ShareFetchPartitionData> fetchQueue;
    private final AtomicBoolean processFetchQueueLock;

    public SharePartitionManager(ReplicaManager replicaManager, Time time, ShareSessionCache cache) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        partitionCacheMap = new ConcurrentHashMap<>();
        fetchQueue = new ConcurrentLinkedQueue<>();
        this.processFetchQueueLock = new AtomicBoolean(false);
    }

    public void releaseProcessFetchQueueLock() {
        processFetchQueueLock.set(false);
    }

    public boolean isProcessFetchQueueLockAvailable() {
        return !processFetchQueueLock.get();
    }

    public boolean maybeAcquireProcessFetchQueueLock() {
        return processFetchQueueLock.compareAndSet(false, true);
    }

    // TODO: Move some part in share session context and change method signature to accept share
    //  partition data along TopicIdPartition.
    public CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions,
        Map<TopicIdPartition, Integer> partitionMaxBytes) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
                topicIdPartitions, groupId, fetchParams);
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetchPartitionData shareFetchPartitionData = new ShareFetchPartitionData(fetchParams, groupId, memberId,
                topicIdPartitions, future, partitionMaxBytes);
        fetchQueue.add(shareFetchPartitionData);
        maybeProcessFetchQueue();
        return future;
    }

    /**
     * Recursive function to process all the fetch requests present inside the fetch queue
     */
    public void maybeProcessFetchQueue() {
        if (maybeAcquireProcessFetchQueueLock()) {
            try {
                ShareFetchPartitionData shareFetchPartitionData = fetchQueue.poll();
                Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new HashMap<>();
                shareFetchPartitionData.topicIdPartitions.forEach(topicIdPartition -> {
                    Integer partitionMaxBytes = shareFetchPartitionData.partitionMaxBytes.getOrDefault(topicIdPartition, 0);

                    // TODO: Fetch inflight and delivery count from config.
                    SharePartition sharePartition = partitionCacheMap.computeIfAbsent(sharePartitionKey(
                                    shareFetchPartitionData.groupId, topicIdPartition),
                            k -> new SharePartition(shareFetchPartitionData.groupId, topicIdPartition, 100, 5));
                    // we add the share partition to the list of partitions to be fetched only if we can acquire the fetch lock on it.
                    if (sharePartition.maybeAcquireFetchLock()) {
                        topicPartitionData.put(topicIdPartition, new FetchRequest.PartitionData(
                                topicIdPartition.topicId(),
                                sharePartition.nextFetchOffset(),
                                -1,
                                partitionMaxBytes,
                                Optional.empty()));
                        sharePartition.releaseFetchLock();
                    }
                });
                log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
                        topicPartitionData, shareFetchPartitionData.groupId, shareFetchPartitionData.fetchParams);

                replicaManager.fetchMessages(
                        shareFetchPartitionData.fetchParams,
                        CollectionConverters.asScala(
                                topicPartitionData.entrySet().stream().map(entry ->
                                        new Tuple2<>(entry.getKey(), entry.getValue())).collect(Collectors.toList())
                        ),
                        QuotaFactory.UnboundedQuota$.MODULE$,
                        responsePartitionData -> {
                            List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = CollectionConverters.asJava(
                                    responsePartitionData);
                            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = new HashMap<>();
                            responseData.forEach(data -> {
                                TopicIdPartition topicIdPartition = data._1;
                                FetchPartitionData fetchPartitionData = data._2;

                                SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(shareFetchPartitionData.groupId, topicIdPartition));

                                if (sharePartition.maybeAcquireFetchLock()) {
                                     try {
                                        sharePartition.acquire(shareFetchPartitionData.memberId, fetchPartitionData)
                                                .whenComplete((acquiredRecords, throwable) -> {
                                                    ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                                                            .setPartitionIndex(topicIdPartition.partition());

                                                    if (throwable != null) {
                                                        partitionData.setErrorCode(Errors.forException(throwable).code());
                                                    } else {
                                                        // Maybe check if no records are acquired and we want to retry replica
                                                        // manager fetch. Depends on the share partition manager implementation,
                                                        // if we want parallel requests for the same share partition or not.
                                                        partitionData
                                                                .setPartitionIndex(topicIdPartition.partition())
                                                                .setRecords(fetchPartitionData.records)
                                                                .setErrorCode(fetchPartitionData.error.code())
                                                                .setAcquiredRecords(acquiredRecords)
                                                                .setAcknowledgeErrorCode(Errors.NONE.code());
                                                    }
                                                    result.put(topicIdPartition, partitionData);
                                                });
                                    } finally {
                                         sharePartition.releaseFetchLock();
                                    }
                                }
                            });
                            shareFetchPartitionData.future.complete(result);
                            // we are releasing the lock to move ahead with the next request in queue.
                            releaseProcessFetchQueueLock();
                            if (!fetchQueue.isEmpty())
                                maybeProcessFetchQueue();
                            return BoxedUnit.UNIT;
                        });
            } finally {
                if (isProcessFetchQueueLockAvailable())
                    releaseProcessFetchQueueLock();
            }
        }
    }

    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> acknowledge(
            String memberId,
            String groupId,
            Map<TopicIdPartition, List<SharePartition.AcknowledgementBatch>> acknowledgeTopics
    ) {
        log.debug("Acknowledge request for topicIdPartitions: {} with groupId: {}",
                acknowledgeTopics.keySet(), groupId);
        Map<TopicIdPartition, CompletableFuture<Errors>> futures = new HashMap<>();
        acknowledgeTopics.forEach((topicIdPartition, acknowledgePartitionBatches) -> {
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
            if (sharePartition != null) {
                synchronized (sharePartition) {
                    CompletableFuture<Errors> future = sharePartition.acknowledge(memberId, acknowledgePartitionBatches).thenApply(throwable -> {
                        if (throwable.isPresent()) {
                            return Errors.forException(throwable.get());
                        } else {
                            return Errors.NONE;
                        }

                    });
                    futures.put(topicIdPartition, future);
                }
            } else {
                futures.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.values().toArray(new CompletableFuture[futures.size()]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> {
                result.put(topicIdPartition, new ShareAcknowledgeResponseData.PartitionData()
                                .setPartitionIndex(topicIdPartition.partition())
                                .setErrorCode(future.join().code()));
            });
            return result;
        });
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    public Errors acknowledgeShareSessionCacheUpdate(String groupId, Uuid memberId, int reqEpoch) {
        Errors shareAcknowledgeRequestError = shareAcknowledgeError(groupId, memberId, reqEpoch);
        if (shareAcknowledgeRequestError != Errors.NONE)
            return shareAcknowledgeRequestError;
        ShareSessionKey key = shareSessionKey(groupId, memberId);
        ShareSession shareSession = cache.get(new ShareSessionKey(groupId, memberId));
        if (reqEpoch == ShareFetchMetadata.FINAL_EPOCH) {
            if (cache.remove(key) != null)
                log.info("Removed share session with key " + key);
            return Errors.NONE;
        }
        // Update session's position in the cache
        cache.touch(shareSession, time.milliseconds());
        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
        return Errors.NONE;
    }

    private Errors shareAcknowledgeError(String groupId, Uuid memberId, int reqEpoch) {
        if (reqEpoch == ShareFetchMetadata.INITIAL_EPOCH)
            return Errors.INVALID_SHARE_SESSION_EPOCH;
        else {
            ShareSessionKey key = shareSessionKey(groupId, memberId);
            ShareSession shareSession = cache.get(key);
            if (shareSession == null) {
                log.debug("Share session error for {}: no such share session found", key);
                return Errors.SHARE_SESSION_NOT_FOUND;
            }
            if (reqEpoch != shareSession.epoch && reqEpoch != ShareFetchMetadata.FINAL_EPOCH) {
                log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                        shareSession.epoch, reqEpoch);
                return Errors.INVALID_SHARE_SESSION_EPOCH;
            }
            return Errors.NONE;
        }
    }

    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> toForget,
                                        Map<Uuid, String> topicNames, ShareFetchMetadata reqMetadata) {
        ShareFetchContext context;
        if (reqMetadata.isFull()) {
            String removedFetchSessionStr = "";
            // TODO: We will handle the case of INVALID_MEMBER_ID once we have a clear definition for it
         //  if (!Objects.equals(reqMetadata.memberId(), ShareFetchMetadata.INVALID_MEMBER_ID)) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            if (cache.remove(key) != null)
                removedFetchSessionStr = "Removed share session with key " + key;

            if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                context = new FinalContext(shareFetchData);
            } else {
                context = new ShareSessionContext(time, cache, reqMetadata, shareFetchData);
                log.debug("Created a new ShareSessionContext with {} {}. A new share session will be started.",
                        partitionsToLogString(shareFetchData.keySet()), removedFetchSessionStr);
            }
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such share session found", key);
                    context = new ShareSessionErrorContext(Errors.SHARE_SESSION_NOT_FOUND);
                } else {
                    if (shareSession.epoch != reqMetadata.epoch()) {
                        log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                                shareSession.epoch, reqMetadata.epoch());
                        context = new ShareSessionErrorContext(Errors.INVALID_SHARE_SESSION_EPOCH);
                    } else {
                        Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> modifiedTopicIdPartitions = shareSession.update(
                                shareFetchData, toForget);
                        cache.touch(shareSession, time.milliseconds());
                        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
                        log.debug("Created a new ShareSessionContext for session key {}, epoch {}: " +
                                        "added {}, updated {}, removed {}", shareSession.key, shareSession.epoch,
                                partitionsToLogString(modifiedTopicIdPartitions.get(ModifiedTopicIdPartitionType.ADDED)),
                                partitionsToLogString(modifiedTopicIdPartitions.get(ModifiedTopicIdPartitionType.UPDATED)),
                                partitionsToLogString(modifiedTopicIdPartitions.get(ModifiedTopicIdPartitionType.REMOVED))
                        );
                        context = new ShareSessionContext(time, reqMetadata, shareSession);
                    }
                }
            }
        }
        return context;
    }

    String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    public static class ShareSession {

        private final ShareSessionKey key;
        private final ImplicitLinkedHashCollection<CachedSharePartition> partitionMap;
        private final long creationMs;
        private long lastUsedMs;
        private int epoch;

        // This is used by the ShareSessionCache to store the last known size of this session.
        // If this is -1, the Session is not in the cache.
        private int cachedSize = -1;

        /**
         * The share session.
         * Each share session is protected by its own lock, which must be taken before mutable
         * fields are read or modified.  This includes modification of the share session partition map.
         *
         * @param key                The share session key to identify the share session uniquely.
         * @param partitionMap       The CachedPartitionMap.
         * @param creationMs         The time in milliseconds when this share session was created.
         * @param lastUsedMs         The last used time in milliseconds. This should only be updated by
         *                           ShareSessionCache#touch.
         * @param epoch              The share session sequence number.
         */
        public ShareSession(ShareSessionKey key, ImplicitLinkedHashCollection<CachedSharePartition> partitionMap,
                            long creationMs, long lastUsedMs, int epoch) {
            this.key = key;
            this.partitionMap = partitionMap;
            this.creationMs = creationMs;
            this.lastUsedMs = lastUsedMs;
            this.epoch = epoch;
        }

        public int cachedSize() {
            synchronized (this) {
                return cachedSize;
            }
        }

        public long lastUsedMs() {
            synchronized (this) {
                return lastUsedMs;
            }
        }

        public Collection<CachedSharePartition> partitionMap() {
            synchronized (this) {
                return partitionMap;
            }
        }

        // Visible for testing
        public int epoch() {
            synchronized (this) {
                return epoch;
            }
        }

        public int size() {
            synchronized (this) {
                return partitionMap.size();
            }
        }

        public Boolean isEmpty() {
            synchronized (this) {
                return partitionMap.isEmpty();
            }
        }

        public LastUsedKey lastUsedKey() {
            synchronized (this) {
                return new LastUsedKey(key, lastUsedMs);
            }
        }

        public EvictableKey evictableKey() {
            synchronized (this) {
                return new EvictableKey(key, cachedSize);
            }
        }

        // Update the cached partition data based on the request.
        public Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> update(Map<TopicIdPartition,
                ShareFetchRequest.SharePartitionData> shareFetchData,
                           List<TopicIdPartition> toForget) {
            List<TopicIdPartition> added = new ArrayList<>();
            List<TopicIdPartition> updated = new ArrayList<>();
            List<TopicIdPartition> removed = new ArrayList<>();
            synchronized (this) {
                shareFetchData.forEach((topicIdPartition, sharePartitionData) -> {
                    CachedSharePartition cachedSharePartitionKey = new CachedSharePartition(topicIdPartition, sharePartitionData, true);
                    CachedSharePartition cachedPart = partitionMap.find(cachedSharePartitionKey);
                    if (cachedPart == null) {
                        partitionMap.mustAdd(cachedSharePartitionKey);
                        added.add(topicIdPartition);
                    } else {
                        cachedPart.updateRequestParams(sharePartitionData);
                        updated.add(topicIdPartition);
                    }
                });
                toForget.forEach(topicIdPartition -> {
                    if (partitionMap.remove(new CachedSharePartition(topicIdPartition)))
                        removed.add(topicIdPartition);
                });
            }
            Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> result = new HashMap<>();
            result.put(ModifiedTopicIdPartitionType.ADDED, added);
            result.put(ModifiedTopicIdPartitionType.UPDATED, updated);
            result.put(ModifiedTopicIdPartitionType.REMOVED, removed);
            return result;
        }

        public String toString() {
            return "ShareSession(" +
                    " key=" + key +
                    ", partitionMap=" + partitionMap +
                    ", creationMs=" + creationMs +
                    ", lastUsedMs=" + lastUsedMs +
                    ", epoch=" + epoch +
                    ", cachedSize=" + cachedSize +
                    ")";
        }
    }


    // Helper enum to return the possible type of modified list of TopicIdPartitions in cache
    public enum ModifiedTopicIdPartitionType {
        ADDED,
        UPDATED,
        REMOVED
    }

    // Helper class to return the erroneous partitions and valid partition data
    public static class ErroneousAndValidPartitionData {
        private final List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous;
        private final List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions;

        public ErroneousAndValidPartitionData(List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous,
                                              List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions) {
            this.erroneous = erroneous;
            this.validTopicIdPartitions = validTopicIdPartitions;
        }

        public ErroneousAndValidPartitionData(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            erroneous = new ArrayList<>();
            validTopicIdPartitions = new ArrayList<>();
            shareFetchData.forEach((topicIdPartition, sharePartitionData) -> {
                if (topicIdPartition.topic() == null) {
                    erroneous.add(new Tuple2<>(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)));
                } else {
                    validTopicIdPartitions.add(new Tuple2<>(topicIdPartition, sharePartitionData));
                }
            });
        }

        public List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous() {
            return erroneous;
        }

        public List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions() {
            return validTopicIdPartitions;
        }
    }

    /**
     * The share fetch context for a final share fetch request.
     */
    public static class FinalContext extends ShareFetchContext {
        private final Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        public FinalContext(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.log = LoggerFactory.getLogger(FinalContext.class);
            this.shareFetchData = shareFetchData;
        }

        public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData() {
            return shareFetchData;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
                                                         LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Final context returning" + partitionsToLogString(updates.keySet()));
            return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
            return new ErroneousAndValidPartitionData(shareFetchData);
        }
    }

    /**
     * The context for a share session fetch request.
     */
    public static class ShareSessionContext extends ShareFetchContext {

        private final Time time;
        private ShareSessionCache cache;
        private final ShareFetchMetadata reqMetadata;
        private Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;
        private final boolean isSubsequent;
        private ShareSession session;

        private final Logger log = LoggerFactory.getLogger(ShareSessionContext.class);

        /**
         * The share fetch context for the first request that starts a share session.
         *
         * @param time               The clock to use.
         * @param cache              The share session cache.
         * @param reqMetadata        The request metadata.
         * @param shareFetchData     The share partition data from the share fetch request.
         */
        public ShareSessionContext(Time time, ShareSessionCache cache, ShareFetchMetadata reqMetadata,
                                       Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.time = time;
            this.cache = cache;
            this.reqMetadata = reqMetadata;
            this.shareFetchData = shareFetchData;
            this.isSubsequent = false;
        }

        /**
         * The share fetch context for a subsequent request that utilizes an existing share session.
         *
         * @param time         The clock to use.
         * @param reqMetadata  The request metadata.
         * @param session      The subsequent fetch request session.
         */
        public ShareSessionContext(Time time, ShareFetchMetadata reqMetadata, ShareSession session) {
            this.time = time;
            this.reqMetadata = reqMetadata;
            this.session = session;
            this.isSubsequent = true;
        }

        public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData() {
            return shareFetchData;
        }

        public boolean isSubsequent() {
            return isSubsequent;
        }

        public ShareSession session() {
            return session;
        }

        @Override
        ShareFetchResponse throttleResponse(int throttleTimeMs) {
            if (!isSubsequent) {
                return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
                        Collections.emptyIterator(), Collections.emptyList()));
            } else {
                int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
                int sessionEpoch;
                synchronized (session) {
                    sessionEpoch = session.epoch;
                }
                if (sessionEpoch != expectedEpoch) {
                    log.debug("Subsequent share session {} expected epoch {}, but got {}. " +
                            "Possible duplicate request.", session.key, expectedEpoch, sessionEpoch);
                    return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.INVALID_SHARE_SESSION_EPOCH,
                            throttleTimeMs, Collections.emptyIterator(), Collections.emptyList()));
                } else
                    return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
                            Collections.emptyIterator(), Collections.emptyList()));
            }
        }

        // Iterator that goes over the given partition map and selects partitions that need to be included in the response.
        // If updateShareContextAndRemoveUnselected is set to true, the share context will be updated for the selected
        // partitions and also remove unselected ones as they are encountered.
        private class PartitionIterator implements Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> {
            private final Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator;
            private final boolean updateShareContextAndRemoveUnselected;
            private Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> nextElement;


            public PartitionIterator(Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator, boolean updateShareContextAndRemoveUnselected) {
                this.iterator = iterator;
                this.updateShareContextAndRemoveUnselected = updateShareContextAndRemoveUnselected;
            }

            @Override
            public boolean hasNext() {
                while ((nextElement == null) && iterator.hasNext()) {
                    Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = iterator.next();
                    TopicIdPartition topicPart = element.getKey();
                    ShareFetchResponseData.PartitionData respData = element.getValue();
                    CachedSharePartition cachedPart = session.partitionMap.find(new CachedSharePartition(topicPart));
                    boolean mustRespond = cachedPart.maybeUpdateResponseData(respData, updateShareContextAndRemoveUnselected);
                    if (mustRespond) {
                        nextElement = element;
                        if (updateShareContextAndRemoveUnselected && ShareFetchResponse.recordsSize(respData) > 0) {
                            // Session.partitionMap is of type ImplicitLinkedHashCollection<> which tracks the order of insertion of elements.
                            // Since, we are updating an element in this case, we need to perform a remove and then a mustAdd to maintain the correct order
                            session.partitionMap.remove(cachedPart);
                            session.partitionMap.mustAdd(cachedPart);
                        }
                    } else {
                        if (updateShareContextAndRemoveUnselected) {
                            iterator.remove();
                        }
                    }
                }
                return nextElement != null;
            }

            @Override
            public Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> next() {
                if (!hasNext()) throw new NoSuchElementException();
                Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = nextElement;
                nextElement = null;
                return element;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            if (!isSubsequent)
                return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
            else {
                synchronized (session) {
                    int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
                    if (session.epoch != expectedEpoch) {
                        return ShareFetchResponse.sizeOf(version, Collections.emptyIterator());
                    } else {
                        // Pass the partition iterator which updates neither the share fetch context nor the partition map.
                        return ShareFetchResponse.sizeOf(version, new PartitionIterator(updates.entrySet().iterator(), false));
                    }
                }
            }
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
                                                         LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            if (!isSubsequent) {
                ImplicitLinkedHashCollection<CachedSharePartition> cachedSharePartitions = new
                        ImplicitLinkedHashCollection<>(updates.size());
                updates.forEach((topicIdPartition, respData) -> {
                    ShareFetchRequest.SharePartitionData reqData = shareFetchData.get(topicIdPartition);
                    cachedSharePartitions.mustAdd(new CachedSharePartition(topicIdPartition, reqData, false));
                });
                ShareSessionKey responseShareSessionKey = cache.maybeCreateSession(groupId, memberId,
                        time.milliseconds(), updates.size(), cachedSharePartitions);
                log.debug("Share session context with key {} isSubsequent {} returning {}", responseShareSessionKey,
                        isSubsequent, partitionsToLogString(updates.keySet()));
                return new ShareFetchResponse(ShareFetchResponse.toMessage(
                        Errors.NONE, 0, updates.entrySet().iterator(), Collections.emptyList()));
            } else {
                int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
                int sessionEpoch;
                synchronized (session) {
                    sessionEpoch = session.epoch;
                }
                if (session.epoch != expectedEpoch) {
                    log.info("Subsequent share session {} expected epoch {}, but got {}. Possible duplicate request.",
                            session.key, expectedEpoch, sessionEpoch);
                    return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.INVALID_SHARE_SESSION_EPOCH,
                            0, Collections.emptyIterator(), Collections.emptyList()));
                } else {
                    // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
                    Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partitionIterator = new PartitionIterator(
                            updates.entrySet().iterator(), true);
                    while (partitionIterator.hasNext()) {
                        partitionIterator.next();
                    }
                    log.debug("Subsequent share session context with session key {} returning {}", session.key,
                            partitionsToLogString(updates.keySet()));
                    return new ShareFetchResponse(ShareFetchResponse.toMessage(
                            Errors.NONE, 0, updates.entrySet().iterator(), Collections.emptyList()));
                }
            }
        }

        @Override
        ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
            if (!isSubsequent) {
                return new ErroneousAndValidPartitionData(shareFetchData);
            } else {
                List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous = new ArrayList<>();
                List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> valid = new ArrayList<>();
                // Take the session lock and iterate over all the cached partitions.
                synchronized (session) {
                    session.partitionMap.forEach(cachedSharePartition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId, new
                                TopicPartition(cachedSharePartition.topic, cachedSharePartition.partition));
                        ShareFetchRequest.SharePartitionData reqData = cachedSharePartition.reqData();
                        if (topicIdPartition.topic() == null) {
                            erroneous.add(new Tuple2<>(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)));
                        } else {
                            valid.add(new Tuple2<>(topicIdPartition, reqData));
                        }
                    });
                    return new ErroneousAndValidPartitionData(erroneous, valid);
                }
            }
        }
    }

    /**
     * The share fetch context for a share fetch request that had a session error.
     */
    public static class ShareSessionErrorContext extends ShareFetchContext {
        private final Errors error;

        private final Logger log = LoggerFactory.getLogger(ShareSessionErrorContext.class);

        public ShareSessionErrorContext(Errors error) {
            this.error = error;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            return ShareFetchResponse.sizeOf(version, Collections.emptyIterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
                                                         LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Share session error context returning " + error);
            return new ShareFetchResponse(ShareFetchResponse.toMessage(error, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        SharePartitionManager.ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
            return new ErroneousAndValidPartitionData(new ArrayList<>(), new ArrayList<>());
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

    // visible for testing
    public static class ShareSessionKey {
        private final String groupId;
        private final Uuid memberId;

        public ShareSessionKey(String groupId, Uuid memberId) {
            this.groupId = Objects.requireNonNull(groupId);
            this.memberId = Objects.requireNonNull(memberId);
        }

        public Uuid memberId() {
            return memberId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, memberId);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            else if (obj == null || getClass() != obj.getClass())
                return false;
            else {
                ShareSessionKey that = (ShareSessionKey) obj;
                return groupId.equals(that.groupId) && Objects.equals(memberId, that.memberId);
            }
        }

        public String toString() {
            return "ShareSessionKey(" +
                    " groupId=" + groupId +
                    ", memberId=" + memberId +
                    ")";
        }
    }

    // visible for testing
    public static class LastUsedKey implements Comparable<LastUsedKey> {
        private final ShareSessionKey key;
        private final long lastUsedMs;

        public LastUsedKey(ShareSessionKey key, long lastUsedMs) {
            this.key = key;
            this.lastUsedMs = lastUsedMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, lastUsedMs);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            LastUsedKey other = (LastUsedKey) obj;
            return lastUsedMs == other.lastUsedMs && Objects.equals(key, other.key);
        }

        @Override
        public int compareTo(LastUsedKey other) {
            int res = Long.compare(lastUsedMs, other.lastUsedMs);
            if (res != 0)
                return res;
            else
                return Integer.compare(key.hashCode(), other.key.hashCode());
        }
    }

    public static class EvictableKey implements Comparable<EvictableKey> {
        private final ShareSessionKey key;
        private final int size;

        public EvictableKey(ShareSessionKey key, int size) {
            this.key = key;
            this.size = size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EvictableKey other = (EvictableKey) obj;
            return size == other.size && Objects.equals(key, other.key);
        }

        @Override
        public int compareTo(EvictableKey other) {
            int res = Integer.compare(size, other.size);
            if (res != 0)
                return res;
            else {
                return Integer.compare(key.hashCode(), other.key.hashCode());
            }
        }
    }

    /*
     * Caches share sessions.
     *
     * See tryEvict for an explanation of the cache eviction strategy.
     *
     * The ShareSessionCache is thread-safe because all of its methods are synchronized.
     * Note that individual share sessions have their own locks which are separate from the
     * ShareSessionCache lock.  In order to avoid deadlock, the ShareSessionCache lock
     * must never be acquired while an individual ShareSession lock is already held.
     */
    public static class ShareSessionCache {
        private final int maxEntries;
        private final long evictionMs;
        private long numPartitions = 0;

        // A map of session key to ShareSession.
        private Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();

        // Maps last used times to sessions.
        private TreeMap<LastUsedKey, ShareSession> lastUsed = new TreeMap<>();

        // A map containing sessions which can be evicted by sessions on basis of size
        private TreeMap<EvictableKey, ShareSession> evictable = new TreeMap<>();

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
        public ShareSession get(ShareSessionKey key) {
            synchronized (this) {
                return sessions.getOrDefault(key, null);
            }
        }

        /**
         * Get the number of entries currently in the share session cache.
         */
        public int size() {
            synchronized (this) {
                return sessions.size();
            }
        }

        public long totalPartitions() {
            synchronized (this) {
                return numPartitions;
            }
        }

        public ShareSession remove(ShareSessionKey key) {
            synchronized (this) {
                ShareSession session = get(key);
                if (session != null)
                    return remove(session);
                return null;
            }
        }

        /**
         * Remove an entry from the session cache.
         *
         * @param session The session.
         * @return The removed session, or None if there was no such session.
         */
        public ShareSession remove(ShareSession session) {
            synchronized (this) {
                EvictableKey evictableKey;
                synchronized (session) {
                    lastUsed.remove(session.lastUsedKey());
                    evictableKey = session.evictableKey();
                }
                evictable.remove(evictableKey);
                ShareSession removeResult = sessions.remove(session.key);
                if (removeResult != null) {
                    numPartitions = numPartitions - session.cachedSize();
                }
                return removeResult;
            }
        }

        /**
         * Update a session's position in the lastUsed and evictable trees.
         *
         * @param session  The session.
         * @param now      The current time in milliseconds.
         */
        public void touch(ShareSession session, long now) {
            synchronized (session) {
                // Update the lastUsed map.
                lastUsed.remove(session.lastUsedKey());
                session.lastUsedMs = now;
                lastUsed.put(session.lastUsedKey(), session);

                int oldSize = session.cachedSize;
                if (oldSize != -1) {
                    EvictableKey oldEvictableKey = session.evictableKey();
                    evictable.remove(oldEvictableKey);
                    numPartitions = numPartitions - oldSize;
                }
                session.cachedSize = session.size();
                EvictableKey newEvictableKey = session.evictableKey();
                if (now - session.creationMs > evictionMs) {
                    evictable.put(newEvictableKey, session);
                }
                numPartitions = numPartitions + session.cachedSize;
            }
        }

        /**
         * Try to evict an entry from the session cache.
         *
         * A proposed new element A may evict an existing element B if:
         * B is considered "stale" because it has been inactive for a long time.
         *
         * @param now        The current time in milliseconds.
         * @return           True if an entry was evicted; false otherwise.
         */
        public boolean tryEvict(long now) {
            synchronized (this) {
                // Try to evict an entry which is stale.
                Map.Entry<LastUsedKey, ShareSession> lastUsedEntry = lastUsed.firstEntry();
                if (lastUsedEntry == null) {
                    log.trace("There are no cache entries to evict.");
                    return false;
                } else if (now - lastUsedEntry.getKey().lastUsedMs > evictionMs) {
                    ShareSession session = lastUsedEntry.getValue();
                    log.trace("Evicting stale FetchSession {}.", session.key);
                    remove(session);
                    return true;
                }
                return false;
            }
        }

        public ShareSessionKey maybeCreateSession(String groupId, Uuid memberId, long now, int size, ImplicitLinkedHashCollection<CachedSharePartition> partitionMap) {
            synchronized (this) {
                if (sessions.size() < maxEntries || tryEvict(now)) {
                    ShareSession session = new ShareSession(new ShareSessionKey(groupId, memberId), partitionMap,
                            now, now, ShareFetchMetadata.nextEpoch(ShareFetchMetadata.INITIAL_EPOCH));
                    log.debug("Created share session " + session);
                    sessions.put(session.key, session);
                    touch(session, now);
                    return session.key;
                } else {
                    log.debug("No share session created for size = " + size);
                    return null;
                }
            }
        }
    }

    /*
     * A cached partition.
     *
     * The broker maintains a set of these objects for each share session.
     * When a share fetch request is made, any partitions which are not explicitly
     * enumerated in the fetch request are loaded from the cache. Similarly, when a
     * share fetch response is being prepared, any partitions that have not changed and
     * do not have errors are left out of the response.
     *
     * We store many of these objects, so it is important for them to be memory-efficient.
     * That is why we store topic and partition separately rather than storing a TopicPartition
     * object. The TP object takes up more memory because it is a separate JVM object, and
     * because it stores the cached hash code in memory.
     *
     */
    public static class CachedSharePartition implements ImplicitLinkedHashCollection.Element {
        private final String topic;
        private final Uuid topicId;
        private int partition, maxBytes;
        private Optional<Integer> leaderEpoch;
        private boolean requiresUpdateInResponse;

        private int cachedNext = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int cachedPrev = ImplicitLinkedHashCollection.INVALID_INDEX;

        private CachedSharePartition(String topic, Uuid topicId, int partition, int maxBytes, Optional<Integer> leaderEpoch,
                                     boolean requiresUpdateInResponse) {
            this.topic = topic;
            this.topicId = topicId;
            this.partition = partition;
            this.maxBytes = maxBytes;
            this.leaderEpoch = leaderEpoch;
            this.requiresUpdateInResponse = requiresUpdateInResponse;
        }

        public CachedSharePartition(String topic, Uuid topicId, int partition, boolean requiresUpdateInResponse) {
            this(topic, topicId, partition, -1, Optional.empty(), requiresUpdateInResponse);
        }

        public CachedSharePartition(TopicIdPartition topicIdPartition) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), false);
        }

        public CachedSharePartition(TopicIdPartition topicIdPartition, ShareFetchRequest.SharePartitionData reqData,
                                    boolean requiresUpdateInResponse) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), reqData.maxBytes,
                    reqData.currentLeaderEpoch, requiresUpdateInResponse);
        }

        public Uuid topicId() {
            return topicId;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public ShareFetchRequest.SharePartitionData reqData() {
            return new ShareFetchRequest.SharePartitionData(topicId, maxBytes, leaderEpoch);
        }

        public void updateRequestParams(ShareFetchRequest.SharePartitionData reqData) {
            // Update our cached request parameters.
            maxBytes = reqData.maxBytes;
            leaderEpoch = reqData.currentLeaderEpoch;
        }

        /**
         * Determine whether the specified cached partition should be included in the ShareFetchResponse we send back to
         * the fetcher and update it if requested.
         * This function should be called while holding the appropriate session lock.
         *
         * @param respData partition data
         * @param updateResponseData if set to true, update this CachedSharePartition with new request and response data.
         * @return True if this partition should be included in the response; false if it can be omitted.
         */
        public boolean maybeUpdateResponseData(ShareFetchResponseData.PartitionData respData, boolean updateResponseData) {
            boolean mustRespond = false;
            // Check the response data
            // Partitions with new data are always included in the response.
            if (ShareFetchResponse.recordsSize(respData) > 0)
                mustRespond = true;
            if (requiresUpdateInResponse) {
                mustRespond = true;
                if (updateResponseData)
                    requiresUpdateInResponse = false;
            }
            if (respData.errorCode() != Errors.NONE.code()) {
                // Partitions with errors are always included in the response.
                // We also set the cached requiresUpdateInResponse to false.
                // This ensures that when the error goes away, we re-send the partition.
                if (updateResponseData)
                    requiresUpdateInResponse = true;
                mustRespond = true;
            }
            return mustRespond;
        }

        public String toString() {
            return  "CachedSharePartition(topic=" + topic +
                    ", topicId=" + topicId +
                    ", partition=" + partition +
                    ", maxBytes=" + maxBytes +
                    ", leaderEpoch=" + leaderEpoch +
                    ")";
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, topicId);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            else if (obj == null || getClass() != obj.getClass())
                return false;
            else {
                CachedSharePartition that = (CachedSharePartition) obj;
                return partition == that.partition && Objects.equals(topicId, that.topicId);
            }
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

    private static class ShareFetchPartitionData {
        private final FetchParams fetchParams;
        private final String groupId;
        private final String memberId;
        private final List<TopicIdPartition> topicIdPartitions;
        private final CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future;
        private final Map<TopicIdPartition, Integer> partitionMaxBytes;

        public ShareFetchPartitionData(FetchParams fetchParams, String groupId, String memberId,
                                       List<TopicIdPartition> topicIdPartitions,
                                       CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future,
                                       Map<TopicIdPartition, Integer> partitionMaxBytes) {
            this.fetchParams = fetchParams;
            this.groupId = groupId;
            this.memberId = memberId;
            this.topicIdPartitions = topicIdPartitions;
            this.future = future;
            this.partitionMaxBytes = partitionMaxBytes;
        }
    }
}
