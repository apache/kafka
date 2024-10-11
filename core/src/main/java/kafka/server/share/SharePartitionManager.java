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
package kafka.server.share;

import kafka.server.ActionQueue;
import kafka.server.DelayedOperationPurgatory;
import kafka.server.ReplicaManager;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedStateEpochException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.context.FinalContext;
import org.apache.kafka.server.share.context.ShareFetchContext;
import org.apache.kafka.server.share.context.ShareSessionContext;
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.share.persister.Persister;
import org.apache.kafka.server.share.session.ShareSession;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.share.session.ShareSessionKey;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

/**
 * The SharePartitionManager is responsible for managing the SharePartitions and ShareSessions.
 * It is responsible for fetching messages from the log and acknowledging the messages.
 */
public class SharePartitionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    /**
     * The partition cache map is used to store the SharePartition objects for each share group topic-partition.
     */
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;

    /**
     * The replica manager is used to fetch messages from the log.
     */
    private final ReplicaManager replicaManager;

    /**
     * The time instance is used to get the current time.
     */
    private final Time time;

    /**
     * The share session cache stores the share sessions.
     */
    private final ShareSessionCache cache;

    /**
     * The fetch queue stores the share fetch requests that are waiting to be processed.
     */
    private final ConcurrentLinkedQueue<ShareFetchData> fetchQueue;

    /**
     * The process fetch queue lock is used to ensure that only one thread is processing the fetch queue at a time.
     */
    private final AtomicBoolean processFetchQueueLock;

    /**
     * The group config manager is used to retrieve the values for dynamic group configurations
     */
    private final GroupConfigManager groupConfigManager;

    /**
     * The default record lock duration is the time in milliseconds that a record lock is held for.
     * This default value can be overridden by a group-specific configuration.
     */
    private final int defaultRecordLockDurationMs;

    /**
     * The timer is used to schedule the records lock timeout.
     */
    private final Timer timer;

    /**
     * The max in flight messages is the maximum number of messages that can be in flight at any one time per share-partition.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is the maximum number of times a message can be delivered before it is considered to be archived.
     */
    private final int maxDeliveryCount;

    /**
     * The persister is used to persist the share partition state.
     */
    private final Persister persister;

    /**
     * Class with methods to record share group metrics.
     */
    private final ShareGroupMetrics shareGroupMetrics;

    /**
     * The delayed share fetch purgatory is used to store the share fetch requests that could not be processed immediately.
     */
    private final DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory;

    /**
     * The delayed actions queue is used to complete any pending delayed share fetch actions.
     */
    private final ActionQueue delayedActionsQueue;

    public SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        int defaultRecordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        int shareFetchPurgatoryPurgeIntervalRequests,
        Persister persister,
        ActionQueue delayedActionsQueue,
        GroupConfigManager groupConfigManager,
        Metrics metrics
    ) {
        this(replicaManager,
            time,
            cache,
            new ConcurrentHashMap<>(),
            defaultRecordLockDurationMs,
            maxDeliveryCount,
            maxInFlightMessages,
            shareFetchPurgatoryPurgeIntervalRequests,
            persister,
            delayedActionsQueue,
            groupConfigManager,
            metrics
        );
    }

    private SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        Map<SharePartitionKey, SharePartition> partitionCacheMap,
        int defaultRecordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        int shareFetchPurgatoryPurgeIntervalRequests,
        Persister persister,
        ActionQueue delayedActionsQueue,
        GroupConfigManager groupConfigManager,
        Metrics metrics
    ) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        this.partitionCacheMap = partitionCacheMap;
        this.fetchQueue = new ConcurrentLinkedQueue<>();
        this.processFetchQueueLock = new AtomicBoolean(false);
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = new SystemTimerReaper("share-group-lock-timeout-reaper",
            new SystemTimer("share-group-lock-timeout"));
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
        this.delayedShareFetchPurgatory = new DelayedOperationPurgatory<>("ShareFetch", this.timer, this.replicaManager.localBrokerId(), shareFetchPurgatoryPurgeIntervalRequests, true, true);
        this.delayedActionsQueue = delayedActionsQueue;
        this.groupConfigManager = groupConfigManager;
        this.shareGroupMetrics = new ShareGroupMetrics(Objects.requireNonNull(metrics), time);
    }

    // Visible for testing.
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    SharePartitionManager(
            ReplicaManager replicaManager,
            Time time,
            ShareSessionCache cache,
            Map<SharePartitionKey, SharePartition> partitionCacheMap,
            ConcurrentLinkedQueue<ShareFetchData> fetchQueue,
            int defaultRecordLockDurationMs,
            Timer timer,
            int maxDeliveryCount,
            int maxInFlightMessages,
            Persister persister,
            DelayedOperationPurgatory<DelayedShareFetch> delayedShareFetchPurgatory,
            ActionQueue delayedActionsQueue,
            GroupConfigManager groupConfigManager,
            Metrics metrics
    ) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        this.partitionCacheMap = partitionCacheMap;
        this.fetchQueue = fetchQueue;
        this.processFetchQueueLock = new AtomicBoolean(false);
        this.defaultRecordLockDurationMs = defaultRecordLockDurationMs;
        this.timer = timer;
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
        this.delayedShareFetchPurgatory = delayedShareFetchPurgatory;
        this.delayedActionsQueue = delayedActionsQueue;
        this.groupConfigManager = groupConfigManager;
        this.shareGroupMetrics = new ShareGroupMetrics(Objects.requireNonNull(metrics), time);
    }

    /**
     * The fetch messages method is used to fetch messages from the log for the specified topic-partitions.
     * The method returns a future that will be completed with the fetched messages.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param fetchParams The fetch parameters from the share fetch request.
     * @param partitionMaxBytes The maximum number of bytes to fetch for each partition.
     *
     * @return A future that will be completed with the fetched messages.
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        Map<TopicIdPartition, Integer> partitionMaxBytes
    ) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
                partitionMaxBytes.keySet(), groupId, fetchParams);

        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();
        ShareFetchData shareFetchData = new ShareFetchData(fetchParams, groupId, memberId, future, partitionMaxBytes);
        fetchQueue.add(shareFetchData);
        maybeProcessFetchQueue();

        return future;
    }

    /**
     * The acknowledge method is used to acknowledge the messages that have been fetched.
     * The method returns a future that will be completed with the acknowledge response.
     *
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param groupId The group id, this is used to identify the share group.
     * @param acknowledgeTopics The acknowledge topics and their corresponding acknowledge batches.
     *
     * @return A future that will be completed with the acknowledge response.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> acknowledge(
        String memberId,
        String groupId,
        Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics
    ) {
        log.trace("Acknowledge request for topicIdPartitions: {} with groupId: {}",
            acknowledgeTopics.keySet(), groupId);
        this.shareGroupMetrics.shareAcknowledgement();
        Map<TopicIdPartition, CompletableFuture<Errors>> futures = new HashMap<>();
        acknowledgeTopics.forEach((topicIdPartition, acknowledgePartitionBatches) -> {
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
            if (sharePartition != null) {
                CompletableFuture<Errors> future = new CompletableFuture<>();
                sharePartition.acknowledge(memberId, acknowledgePartitionBatches).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        future.complete(Errors.forException(throwable));
                        return;
                    }
                    acknowledgePartitionBatches.forEach(batch -> batch.acknowledgeTypes().forEach(this.shareGroupMetrics::recordAcknowledgement));
                    future.complete(Errors.NONE);
                });

                // If we have an acknowledgement completed for a topic-partition, then we should check if
                // there is a pending share fetch request for the topic-partition and complete it.
                DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
                delayedShareFetchPurgatory.checkAndComplete(delayedShareFetchKey);

                futures.put(topicIdPartition, future);
            } else {
                futures.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.values().toArray(new CompletableFuture[0]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> result.put(topicIdPartition, new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(topicIdPartition.partition())
                .setErrorCode(future.join().code())));
            return result;
        });
    }

    void addPurgatoryCheckAndCompleteDelayedActionToActionQueue(Set<TopicIdPartition> topicIdPartitions, String groupId) {
        delayedActionsQueue.add(() -> {
            topicIdPartitions.forEach(topicIdPartition ->
                delayedShareFetchPurgatory.checkAndComplete(
                    new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition())));
            return BoxedUnit.UNIT;
        });
    }

    /**
     * The release session method is used to release the session for the memberId of respective group.
     * The method post removing session also releases acquired records for the respective member.
     * The method returns a future that will be completed with the release response.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     *
     * @return A future that will be completed with the release response.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseSession(
        String groupId,
        String memberId
    ) {
        log.trace("Release session request for groupId: {}, memberId: {}", groupId, memberId);
        Uuid memberIdUuid = Uuid.fromString(memberId);
        List<TopicIdPartition> topicIdPartitions = cachedTopicIdPartitionsInShareSession(
            groupId, memberIdUuid);
        // Remove the share session from the cache.
        ShareSessionKey key = shareSessionKey(groupId, memberIdUuid);
        if (cache.remove(key) == null) {
            log.error("Share session error for {}: no such share session found", key);
            return FutureUtils.failedFuture(Errors.SHARE_SESSION_NOT_FOUND.exception());
        } else {
            log.debug("Removed share session with key " + key);
        }

        // Additionally release the acquired records for the respective member.
        if (topicIdPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<TopicIdPartition, CompletableFuture<Errors>> futuresMap = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
            if (sharePartition == null) {
                log.error("No share partition found for groupId {} topicPartition {} while releasing acquired topic partitions", groupId, topicIdPartition);
                futuresMap.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION));
            } else {
                CompletableFuture<Errors> future = new CompletableFuture<>();
                sharePartition.releaseAcquiredRecords(memberId).whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        future.complete(Errors.forException(throwable));
                        return;
                    }
                    future.complete(Errors.NONE);
                });
                // If we have a release acquired request completed for a topic-partition, then we should check if
                // there is a pending share fetch request for the topic-partition and complete it.
                DelayedShareFetchKey delayedShareFetchKey = new DelayedShareFetchGroupKey(groupId, topicIdPartition.topicId(), topicIdPartition.partition());
                delayedShareFetchPurgatory.checkAndComplete(delayedShareFetchKey);

                futuresMap.put(topicIdPartition, future);
            }
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futuresMap.values().toArray(new CompletableFuture[0]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            futuresMap.forEach((topicIdPartition, future) -> result.put(topicIdPartition, new ShareAcknowledgeResponseData.PartitionData()
                    .setPartitionIndex(topicIdPartition.partition())
                    .setErrorCode(future.join().code())));
            return result;
        });
    }

    /**
     * The newContext method is used to create a new share fetch context for every share fetch request.
     * @param groupId The group id in the share fetch request.
     * @param shareFetchData The topic-partitions and their corresponding maxBytes data in the share fetch request.
     * @param toForget The topic-partitions to forget present in the share fetch request.
     * @param reqMetadata The metadata in the share fetch request.
     * @param isAcknowledgeDataPresent This tells whether the fetch request received includes piggybacked acknowledgements or not
     * @return The new share fetch context object
     */
    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData,
                                        List<TopicIdPartition> toForget, ShareRequestMetadata reqMetadata, Boolean isAcknowledgeDataPresent) {
        ShareFetchContext context;
        // TopicPartition with maxBytes as 0 should not be added in the cachedPartitions
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchDataWithMaxBytes = new HashMap<>();
        shareFetchData.forEach((tp, sharePartitionData) -> {
            if (sharePartitionData.maxBytes > 0) shareFetchDataWithMaxBytes.put(tp, sharePartitionData);
        });
        // If the request's epoch is FINAL_EPOCH or INITIAL_EPOCH, we should remove the existing sessions. Also, start a
        // new session in case it is INITIAL_EPOCH. Hence, we need to treat them as special cases.
        if (reqMetadata.isFull()) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            if (reqMetadata.epoch() == ShareRequestMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                if (!shareFetchDataWithMaxBytes.isEmpty()) {
                    throw Errors.INVALID_REQUEST.exception();
                }
                if (cache.get(key) == null) {
                    log.error("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }
                context = new FinalContext();
            } else {
                if (isAcknowledgeDataPresent) {
                    log.error("Acknowledge data present in Initial Fetch Request for group {} member {}", groupId, reqMetadata.memberId());
                    throw Errors.INVALID_REQUEST.exception();
                }
                if (cache.remove(key) != null) {
                    log.debug("Removed share session with key {}", key);
                }
                ImplicitLinkedHashCollection<CachedSharePartition> cachedSharePartitions = new
                        ImplicitLinkedHashCollection<>(shareFetchDataWithMaxBytes.size());
                shareFetchDataWithMaxBytes.forEach((topicIdPartition, reqData) ->
                    cachedSharePartitions.mustAdd(new CachedSharePartition(topicIdPartition, reqData, false)));
                ShareSessionKey responseShareSessionKey = cache.maybeCreateSession(groupId, reqMetadata.memberId(),
                        time.milliseconds(), cachedSharePartitions);
                if (responseShareSessionKey == null) {
                    log.error("Could not create a share session for group {} member {}", groupId, reqMetadata.memberId());
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }

                context = new ShareSessionContext(reqMetadata, shareFetchDataWithMaxBytes);
                log.debug("Created a new ShareSessionContext with key {} isSubsequent {} returning {}. A new share " +
                        "session will be started.", responseShareSessionKey, false,
                        partitionsToLogString(shareFetchDataWithMaxBytes.keySet()));
            }
        } else {
            // We update the already existing share session.
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.error("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }
                if (shareSession.epoch != reqMetadata.epoch()) {
                    log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                            shareSession.epoch, reqMetadata.epoch());
                    throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                }
                Map<ShareSession.ModifiedTopicIdPartitionType, List<TopicIdPartition>> modifiedTopicIdPartitions = shareSession.update(
                        shareFetchDataWithMaxBytes, toForget);
                cache.touch(shareSession, time.milliseconds());
                shareSession.epoch = ShareRequestMetadata.nextEpoch(shareSession.epoch);
                log.debug("Created a new ShareSessionContext for session key {}, epoch {}: " +
                                "added {}, updated {}, removed {}", shareSession.key(), shareSession.epoch,
                        partitionsToLogString(modifiedTopicIdPartitions.get(
                                ShareSession.ModifiedTopicIdPartitionType.ADDED)),
                        partitionsToLogString(modifiedTopicIdPartitions.get(ShareSession.ModifiedTopicIdPartitionType.UPDATED)),
                        partitionsToLogString(modifiedTopicIdPartitions.get(ShareSession.ModifiedTopicIdPartitionType.REMOVED))
                );
                context = new ShareSessionContext(reqMetadata, shareSession);
            }
        }
        return context;
    }

    /**
     * The acknowledgeSessionUpdate method is used to update the request epoch and lastUsed time of the share session.
     * @param groupId The group id in the share fetch request.
     * @param reqMetadata The metadata in the share acknowledge request.
     */
    public void acknowledgeSessionUpdate(String groupId, ShareRequestMetadata reqMetadata) {
        if (reqMetadata.epoch() == ShareRequestMetadata.INITIAL_EPOCH) {
            // ShareAcknowledge Request cannot have epoch as INITIAL_EPOCH (0)
            throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                }

                if (reqMetadata.epoch() == ShareRequestMetadata.FINAL_EPOCH) {
                    // If the epoch is FINAL_EPOCH, then return. Do not update the cache.
                    return;
                } else if (shareSession.epoch != reqMetadata.epoch()) {
                    log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                            shareSession.epoch, reqMetadata.epoch());
                    throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                }
                cache.touch(shareSession, time.milliseconds());
                shareSession.epoch = ShareRequestMetadata.nextEpoch(shareSession.epoch);
            }
        }
    }

    /**
     * The cachedTopicIdPartitionsInShareSession method is used to get the cached topic-partitions in the share session.
     *
     * @param groupId The group id in the share fetch request.
     * @param memberId The member id in the share fetch request.
     *
     * @return The list of cached topic-partitions in the share session if present, otherwise an empty list.
     */
    // Visible for testing.
    List<TopicIdPartition> cachedTopicIdPartitionsInShareSession(String groupId, Uuid memberId) {
        ShareSessionKey key = shareSessionKey(groupId, memberId);
        ShareSession shareSession = cache.get(key);
        if (shareSession == null) {
            return Collections.emptyList();
        }
        List<TopicIdPartition> cachedTopicIdPartitions = new ArrayList<>();
        shareSession.partitionMap().forEach(cachedSharePartition -> cachedTopicIdPartitions.add(
            new TopicIdPartition(cachedSharePartition.topicId(), new TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()
            ))));
        return cachedTopicIdPartitions;
    }

    // Add the share fetch request to the delayed share fetch purgatory to process the fetch request if it can be
    // completed else watch until it can be completed/timeout.
    private void addDelayedShareFetch(DelayedShareFetch delayedShareFetch, Set<DelayedShareFetchKey> keys) {
        delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch,
            CollectionConverters.asScala(keys).toSeq().indices());
    }

    @Override
    public void close() throws Exception {
        this.delayedShareFetchPurgatory.shutdown();
        this.timer.close();
        this.persister.stop();
        if (!fetchQueue.isEmpty()) {
            log.warn("Closing SharePartitionManager with pending fetch requests count: {}", fetchQueue.size());
            fetchQueue.forEach(shareFetchData -> shareFetchData.future().completeExceptionally(
                Errors.BROKER_NOT_AVAILABLE.exception()));
            fetchQueue.clear();
        }
    }

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    private static String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return ShareSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    /**
     * Recursive function to process all the fetch requests present inside the fetch queue
     */
    // Visible for testing.
    void maybeProcessFetchQueue() {
        if (!acquireProcessFetchQueueLock()) {
            // The queue is already being processed hence avoid re-triggering.
            return;
        }

        ShareFetchData shareFetchData = fetchQueue.poll();
        if (shareFetchData == null) {
            // No more requests to process, so release the lock. Though we should not reach here as the lock
            // is acquired only when there are requests in the queue. But still, it's safe to release the lock.
            releaseProcessFetchQueueLock();
            return;
        }

        if (shareFetchData.partitionMaxBytes().isEmpty()) {
            // If there are no partitions to fetch then complete the future with an empty map.
            shareFetchData.future().complete(Collections.emptyMap());
            // Release the lock so that other threads can process the queue.
            releaseProcessFetchQueueLock();
            if (!fetchQueue.isEmpty())
                maybeProcessFetchQueue();
            return;
        }

        try {
            shareFetchData.partitionMaxBytes().keySet().forEach(topicIdPartition -> {
                SharePartitionKey sharePartitionKey = sharePartitionKey(
                    shareFetchData.groupId(),
                    topicIdPartition
                );
                SharePartition sharePartition = getOrCreateSharePartition(sharePartitionKey);

                // The share partition is initialized asynchronously, so we need to wait for it to be initialized.
                // But if the share partition is already initialized, then the future will be completed immediately.
                // Hence, it's safe to call the maybeInitialize method and then wait for the future to be completed.
                // TopicPartitionData list will be populated only if the share partition is already initialized.
                sharePartition.maybeInitialize().whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        maybeCompleteInitializationWithException(sharePartitionKey, shareFetchData.future(), throwable);
                        return;
                    }
                });
            });

            Set<DelayedShareFetchKey> delayedShareFetchWatchKeys = new HashSet<>();
            shareFetchData.partitionMaxBytes().keySet().forEach(
                topicIdPartition -> {
                    // We add a key corresponding to each share partition in the request in the group so that when there are
                    // acknowledgements/acquisition lock timeout etc, we have a way to perform checkAndComplete for all
                    // such requests which are delayed because of lack of data to acquire for the share partition.
                    delayedShareFetchWatchKeys.add(new DelayedShareFetchGroupKey(shareFetchData.groupId(), topicIdPartition.topicId(), topicIdPartition.partition()));
                    // We add a key corresponding to each topic partition in the request so that when the HWM is updated
                    // for any topic partition, we have a way to perform checkAndComplete for all such requests which are
                    // delayed because of lack of data to acquire for the topic partition.
                    delayedShareFetchWatchKeys.add(new DelayedShareFetchPartitionKey(topicIdPartition.topicId(), topicIdPartition.partition()));
                });

            // Add the share fetch to the delayed share fetch purgatory to process the fetch request.
            addDelayedShareFetch(new DelayedShareFetch(shareFetchData, replicaManager, this),
                delayedShareFetchWatchKeys);

            // Release the lock so that other threads can process the queue.
            releaseProcessFetchQueueLock();
            // If there are more requests in the queue, then process them.
            if (!fetchQueue.isEmpty())
                maybeProcessFetchQueue();

        } catch (Exception e) {
            // In case exception occurs then release the locks so queue can be further processed.
            log.error("Error processing fetch queue for share partitions", e);
            releaseProcessFetchQueueLock();
            // If there are more requests in the queue, then process them.
            if (!fetchQueue.isEmpty())
                maybeProcessFetchQueue();
        }
    }

    private SharePartition getOrCreateSharePartition(SharePartitionKey sharePartitionKey) {
        return partitionCacheMap.computeIfAbsent(sharePartitionKey,
                k -> {
                    long start = time.hiResClockMs();
                    SharePartition partition = new SharePartition(
                            sharePartitionKey.groupId(),
                            sharePartitionKey.topicIdPartition(),
                            maxInFlightMessages,
                            maxDeliveryCount,
                            defaultRecordLockDurationMs,
                            timer,
                            time,
                            persister,
                            delayedShareFetchPurgatory,
                            groupConfigManager
                    );
                    this.shareGroupMetrics.partitionLoadTime(start);
                    return partition;
                });
    }

    private void maybeCompleteInitializationWithException(
            SharePartitionKey sharePartitionKey,
            CompletableFuture<Map<TopicIdPartition, PartitionData>> future,
            Throwable throwable) {
        if (throwable instanceof LeaderNotAvailableException) {
            log.debug("The share partition with key {} is not initialized yet", sharePartitionKey);
            // Do not process the fetch request for this partition as the leader is not initialized yet.
            // The fetch request will be retried in the next poll.
            // TODO: Add the request to delayed fetch purgatory.
            return;
        }

        if (throwable instanceof NotLeaderOrFollowerException || throwable instanceof FencedStateEpochException) {
            log.info("The share partition with key {} is fenced: {}", sharePartitionKey, throwable.getMessage());
            // The share partition is fenced hence remove the partition from map and let the client retry.
            // But surface the error to the client so client might take some action i.e. re-fetch
            // the metadata and retry the fetch on new leader.
            partitionCacheMap.remove(sharePartitionKey);
            future.completeExceptionally(throwable);
            return;
        }

        // The partition initialization failed, so complete the request with the exception.
        // The server should not be in this state, so log the error on broker and surface the same
        // to the client. As of now this state is in-recoverable for the broker, and we should
        // investigate the root cause of the error.
        log.error("Error initializing share partition with key {}", sharePartitionKey, throwable);
        future.completeExceptionally(throwable);
    }

    // Visible for testing.
    boolean acquireProcessFetchQueueLock() {
        return processFetchQueueLock.compareAndSet(false, true);
    }

    private void releaseProcessFetchQueueLock() {
        processFetchQueueLock.set(false);
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    /**
     *
     * @param groupId The share group id, this is used to identify the share group.
     * @param topicIdPartition The topic partition that the group is subscribed to.
     * @return The share partition stored for the share group topic-partition.
     */
    protected SharePartition sharePartition(String groupId, TopicIdPartition topicIdPartition) {
        return partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
    }

    static class ShareGroupMetrics {
        /**
         * share-acknowledgement (share-acknowledgement-rate and share-acknowledgement-count) - The total number of offsets acknowledged for share groups (requests to be ack).
         * record-acknowledgement (record-acknowledgement-rate and record-acknowledgement-count) - The number of records acknowledged per acknowledgement type.
         * partition-load-time (partition-load-time-avg and partition-load-time-max) - The time taken to load the share partitions.
         */

        public static final String METRICS_GROUP_NAME = "share-group-metrics";

        public static final String SHARE_ACK_SENSOR = "share-acknowledgement-sensor";
        public static final String SHARE_ACK_RATE = "share-acknowledgement-rate";
        public static final String SHARE_ACK_COUNT = "share-acknowledgement-count";

        public static final String RECORD_ACK_SENSOR_PREFIX = "record-acknowledgement";
        public static final String RECORD_ACK_RATE = "record-acknowledgement-rate";
        public static final String RECORD_ACK_COUNT = "record-acknowledgement-count";
        public static final String ACK_TYPE = "ack-type";

        public static final String PARTITION_LOAD_TIME_SENSOR = "partition-load-time-sensor";
        public static final String PARTITION_LOAD_TIME_AVG = "partition-load-time-avg";
        public static final String PARTITION_LOAD_TIME_MAX = "partition-load-time-max";

        public static final Map<Byte, String> RECORD_ACKS_MAP = new HashMap<>();
        
        private final Time time;
        private final Sensor shareAcknowledgementSensor;
        private final Map<Byte, Sensor> recordAcksSensorMap = new HashMap<>();
        private final Sensor partitionLoadTimeSensor;

        static {
            RECORD_ACKS_MAP.put((byte) 1, AcknowledgeType.ACCEPT.toString());
            RECORD_ACKS_MAP.put((byte) 2, AcknowledgeType.RELEASE.toString());
            RECORD_ACKS_MAP.put((byte) 3, AcknowledgeType.REJECT.toString());
        }

        public ShareGroupMetrics(Metrics metrics, Time time) {
            this.time = time;

            shareAcknowledgementSensor = metrics.sensor(SHARE_ACK_SENSOR);
            shareAcknowledgementSensor.add(new Meter(
                metrics.metricName(
                    SHARE_ACK_RATE,
                    METRICS_GROUP_NAME,
                    "Rate of acknowledge requests."),
                metrics.metricName(
                    SHARE_ACK_COUNT,
                    METRICS_GROUP_NAME,
                    "The number of acknowledge requests.")));

            for (Map.Entry<Byte, String> entry : RECORD_ACKS_MAP.entrySet()) {
                recordAcksSensorMap.put(entry.getKey(), metrics.sensor(String.format("%s-%s-sensor", RECORD_ACK_SENSOR_PREFIX, entry.getValue())));
                recordAcksSensorMap.get(entry.getKey())
                    .add(new Meter(
                        metrics.metricName(
                            RECORD_ACK_RATE,
                            METRICS_GROUP_NAME,
                            "Rate of records acknowledged per acknowledgement type.",
                            ACK_TYPE, entry.getValue()),
                        metrics.metricName(
                            RECORD_ACK_COUNT,
                            METRICS_GROUP_NAME,
                            "The number of records acknowledged per acknowledgement type.",
                            ACK_TYPE, entry.getValue())));
            }

            partitionLoadTimeSensor = metrics.sensor(PARTITION_LOAD_TIME_SENSOR);
            partitionLoadTimeSensor.add(metrics.metricName(
                    PARTITION_LOAD_TIME_AVG,
                    METRICS_GROUP_NAME,
                    "The average time in milliseconds to load the share partitions."),
                new Avg());
            partitionLoadTimeSensor.add(metrics.metricName(
                    PARTITION_LOAD_TIME_MAX,
                    METRICS_GROUP_NAME,
                    "The maximum time in milliseconds to load the share partitions."),
                new Max());
        }

        void shareAcknowledgement() {
            shareAcknowledgementSensor.record();
        }

        void recordAcknowledgement(byte ackType) {
            // unknown ack types (such as gaps for control records) are intentionally ignored
            if (recordAcksSensorMap.containsKey(ackType)) {
                recordAcksSensorMap.get(ackType).record();
            }
        }

        void partitionLoadTime(long start) {
            partitionLoadTimeSensor.record(time.hiResClockMs() - start);
        }
    }
}
