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

import kafka.server.FetchSession;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.ShareSession;
import org.apache.kafka.server.share.ShareSessionCache;
import org.apache.kafka.server.share.ShareSessionKey;
import org.apache.kafka.storage.internals.log.FetchParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final ConcurrentLinkedQueue<ShareFetchPartitionData> fetchQueue;

    /**
     * The process fetch queue lock is used to ensure that only one thread is processing the fetch queue at a time.
     */
    private final AtomicBoolean processFetchQueueLock;

    /**
     * The record lock duration is the time in milliseconds that a record lock is held for.
     */
    private final int recordLockDurationMs;

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

    public SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        int recordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        Persister persister
    ) {
        this(replicaManager, time, cache, new ConcurrentHashMap<>(), recordLockDurationMs, maxDeliveryCount, maxInFlightMessages, persister);
    }

    SharePartitionManager(
        ReplicaManager replicaManager,
        Time time,
        ShareSessionCache cache,
        Map<SharePartitionKey, SharePartition> partitionCacheMap,
        int recordLockDurationMs,
        int maxDeliveryCount,
        int maxInFlightMessages,
        Persister persister
    ) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        this.partitionCacheMap = partitionCacheMap;
        this.fetchQueue = new ConcurrentLinkedQueue<>();
        this.processFetchQueueLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
    }

    /**
     * The fetch messages method is used to fetch messages from the log for the specified topic-partitions.
     * The method returns a future that will be completed with the fetched messages.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param fetchParams The fetch parameters from the share fetch request.
     * @param topicIdPartitions The topic-partitions to fetch messages for.
     * @param partitionMaxBytes The maximum number of bytes to fetch for each partition.
     *
     * @return A future that will be completed with the fetched messages.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions,
        Map<TopicIdPartition, Integer> partitionMaxBytes
    ) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
            topicIdPartitions, groupId, fetchParams);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented yet"));

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

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented yet"));

        return future;
    }

    /**
     * The release acquired records method is used to release the acquired records for the specified topic-partitions.
     * The method returns a future that will be completed with the release response.
     *
     * @param groupId The group id, this is used to identify the share group.
     * @param memberId The member id, generated by the group-coordinator, this is used to identify the client.
     * @param topicIdPartitions The topic-partitions to release the acquired records for.
     *
     * @return A future that will be completed with the release response.
     */
    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseAcquiredRecords(
        String groupId,
        String memberId,
        List<TopicIdPartition> topicIdPartitions
    ) {
        log.trace("Release acquired records request for topicIdPartitions: {} with groupId: {}",
            topicIdPartitions, groupId);

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException("Not implemented yet"));

        return future;
    }

    /**
     * The newContext method is used to create a new share fetch context for every share fetch request.
     * @param groupId The group id in the share fetch request.
     * @param shareFetchData The topic-partitions and their corresponding maxBytes data in the share fetch request.
     * @param toForget The topic-partitions to forget present in the share fetch request.
     * @param reqMetadata The metadata in the share fetch request.
     * @return The new share fetch context object
     */
    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> toForget, ShareFetchMetadata reqMetadata) {
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
            if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                if (!shareFetchDataWithMaxBytes.isEmpty()) {
                    throw Errors.INVALID_REQUEST.exception();
                }
                context = new FinalContext();
                synchronized (cache) {
                    if (cache.remove(key) != null) {
                        log.debug("Removed share session with key {}", key);
                    }
                }
            } else {
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
                shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
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

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    private static String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    public List<TopicIdPartition> cachedTopicIdPartitionsInShareSession(String groupId, Uuid memberId) {
        ShareSessionKey key = shareSessionKey(groupId, memberId);
        ShareSession shareSession = cache.get(key);
        if (shareSession == null) {
            throw new ShareSessionNotFoundException("Share session not found in cache");
        }
        List<TopicIdPartition> cachedTopicIdPartitions = new ArrayList<>();
        shareSession.partitionMap().forEach(cachedSharePartition -> cachedTopicIdPartitions.add(
                new TopicIdPartition(cachedSharePartition.topicId(), new TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()
                ))));
        return cachedTopicIdPartitions;
    }

    @Override
    public void close() throws Exception {
        // TODO: Provide Implementation
    }

    /**
     * The SharePartitionKey is used to uniquely identify a share partition. The key is made up of the
     * share group id, the topic id and the partition id. The key is used to store the SharePartition
     * objects in the partition cache map.
     */
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

    /**
     * The ShareFetchPartitionData class is used to store the fetch parameters for a share fetch request.
     */
    private static class ShareFetchPartitionData {
        // TODO: Provide Implementation
    }
}
