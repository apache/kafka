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

import kafka.cluster.Partition;
import kafka.server.LogReadResult;
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.purgatory.DelayedOperation;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

/**
 * A delayed share fetch operation has been introduced in case there is a share fetch request which cannot be completed instantaneously.
 */
public class DelayedShareFetch extends DelayedOperation {

    private static final Logger log = LoggerFactory.getLogger(DelayedShareFetch.class);

    private final ShareFetch shareFetch;
    private final ReplicaManager replicaManager;
    private final SharePartitionManager sharePartitionManager;
    // The topic partitions that need to be completed for the share fetch request are given by sharePartitions.
    // sharePartitions is a subset of shareFetchData. The order of insertion/deletion of entries in sharePartitions is important.
    private final LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions;
    private LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> partitionsAcquired;
    private LinkedHashMap<TopicIdPartition, LogReadResult> partitionsAlreadyFetched;

    DelayedShareFetch(
            ShareFetch shareFetch,
            ReplicaManager replicaManager,
            SharePartitionManager sharePartitionManager,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions) {
        super(shareFetch.fetchParams().maxWaitMs, Optional.empty());
        this.shareFetch = shareFetch;
        this.replicaManager = replicaManager;
        this.partitionsAcquired = new LinkedHashMap<>();
        this.partitionsAlreadyFetched = new LinkedHashMap<>();
        this.sharePartitionManager = sharePartitionManager;
        this.sharePartitions = sharePartitions;
    }

    @Override
    public void onExpiration() {
    }

    /**
     * Complete the share fetch operation by fetching records for all partitions in the share fetch request irrespective
     * of whether they have any acquired records. This is called when the fetch operation is forced to complete either
     * because records can be acquired for some partitions or due to MaxWaitMs timeout.
     */
    @Override
    public void onComplete() {
        // We are utilizing lock so that onComplete doesn't do a dirty read for instance variables -
        // partitionsAcquired and partitionsAlreadyFetched, since these variables can get updated in a different tryComplete thread.
        lock.lock();
        log.trace("Completing the delayed share fetch request for group {}, member {}, "
            + "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
            partitionsAcquired.keySet());

        try {
            LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData;
            // tryComplete did not invoke forceComplete, so we need to check if we have any partitions to fetch.
            if (partitionsAcquired.isEmpty())
                topicPartitionData = acquirablePartitions();
            // tryComplete invoked forceComplete, so we can use the data from tryComplete.
            else
                topicPartitionData = partitionsAcquired;

            if (topicPartitionData.isEmpty()) {
                // No locks for share partitions could be acquired, so we complete the request with an empty response.
                shareFetch.maybeComplete(Collections.emptyMap());
                return;
            }
            log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
                topicPartitionData, shareFetch.groupId(), shareFetch.fetchParams());

            completeShareFetchRequest(topicPartitionData);
        } finally {
            lock.unlock();
        }
    }

    private void completeShareFetchRequest(LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        try {
            LinkedHashMap<TopicIdPartition, LogReadResult> responseData;
            if (partitionsAlreadyFetched.isEmpty())
                responseData = readFromLog(topicPartitionData);
            else
                // There shouldn't be a case when we have a partitionsAlreadyFetched value here and this variable is getting
                // updated in a different tryComplete thread.
                responseData = combineLogReadResponse(topicPartitionData, partitionsAlreadyFetched);

            LinkedHashMap<TopicIdPartition, FetchPartitionData> fetchPartitionsData = new LinkedHashMap<>();
            for (Map.Entry<TopicIdPartition, LogReadResult> entry : responseData.entrySet())
                fetchPartitionsData.put(entry.getKey(), entry.getValue().toFetchPartitionData(false));

            shareFetch.maybeComplete(ShareFetchUtils.processFetchResponse(shareFetch, fetchPartitionsData,
                sharePartitions, replicaManager));
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request", e);
            handleFetchException(shareFetch, topicPartitionData.keySet(), e);
        } finally {
            // Releasing the lock to move ahead with the next request in queue.
            releasePartitionLocks(topicPartitionData.keySet());
            // If we have a fetch request completed for a topic-partition, we release the locks for that partition,
            // then we should check if there is a pending share fetch request for the topic-partition and complete it.
            // We add the action to delayed actions queue to avoid an infinite call stack, which could happen if
            // we directly call delayedShareFetchPurgatory.checkAndComplete
            replicaManager.addToActionQueue(() -> topicPartitionData.keySet().forEach(topicIdPartition ->
                replicaManager.completeDelayedShareFetchRequest(
                    new DelayedShareFetchGroupKey(shareFetch.groupId(), topicIdPartition.topicId(), topicIdPartition.partition()))));
        }
    }

    /**
     * Try to complete the fetch operation if we can acquire records for any partition in the share fetch request.
     */
    @Override
    public boolean tryComplete() {
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = acquirablePartitions();

        try {
            if (!topicPartitionData.isEmpty()) {
                // In case, fetch offset metadata doesn't exist for one or more topic partitions, we do a
                // replicaManager.readFromLog to populate the offset metadata and update the fetch offset metadata for
                // those topic partitions.
                LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse = maybeReadFromLog(topicPartitionData);
                maybeUpdateFetchOffsetMetadata(topicPartitionData, replicaManagerReadResponse);
                if (anyPartitionHasLogReadError(replicaManagerReadResponse) || isMinBytesSatisfied(topicPartitionData)) {
                    partitionsAcquired = topicPartitionData;
                    partitionsAlreadyFetched = replicaManagerReadResponse;
                    boolean completedByMe = forceComplete();
                    // If invocation of forceComplete is not successful, then that means the request is already completed
                    // hence release the acquired locks.
                    if (!completedByMe) {
                        releasePartitionLocks(partitionsAcquired.keySet());
                    }
                    return completedByMe;
                } else {
                    log.debug("minBytes is not satisfied for the share fetch request for group {}, member {}, " +
                            "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                        sharePartitions.keySet());
                    releasePartitionLocks(topicPartitionData.keySet());
                }
            } else {
                log.trace("Can't acquire records for any partition in the share fetch request for group {}, member {}, " +
                        "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                    sharePartitions.keySet());
            }
            return false;
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request", e);
            partitionsAcquired.clear();
            partitionsAlreadyFetched.clear();
            releasePartitionLocks(topicPartitionData.keySet());
            return forceComplete();
        }
    }

    /**
     * Prepare fetch request structure for partitions in the share fetch request for which we can acquire records.
     */
    // Visible for testing
    LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> acquirablePartitions() {
        // Initialize the topic partitions for which the fetch should be attempted.
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();

        sharePartitions.forEach((topicIdPartition, sharePartition) -> {
            int partitionMaxBytes = shareFetch.partitionMaxBytes().getOrDefault(topicIdPartition, 0);
            // Add the share partition to the list of partitions to be fetched only if we can
            // acquire the fetch lock on it.
            if (sharePartition.maybeAcquireFetchLock()) {
                try {
                    // If the share partition is already at capacity, we should not attempt to fetch.
                    if (sharePartition.canAcquireRecords()) {
                        topicPartitionData.put(
                            topicIdPartition,
                            new FetchRequest.PartitionData(
                                topicIdPartition.topicId(),
                                sharePartition.nextFetchOffset(),
                                0,
                                partitionMaxBytes,
                                Optional.empty()
                            )
                        );
                    } else {
                        sharePartition.releaseFetchLock();
                        log.trace("Record lock partition limit exceeded for SharePartition {}, " +
                            "cannot acquire more records", sharePartition);
                    }
                } catch (Exception e) {
                    log.error("Error checking condition for SharePartition: {}", sharePartition, e);
                    // Release the lock, if error occurred.
                    sharePartition.releaseFetchLock();
                }
            }
        });
        return topicPartitionData;
    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> maybeReadFromLog(LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> partitionsNotMatchingFetchOffsetMetadata = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, partitionData) -> {
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            if (sharePartition.fetchOffsetMetadata(partitionData.fetchOffset).isEmpty()) {
                partitionsNotMatchingFetchOffsetMetadata.put(topicIdPartition, partitionData);
            }
        });
        if (partitionsNotMatchingFetchOffsetMetadata.isEmpty()) {
            return new LinkedHashMap<>();
        }
        // We fetch data from replica manager corresponding to the topic partitions that have missing fetch offset metadata.
        return readFromLog(partitionsNotMatchingFetchOffsetMetadata);
    }

    private void maybeUpdateFetchOffsetMetadata(
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponseData) {
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : replicaManagerReadResponseData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            LogReadResult replicaManagerLogReadResult = entry.getValue();
            if (replicaManagerLogReadResult.error().code() != Errors.NONE.code()) {
                log.debug("Replica manager read log result {} errored out for topic partition {}",
                    replicaManagerLogReadResult, topicIdPartition);
                continue;
            }
            sharePartition.updateFetchOffsetMetadata(
                topicPartitionData.get(topicIdPartition).fetchOffset,
                replicaManagerLogReadResult.info().fetchOffsetMetadata);
        }
    }

    // minByes estimation currently assumes the common case where all fetched data is acquirable.
    private boolean isMinBytesSatisfied(LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        long accumulatedSize = 0;
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : topicPartitionData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            FetchRequest.PartitionData partitionData = entry.getValue();

            LogOffsetMetadata endOffsetMetadata;
            try {
                endOffsetMetadata = endOffsetMetadataForTopicPartition(topicIdPartition);
            } catch (Exception e) {
                shareFetch.addErroneous(topicIdPartition, e);
                sharePartitionManager.handleFencedSharePartitionException(
                    new SharePartitionKey(shareFetch.groupId(), topicIdPartition), e);
                continue;
            }

            if (endOffsetMetadata == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;

            SharePartition sharePartition = sharePartitions.get(topicIdPartition);

            Optional<LogOffsetMetadata> optionalFetchOffsetMetadata = sharePartition.fetchOffsetMetadata(partitionData.fetchOffset);
            if (optionalFetchOffsetMetadata.isEmpty() || optionalFetchOffsetMetadata.get() == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;
            LogOffsetMetadata fetchOffsetMetadata = optionalFetchOffsetMetadata.get();

            if (fetchOffsetMetadata.messageOffset > endOffsetMetadata.messageOffset) {
                log.debug("Satisfying delayed share fetch request for group {}, member {} since it is fetching later segments of " +
                    "topicIdPartition {}", shareFetch.groupId(), shareFetch.memberId(), topicIdPartition);
                return true;
            } else if (fetchOffsetMetadata.messageOffset < endOffsetMetadata.messageOffset) {
                if (fetchOffsetMetadata.onOlderSegment(endOffsetMetadata)) {
                    // This can happen when the fetch operation is falling behind the current segment or the partition
                    // has just rolled a new segment.
                    log.debug("Satisfying delayed share fetch request for group {}, member {} immediately since it is fetching older " +
                        "segments of topicIdPartition {}", shareFetch.groupId(), shareFetch.memberId(), topicIdPartition);
                    return true;
                } else if (fetchOffsetMetadata.onSameSegment(endOffsetMetadata)) {
                    // we take the partition fetch size as upper bound when accumulating the bytes.
                    long bytesAvailable = Math.min(endOffsetMetadata.positionDiff(fetchOffsetMetadata), partitionData.maxBytes);
                    accumulatedSize += bytesAvailable;
                }
            }
        }
        return accumulatedSize >= shareFetch.fetchParams().minBytes;
    }

    private LogOffsetMetadata endOffsetMetadataForTopicPartition(TopicIdPartition topicIdPartition) {
        Partition partition = ShareFetchUtils.partition(replicaManager, topicIdPartition.topicPartition());
        LogOffsetSnapshot offsetSnapshot = partition.fetchOffsetSnapshot(Optional.empty(), true);
        // The FetchIsolation type that we use for share fetch is FetchIsolation.HIGH_WATERMARK. In the future, we can
        // extend it to support other FetchIsolation types.
        FetchIsolation isolationType = shareFetch.fetchParams().isolation;
        if (isolationType == FetchIsolation.LOG_END)
            return offsetSnapshot.logEndOffset;
        else if (isolationType == FetchIsolation.HIGH_WATERMARK)
            return offsetSnapshot.highWatermark;
        else
            return offsetSnapshot.lastStableOffset;

    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> readFromLog(LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        // Filter if there already exists any erroneous topic partition.
        Set<TopicIdPartition> partitionsToFetch = shareFetch.filterErroneousTopicPartitions(topicPartitionData.keySet());
        if (partitionsToFetch.isEmpty()) {
            return new LinkedHashMap<>();
        }

        Seq<Tuple2<TopicIdPartition, LogReadResult>> responseLogResult = replicaManager.readFromLog(
            shareFetch.fetchParams(),
            CollectionConverters.asScala(
                partitionsToFetch.stream().map(topicIdPartition ->
                    new Tuple2<>(topicIdPartition, topicPartitionData.get(topicIdPartition))).collect(Collectors.toList())
            ),
            QuotaFactory.UNBOUNDED_QUOTA,
            true);

        LinkedHashMap<TopicIdPartition, LogReadResult> responseData = new LinkedHashMap<>();
        responseLogResult.foreach(tpLogResult -> {
            responseData.put(tpLogResult._1(), tpLogResult._2());
            return BoxedUnit.UNIT;
        });

        log.trace("Data successfully retrieved by replica manager: {}", responseData);
        return responseData;
    }

    private boolean anyPartitionHasLogReadError(LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse) {
        return replicaManagerReadResponse.values().stream()
            .anyMatch(logReadResult -> logReadResult.error().code() != Errors.NONE.code());
    }

    /**
     * The handleFetchException method is used to handle the exception that occurred while reading from log.
     * The method will handle the exception for each topic-partition in the request. The share partition
     * might get removed from the cache.
     * <p>
     * The replica read request might error out for one share partition
     * but as we cannot determine which share partition errored out, we might remove all the share partitions
     * in the request.
     *
     * @param shareFetch The share fetch request.
     * @param topicIdPartitions The topic-partitions in the replica read request.
     * @param throwable The exception that occurred while fetching messages.
     */
    private void handleFetchException(
        ShareFetch shareFetch,
        Set<TopicIdPartition> topicIdPartitions,
        Throwable throwable
    ) {
        topicIdPartitions.forEach(topicIdPartition -> sharePartitionManager.handleFencedSharePartitionException(
            new SharePartitionKey(shareFetch.groupId(), topicIdPartition), throwable));
        shareFetch.maybeCompleteWithException(topicIdPartitions, throwable);
    }

    // Visible for testing.
    LinkedHashMap<TopicIdPartition, LogReadResult> combineLogReadResponse(LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData,
                                                                LinkedHashMap<TopicIdPartition, LogReadResult> existingFetchedData) {
        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> missingLogReadTopicPartitions = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, partitionData) -> {
            if (!existingFetchedData.containsKey(topicIdPartition)) {
                missingLogReadTopicPartitions.put(topicIdPartition, partitionData);
            }
        });
        if (missingLogReadTopicPartitions.isEmpty()) {
            return existingFetchedData;
        }
        LinkedHashMap<TopicIdPartition, LogReadResult> missingTopicPartitionsLogReadResponse = readFromLog(missingLogReadTopicPartitions);
        missingTopicPartitionsLogReadResponse.putAll(existingFetchedData);
        return missingTopicPartitionsLogReadResponse;
    }

    // Visible for testing.
    void releasePartitionLocks(Set<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.forEach(tp -> {
            SharePartition sharePartition = sharePartitions.get(tp);
            sharePartition.releaseFetchLock();
        });
    }

    // Visible for testing.
    Lock lock() {
        return lock;
    }
}
