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
import org.apache.kafka.server.share.fetch.ShareFetchData;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    private final ShareFetchData shareFetchData;
    private final ReplicaManager replicaManager;

    private Map<TopicIdPartition, FetchRequest.PartitionData> partitionsAcquired;
    private Map<TopicIdPartition, LogReadResult> partitionsAlreadyFetched;
    private final SharePartitionManager sharePartitionManager;
    // The topic partitions that need to be completed for the share fetch request are given by sharePartitions.
    // sharePartitions is a subset of shareFetchData. The order of insertion/deletion of entries in sharePartitions is important.
    private final LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions;

    DelayedShareFetch(
            ShareFetchData shareFetchData,
            ReplicaManager replicaManager,
            SharePartitionManager sharePartitionManager,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions) {
        super(shareFetchData.fetchParams().maxWaitMs, Optional.empty());
        this.shareFetchData = shareFetchData;
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
        log.trace("Completing the delayed share fetch request for group {}, member {}, "
            + "topic partitions {}", shareFetchData.groupId(), shareFetchData.memberId(),
            partitionsAcquired.keySet());

        if (shareFetchData.future().isDone())
            return;

        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData;
        // tryComplete did not invoke forceComplete, so we need to check if we have any partitions to fetch.
        if (partitionsAcquired.isEmpty())
            topicPartitionData = acquirablePartitions();
        // tryComplete invoked forceComplete, so we can use the data from tryComplete.
        else
            topicPartitionData = partitionsAcquired;

        if (topicPartitionData.isEmpty()) {
            // No locks for share partitions could be acquired, so we complete the request with an empty response.
            shareFetchData.future().complete(Collections.emptyMap());
            return;
        }
        log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
            topicPartitionData, shareFetchData.groupId(), shareFetchData.fetchParams());

        try {
            Map<TopicIdPartition, LogReadResult> responseData;
            if (partitionsAlreadyFetched.isEmpty())
                responseData = readFromLog(topicPartitionData);
            else
                // There shouldn't be a case when we have a partitionsAlreadyFetched value here and this variable is getting
                // updated in a different tryComplete thread.
                responseData = combineLogReadResponse(topicPartitionData, partitionsAlreadyFetched);

            Map<TopicIdPartition, FetchPartitionData> fetchPartitionsData = new LinkedHashMap<>();
            for (Map.Entry<TopicIdPartition, LogReadResult> entry : responseData.entrySet())
                fetchPartitionsData.put(entry.getKey(), entry.getValue().toFetchPartitionData(false));

            shareFetchData.future().complete(ShareFetchUtils.processFetchResponse(shareFetchData, fetchPartitionsData,
                sharePartitions, replicaManager));
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request", e);
            sharePartitionManager.handleFetchException(shareFetchData.groupId(), topicPartitionData.keySet(), shareFetchData.future(), e);
        } finally {
            // Releasing the lock to move ahead with the next request in queue.
            releasePartitionLocks(topicPartitionData.keySet());
            // If we have a fetch request completed for a topic-partition, we release the locks for that partition,
            // then we should check if there is a pending share fetch request for the topic-partition and complete it.
            // We add the action to delayed actions queue to avoid an infinite call stack, which could happen if
            // we directly call delayedShareFetchPurgatory.checkAndComplete
            replicaManager.addToActionQueue(() -> topicPartitionData.keySet().forEach(topicIdPartition ->
                replicaManager.completeDelayedShareFetchRequest(
                    new DelayedShareFetchGroupKey(shareFetchData.groupId(), topicIdPartition.topicId(), topicIdPartition.partition()))));
        }
    }

    /**
     * Try to complete the fetch operation if we can acquire records for any partition in the share fetch request.
     */
    @Override
    public boolean tryComplete() {
        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = acquirablePartitions();

        try {
            if (!topicPartitionData.isEmpty()) {
                // In case, fetch offset metadata doesn't exist for one or more topic partitions, we do a
                // replicaManager.readFromLog to populate the offset metadata and update the fetch offset metadata for
                // those topic partitions.
                Map<TopicIdPartition, LogReadResult> replicaManagerReadResponse = maybeReadFromLog(topicPartitionData);
                maybeUpdateFetchOffsetMetadata(replicaManagerReadResponse);
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
                            "topic partitions {}", shareFetchData.groupId(), shareFetchData.memberId(),
                        sharePartitions.keySet());
                    releasePartitionLocks(topicPartitionData.keySet());
                }
            } else {
                log.trace("Can't acquire records for any partition in the share fetch request for group {}, member {}, " +
                        "topic partitions {}", shareFetchData.groupId(), shareFetchData.memberId(),
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
    Map<TopicIdPartition, FetchRequest.PartitionData> acquirablePartitions() {
        // Initialize the topic partitions for which the fetch should be attempted.
        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();

        sharePartitions.forEach((topicIdPartition, sharePartition) -> {
            int partitionMaxBytes = shareFetchData.partitionMaxBytes().getOrDefault(topicIdPartition, 0);
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

    private Map<TopicIdPartition, LogReadResult> maybeReadFromLog(Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        Map<TopicIdPartition, FetchRequest.PartitionData> partitionsMissingFetchOffsetMetadata = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, partitionData) -> {
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            if (sharePartition.fetchOffsetMetadata().isEmpty()) {
                partitionsMissingFetchOffsetMetadata.put(topicIdPartition, partitionData);
            }
        });
        if (partitionsMissingFetchOffsetMetadata.isEmpty()) {
            return Collections.emptyMap();
        }
        // We fetch data from replica manager corresponding to the topic partitions that have missing fetch offset metadata.
        return readFromLog(partitionsMissingFetchOffsetMetadata);
    }

    private void maybeUpdateFetchOffsetMetadata(
        Map<TopicIdPartition, LogReadResult> replicaManagerReadResponseData) {
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : replicaManagerReadResponseData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            LogReadResult replicaManagerLogReadResult = entry.getValue();
            if (replicaManagerLogReadResult.error().code() != Errors.NONE.code()) {
                log.debug("Replica manager read log result {} errored out for topic partition {}",
                    replicaManagerLogReadResult, topicIdPartition);
                continue;
            }
            sharePartition.updateFetchOffsetMetadata(Optional.of(replicaManagerLogReadResult.info().fetchOffsetMetadata));
        }
    }

    // minByes estimation currently assumes the common case where all fetched data is acquirable.
    private boolean isMinBytesSatisfied(Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        long accumulatedSize = 0;
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : topicPartitionData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            FetchRequest.PartitionData partitionData = entry.getValue();
            LogOffsetMetadata endOffsetMetadata = endOffsetMetadataForTopicPartition(topicIdPartition);

            if (endOffsetMetadata == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;

            SharePartition sharePartition = sharePartitions.get(topicIdPartition);

            Optional<LogOffsetMetadata> optionalFetchOffsetMetadata = sharePartition.fetchOffsetMetadata();
            if (optionalFetchOffsetMetadata.isEmpty() || optionalFetchOffsetMetadata.get() == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;
            LogOffsetMetadata fetchOffsetMetadata = optionalFetchOffsetMetadata.get();

            if (fetchOffsetMetadata.messageOffset > endOffsetMetadata.messageOffset) {
                log.debug("Satisfying delayed share fetch request for group {}, member {} since it is fetching later segments of " +
                    "topicIdPartition {}", shareFetchData.groupId(), shareFetchData.memberId(), topicIdPartition);
                return true;
            } else if (fetchOffsetMetadata.messageOffset < endOffsetMetadata.messageOffset) {
                if (fetchOffsetMetadata.onOlderSegment(endOffsetMetadata)) {
                    // This can happen when the fetch operation is falling behind the current segment or the partition
                    // has just rolled a new segment.
                    log.debug("Satisfying delayed share fetch request for group {}, member {} immediately since it is fetching older " +
                        "segments of topicIdPartition {}", shareFetchData.groupId(), shareFetchData.memberId(), topicIdPartition);
                    return true;
                } else if (fetchOffsetMetadata.onSameSegment(endOffsetMetadata)) {
                    // we take the partition fetch size as upper bound when accumulating the bytes.
                    long bytesAvailable = Math.min(endOffsetMetadata.positionDiff(fetchOffsetMetadata), partitionData.maxBytes);
                    accumulatedSize += bytesAvailable;
                }
            }
        }
        return accumulatedSize >= shareFetchData.fetchParams().minBytes;
    }

    private LogOffsetMetadata endOffsetMetadataForTopicPartition(TopicIdPartition topicIdPartition) {
        Partition partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition());
        LogOffsetSnapshot offsetSnapshot = partition.fetchOffsetSnapshot(Optional.empty(), true);
        // The FetchIsolation type that we use for share fetch is FetchIsolation.HIGH_WATERMARK. In the future, we can
        // extend it to support other FetchIsolation types.
        FetchIsolation isolationType = shareFetchData.fetchParams().isolation;
        if (isolationType == FetchIsolation.LOG_END)
            return offsetSnapshot.logEndOffset;
        else if (isolationType == FetchIsolation.HIGH_WATERMARK)
            return offsetSnapshot.highWatermark;
        else
            return offsetSnapshot.lastStableOffset;

    }

    private Map<TopicIdPartition, LogReadResult> readFromLog(Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData) {
        Seq<Tuple2<TopicIdPartition, LogReadResult>> responseLogResult = replicaManager.readFromLog(
            shareFetchData.fetchParams(),
            CollectionConverters.asScala(
                topicPartitionData.entrySet().stream().map(entry ->
                    new Tuple2<>(entry.getKey(), entry.getValue())).collect(Collectors.toList())
            ),
            QuotaFactory.UNBOUNDED_QUOTA,
            true);

        Map<TopicIdPartition, LogReadResult> responseData = new HashMap<>();
        responseLogResult.foreach(tpLogResult -> {
            responseData.put(tpLogResult._1(), tpLogResult._2());
            return BoxedUnit.UNIT;
        });

        log.trace("Data successfully retrieved by replica manager: {}", responseData);
        return responseData;
    }

    private boolean anyPartitionHasLogReadError(Map<TopicIdPartition, LogReadResult> replicaManagerReadResponse) {
        return replicaManagerReadResponse.values().stream()
            .anyMatch(logReadResult -> logReadResult.error().code() != Errors.NONE.code());
    }

    // Visible for testing.
    Map<TopicIdPartition, LogReadResult> combineLogReadResponse(Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData,
                                                                Map<TopicIdPartition, LogReadResult> existingFetchedData) {
        Map<TopicIdPartition, FetchRequest.PartitionData> missingLogReadTopicPartitions = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, partitionData) -> {
            if (!existingFetchedData.containsKey(topicIdPartition)) {
                missingLogReadTopicPartitions.put(topicIdPartition, partitionData);
            }
        });
        if (missingLogReadTopicPartitions.isEmpty()) {
            return existingFetchedData;
        }
        Map<TopicIdPartition, LogReadResult> missingTopicPartitionsLogReadResponse = readFromLog(missingLogReadTopicPartitions);
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
}
