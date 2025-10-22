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
import kafka.server.QuotaFactory;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.purgatory.DelayedOperation;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchGroupKey;
import org.apache.kafka.server.share.fetch.DelayedShareFetchPartitionKey;
import org.apache.kafka.server.share.fetch.PartitionMaxBytesStrategy;
import org.apache.kafka.server.share.fetch.ShareFetch;
import org.apache.kafka.server.share.fetch.ShareFetchPartitionData;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import static kafka.server.share.PendingRemoteFetches.RemoteFetch;

/**
 * A delayed share fetch operation has been introduced in case there is a share fetch request which cannot be completed instantaneously.
 */
public class DelayedShareFetch extends DelayedOperation {

    private static final Logger log = LoggerFactory.getLogger(DelayedShareFetch.class);

    private static final String EXPIRES_PER_SEC = "ExpiresPerSec";

    private final ShareFetch shareFetch;
    private final ReplicaManager replicaManager;
    private final BiConsumer<SharePartitionKey, Throwable> exceptionHandler;
    private final PartitionMaxBytesStrategy partitionMaxBytesStrategy;
    private final ShareGroupMetrics shareGroupMetrics;
    private final Time time;
    // The topic partitions that need to be completed for the share fetch request are given by sharePartitions.
    // sharePartitions is a subset of shareFetchData. The order of insertion/deletion of entries in sharePartitions is important.
    private final LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions;
    /**
     * Metric for the rate of expired delayed fetch requests.
     */
    private final Meter expiredRequestMeter;
    /**
     * fetchId serves as a token while acquiring/releasing share partition's fetch lock.
     */
    private final Uuid fetchId;
    // Tracks the start time to acquire any share partition for a fetch request.
    private long acquireStartTimeMs;
    private LinkedHashMap<TopicIdPartition, Long> partitionsAcquired;
    private LinkedHashMap<TopicIdPartition, LogReadResult> localPartitionsAlreadyFetched;
    private Optional<PendingRemoteFetches> pendingRemoteFetchesOpt;
    private Optional<Exception> remoteStorageFetchException;
    private final AtomicBoolean outsidePurgatoryCallbackLock;
    private final long remoteFetchMaxWaitMs;

    /**
     * This function constructs an instance of delayed share fetch operation for completing share fetch
     * requests instantaneously or with delay.
     *
     * @param shareFetch The share fetch parameters of the share fetch request.
     * @param replicaManager The replica manager instance used to read from log/complete the request.
     * @param exceptionHandler The handler to complete share fetch requests with exception.
     * @param sharePartitions The share partitions referenced in the share fetch request.
     * @param shareGroupMetrics The share group metrics to record the metrics.
     * @param time The system time.
     * @param remoteFetchMaxWaitMs The max wait time for a share fetch request having remote storage fetch.
     */
    public DelayedShareFetch(
            ShareFetch shareFetch,
            ReplicaManager replicaManager,
            BiConsumer<SharePartitionKey, Throwable> exceptionHandler,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
            ShareGroupMetrics shareGroupMetrics,
            Time time,
            long remoteFetchMaxWaitMs
    ) {
        this(shareFetch,
            replicaManager,
            exceptionHandler,
            sharePartitions,
            PartitionMaxBytesStrategy.type(PartitionMaxBytesStrategy.StrategyType.UNIFORM),
            shareGroupMetrics,
            time,
            Optional.empty(),
            Uuid.randomUuid(),
            remoteFetchMaxWaitMs
        );
    }

    /**
     * This function constructs an instance of delayed share fetch operation for completing share fetch
     * requests instantaneously or with delay. The direct usage of this constructor is only from tests.
     *
     * @param shareFetch The share fetch parameters of the share fetch request.
     * @param replicaManager The replica manager instance used to read from log/complete the request.
     * @param exceptionHandler The handler to complete share fetch requests with exception.
     * @param sharePartitions The share partitions referenced in the share fetch request.
     * @param partitionMaxBytesStrategy The strategy to identify the max bytes for topic partitions in the share fetch request.
     * @param shareGroupMetrics The share group metrics to record the metrics.
     * @param time The system time.
     * @param pendingRemoteFetchesOpt Optional containing an in-flight remote fetch object or an empty optional.
     * @param remoteFetchMaxWaitMs The max wait time for a share fetch request having remote storage fetch.
     */
    DelayedShareFetch(
        ShareFetch shareFetch,
        ReplicaManager replicaManager,
        BiConsumer<SharePartitionKey, Throwable> exceptionHandler,
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
        PartitionMaxBytesStrategy partitionMaxBytesStrategy,
        ShareGroupMetrics shareGroupMetrics,
        Time time,
        Optional<PendingRemoteFetches> pendingRemoteFetchesOpt,
        Uuid fetchId,
        long remoteFetchMaxWaitMs
    ) {
        super(shareFetch.fetchParams().maxWaitMs);
        this.shareFetch = shareFetch;
        this.replicaManager = replicaManager;
        this.partitionsAcquired = new LinkedHashMap<>();
        this.localPartitionsAlreadyFetched = new LinkedHashMap<>();
        this.exceptionHandler = exceptionHandler;
        this.sharePartitions = sharePartitions;
        this.partitionMaxBytesStrategy = partitionMaxBytesStrategy;
        this.shareGroupMetrics = shareGroupMetrics;
        this.time = time;
        this.acquireStartTimeMs = time.hiResClockMs();
        this.pendingRemoteFetchesOpt = pendingRemoteFetchesOpt;
        this.remoteStorageFetchException = Optional.empty();
        this.fetchId = fetchId;
        this.outsidePurgatoryCallbackLock = new AtomicBoolean(false);
        this.remoteFetchMaxWaitMs = remoteFetchMaxWaitMs;
        // Register metrics for DelayedShareFetch.
        KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "DelayedShareFetchMetrics");
        this.expiredRequestMeter = metricsGroup.newMeter(EXPIRES_PER_SEC, "requests", TimeUnit.SECONDS);
    }

    @Override
    public void onExpiration() {
        expiredRequestMeter.mark();
    }

    /**
     * Complete the share fetch operation by fetching records for all partitions in the share fetch request irrespective
     * of whether they have any acquired records. This is called when the fetch operation is forced to complete either
     * because records can be acquired for some partitions or due to MaxWaitMs timeout.
     * <p>
     * On operation timeout, onComplete is invoked, last try occurs to acquire partitions and read
     * from log, if acquired. The fetch will only happen from local log and not remote storage, on
     * operation expiration.
     */
    @Override
    public void onComplete() {
        log.trace("Completing the delayed share fetch request for group {}, member {}, "
            + "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
            partitionsAcquired.keySet());

        if (remoteStorageFetchException.isPresent()) {
            completeErroneousRemoteShareFetchRequest();
        } else if (pendingRemoteFetchesOpt.isPresent()) {
            if (maybeRegisterCallbackPendingRemoteFetch()) {
                log.trace("Registered remote storage fetch callback for group {}, member {}, "
                        + "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                    partitionsAcquired.keySet());
                return;
            }
            completeRemoteStorageShareFetchRequest();
        } else {
            completeLocalLogShareFetchRequest();
        }
    }

    private void completeLocalLogShareFetchRequest() {
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData;
        // tryComplete did not invoke forceComplete, so we need to check if we have any partitions to fetch.
        if (partitionsAcquired.isEmpty()) {
            topicPartitionData = acquirablePartitions(sharePartitions);
            // The TopicPartitionsAcquireTimeMs metric signifies the tension when acquiring the locks
            // for the share partition, hence if no partitions are yet acquired by tryComplete,
            // we record the metric here. Do not check if the request has successfully acquired any
            // partitions now or not, as then the upper bound of request timeout shall be recorded
            // for the metric.
            updateAcquireElapsedTimeMetric();
        } else {
            // tryComplete invoked forceComplete, so we can use the data from tryComplete.
            topicPartitionData = partitionsAcquired;
        }

        if (topicPartitionData.isEmpty()) {
            // No locks for share partitions could be acquired, so we complete the request with an empty response.
            shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), 0);
            shareFetch.maybeComplete(Map.of());
            return;
        } else {
            // Update metric to record acquired to requested partitions.
            double requestTopicToAcquired = (double) topicPartitionData.size() / shareFetch.topicIdPartitions().size();
            shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), (int) (requestTopicToAcquired * 100));
        }
        log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
            topicPartitionData, shareFetch.groupId(), shareFetch.fetchParams());

        processAcquiredTopicPartitionsForLocalLogFetch(topicPartitionData);
    }

    private void processAcquiredTopicPartitionsForLocalLogFetch(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        try {
            LinkedHashMap<TopicIdPartition, LogReadResult> responseData;
            if (localPartitionsAlreadyFetched.isEmpty())
                responseData = readFromLog(
                    topicPartitionData,
                    partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, topicPartitionData.keySet(), topicPartitionData.size()));
            else
                // There shouldn't be a case when we have a partitionsAlreadyFetched value here and this variable is getting
                // updated in a different tryComplete thread.
                responseData = combineLogReadResponse(topicPartitionData, localPartitionsAlreadyFetched);

            resetFetchOffsetMetadataForRemoteFetchPartitions(topicPartitionData, responseData);

            List<ShareFetchPartitionData> shareFetchPartitionDataList = new ArrayList<>();
            responseData.forEach((topicIdPartition, logReadResult) -> {
                if (logReadResult.info().delayedRemoteStorageFetch.isEmpty()) {
                    shareFetchPartitionDataList.add(new ShareFetchPartitionData(
                        topicIdPartition,
                        topicPartitionData.get(topicIdPartition),
                        logReadResult.toFetchPartitionData(false)
                    ));
                }
            });

            shareFetch.maybeComplete(ShareFetchUtils.processFetchResponse(
                shareFetch,
                shareFetchPartitionDataList,
                sharePartitions,
                replicaManager,
                exceptionHandler
            ));
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request", e);
            handleFetchException(shareFetch, topicPartitionData.keySet(), e);
        } finally {
            releasePartitionLocksAndAddToActionQueue(topicPartitionData.keySet());
        }
    }

    /**
     * This function updates the cached fetch offset metadata to null corresponding to the share partition's fetch offset.
     * This is required in the case when a topic partition that has local log fetch during tryComplete, but changes to remote
     * storage fetch in onComplete. In this situation, if the cached fetchOffsetMetadata got updated in tryComplete, then
     * we will enter a state where each share fetch request for this topic partition from client will use the cached
     * fetchOffsetMetadata in tryComplete and return an empty response to the client from onComplete.
     * Hence, we require to set offsetMetadata to null for this fetch offset, which would cause tryComplete to update
     * fetchOffsetMetadata and thereby we will identify this partition for remote storage fetch.
     * @param topicPartitionData - Map containing the fetch offset for the topic partitions.
     * @param replicaManagerReadResponse - Map containing the readFromLog response from replicaManager for the topic partitions.
     */
    private void resetFetchOffsetMetadataForRemoteFetchPartitions(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse
    ) {
        replicaManagerReadResponse.forEach((topicIdPartition, logReadResult) -> {
            if (logReadResult.info().delayedRemoteStorageFetch.isPresent()) {
                SharePartition sharePartition = sharePartitions.get(topicIdPartition);
                sharePartition.updateFetchOffsetMetadata(
                    topicPartitionData.get(topicIdPartition),
                    null
                );
            }
        });
    }

    /**
     * Try to complete the fetch operation if we can acquire records for any partition in the share fetch request.
     */
    @Override
    public boolean tryComplete() {
        // Check to see if the remote fetch is in flight. If there is an in flight remote fetch we want to resolve it first.
        if (pendingRemoteFetchesOpt.isPresent()) {
            return maybeCompletePendingRemoteFetch();
        }

        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = acquirablePartitions(sharePartitions);
        try {
            if (!topicPartitionData.isEmpty()) {
                // Update the metric to record the time taken to acquire the locks for the share partitions.
                updateAcquireElapsedTimeMetric();
                // In case, fetch offset metadata doesn't exist for one or more topic partitions, we do a
                // replicaManager.readFromLog to populate the offset metadata and update the fetch offset metadata for
                // those topic partitions.
                LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse = maybeReadFromLog(topicPartitionData);
                // Store the remote fetch info for the topic partitions for which we need to perform remote fetch.
                LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap = maybePrepareRemoteStorageFetchInfo(topicPartitionData, replicaManagerReadResponse);

                if (!remoteStorageFetchInfoMap.isEmpty()) {
                    return maybeProcessRemoteFetch(topicPartitionData, remoteStorageFetchInfoMap);
                }
                maybeUpdateFetchOffsetMetadata(topicPartitionData, replicaManagerReadResponse);
                if (anyPartitionHasLogReadError(replicaManagerReadResponse) || isMinBytesSatisfied(topicPartitionData, partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, topicPartitionData.keySet(), topicPartitionData.size()))) {
                    partitionsAcquired = topicPartitionData;
                    localPartitionsAlreadyFetched = replicaManagerReadResponse;
                    return forceComplete();
                } else {
                    log.debug("minBytes is not satisfied for the share fetch request for group {}, member {}, " +
                            "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                        sharePartitions.keySet());
                    releasePartitionLocks(topicPartitionData.keySet());
                }
            } else {
                log.trace("Can't acquire any partitions in the share fetch request for group {}, member {}, " +
                        "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                    sharePartitions.keySet());
            }
            // At this point, there could be delayed requests sitting in the purgatory which are waiting on
            // DelayedShareFetchPartitionKeys corresponding to partitions, whose leader has been changed to a different broker.
            // In that case, such partitions would not be able to get acquired, and the tryComplete will keep on returning false.
            // Eventually the operation will get timed out and completed, but it might not get removed from the purgatory.
            // This has been eventually left it like this because the purging mechanism will trigger whenever the number of completed
            // but still being watched operations is larger than the purge interval. This purge interval is defined by the config
            // share.fetch.purgatory.purge.interval.requests and is 1000 by default, thereby ensuring that such stale operations do not
            // grow indefinitely.
            return false;
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request", e);
            // In case we have a remote fetch exception, we have already released locks for partitions which have potential
            // local log read. We do not release locks for partitions which have a remote storage read because we need to
            // complete the share fetch request in onComplete and if we release the locks early here, some other DelayedShareFetch
            // request might get the locks for those partitions without this one getting complete.
            if (remoteStorageFetchException.isEmpty()) {
                releasePartitionLocks(topicPartitionData.keySet());
                partitionsAcquired.clear();
                localPartitionsAlreadyFetched.clear();
            }
            return forceComplete();
        }
    }

    /**
     * Prepare fetch request structure for partitions in the share fetch request for which we can acquire records.
     */
    // Visible for testing
    LinkedHashMap<TopicIdPartition, Long> acquirablePartitions(
        LinkedHashMap<TopicIdPartition, SharePartition> sharePartitionsForAcquire
    ) {
        // Initialize the topic partitions for which the fetch should be attempted.
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = new LinkedHashMap<>();

        sharePartitionsForAcquire.forEach((topicIdPartition, sharePartition) -> {
            // Add the share partition to the list of partitions to be fetched only if we can
            // acquire the fetch lock on it.
            if (sharePartition.maybeAcquireFetchLock(fetchId)) {
                try {
                    log.trace("Fetch lock for share partition {}-{} has been acquired by {}", shareFetch.groupId(), topicIdPartition, fetchId);
                    // If the share partition is already at capacity, we should not attempt to fetch.
                    if (sharePartition.canAcquireRecords()) {
                        topicPartitionData.put(topicIdPartition, sharePartition.nextFetchOffset());
                    } else {
                        sharePartition.releaseFetchLock(fetchId);
                        log.trace("Record lock partition limit exceeded for SharePartition {}-{}, " +
                            "cannot acquire more records. Releasing the fetch lock by {}", shareFetch.groupId(), topicIdPartition, fetchId);
                    }
                } catch (Exception e) {
                    log.error("Error checking condition for SharePartition: {}-{}", shareFetch.groupId(), topicIdPartition, e);
                    // Release the lock, if error occurred.
                    sharePartition.releaseFetchLock(fetchId);
                    log.trace("Fetch lock for share partition {}-{} is being released by {}", shareFetch.groupId(), topicIdPartition, fetchId);
                }
            }
        });
        return topicPartitionData;
    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> maybeReadFromLog(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        LinkedHashMap<TopicIdPartition, Long> partitionsNotMatchingFetchOffsetMetadata = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            if (sharePartition.fetchOffsetMetadata(fetchOffset).isEmpty()) {
                partitionsNotMatchingFetchOffsetMetadata.put(topicIdPartition, fetchOffset);
            }
        });
        if (partitionsNotMatchingFetchOffsetMetadata.isEmpty()) {
            return new LinkedHashMap<>();
        }
        // We fetch data from replica manager corresponding to the topic partitions that have missing fetch offset metadata.
        // Although we are fetching partition max bytes for partitionsNotMatchingFetchOffsetMetadata,
        // we will take acquired partitions size = topicPartitionData.size() because we do not want to let the
        // leftover partitions to starve which will be fetched later.
        return readFromLog(
            partitionsNotMatchingFetchOffsetMetadata,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, partitionsNotMatchingFetchOffsetMetadata.keySet(), topicPartitionData.size()));
    }

    private void maybeUpdateFetchOffsetMetadata(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
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
                topicPartitionData.get(topicIdPartition),
                replicaManagerLogReadResult.info().fetchOffsetMetadata);
        }
    }

    // minByes estimation currently assumes the common case where all fetched data is acquirable.
    private boolean isMinBytesSatisfied(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                        LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        long accumulatedSize = 0;
        for (Map.Entry<TopicIdPartition, Long> entry : topicPartitionData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            long fetchOffset = entry.getValue();

            LogOffsetMetadata endOffsetMetadata;
            try {
                endOffsetMetadata = endOffsetMetadataForTopicPartition(topicIdPartition);
            } catch (Exception e) {
                shareFetch.addErroneous(topicIdPartition, e);
                exceptionHandler.accept(
                    new SharePartitionKey(shareFetch.groupId(), topicIdPartition), e);
                continue;
            }

            if (endOffsetMetadata == LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)
                continue;

            SharePartition sharePartition = sharePartitions.get(topicIdPartition);

            Optional<LogOffsetMetadata> optionalFetchOffsetMetadata = sharePartition.fetchOffsetMetadata(fetchOffset);
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
                    long bytesAvailable = Math.min(endOffsetMetadata.positionDiff(fetchOffsetMetadata), partitionMaxBytes.get(topicIdPartition));
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
            return offsetSnapshot.logEndOffset();
        else if (isolationType == FetchIsolation.HIGH_WATERMARK)
            return offsetSnapshot.highWatermark();
        else
            return offsetSnapshot.lastStableOffset();

    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> readFromLog(LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
                                                                       LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        // Filter if there already exists any erroneous topic partition.
        Set<TopicIdPartition> partitionsToFetch = shareFetch.filterErroneousTopicPartitions(topicPartitionFetchOffsets.keySet());
        if (partitionsToFetch.isEmpty()) {
            return new LinkedHashMap<>();
        }

        LinkedHashMap<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();

        topicPartitionFetchOffsets.forEach((topicIdPartition, fetchOffset) -> topicPartitionData.put(topicIdPartition,
            new FetchRequest.PartitionData(
                topicIdPartition.topicId(),
                fetchOffset,
                0,
                partitionMaxBytes.get(topicIdPartition),
                Optional.empty())
        ));

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
        topicIdPartitions.forEach(topicIdPartition -> exceptionHandler.accept(
            new SharePartitionKey(shareFetch.groupId(), topicIdPartition), throwable));
        shareFetch.maybeCompleteWithException(topicIdPartitions, throwable);
    }

    /**
     * The method updates the metric for the time taken to acquire the share partition locks. Also,
     * it resets the acquireStartTimeMs to the current time, so that the metric records the time taken
     * to acquire the locks for the re-try, if the partitions are re-acquired. The partitions can be
     * re-acquired if the fetch request is not completed because of the minBytes or some other condition.
     */
    private void updateAcquireElapsedTimeMetric() {
        long currentTimeMs = time.hiResClockMs();
        shareGroupMetrics.recordTopicPartitionsAcquireTimeMs(shareFetch.groupId(), currentTimeMs - acquireStartTimeMs);
        // Reset the acquireStartTimeMs to the current time. If the fetch request is not completed
        // and the partitions are re-acquired then metric should record value from the last acquire time.
        acquireStartTimeMs = currentTimeMs;
    }

    // Visible for testing.
    LinkedHashMap<TopicIdPartition, LogReadResult> combineLogReadResponse(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                                                          LinkedHashMap<TopicIdPartition, LogReadResult> existingFetchedData) {
        LinkedHashMap<TopicIdPartition, Long> missingLogReadTopicPartitions = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            if (!existingFetchedData.containsKey(topicIdPartition)) {
                missingLogReadTopicPartitions.put(topicIdPartition, fetchOffset);
            }
        });
        if (missingLogReadTopicPartitions.isEmpty()) {
            return existingFetchedData;
        }

        LinkedHashMap<TopicIdPartition, LogReadResult> missingTopicPartitionsLogReadResponse = readFromLog(
            missingLogReadTopicPartitions,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, missingLogReadTopicPartitions.keySet(), topicPartitionData.size()));
        missingTopicPartitionsLogReadResponse.putAll(existingFetchedData);
        return missingTopicPartitionsLogReadResponse;
    }

    // Visible for testing.
    void releasePartitionLocks(Set<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.forEach(tp -> {
            SharePartition sharePartition = sharePartitions.get(tp);
            sharePartition.releaseFetchLock(fetchId);
            log.trace("Fetch lock for share partition {}-{} is being released by {}", shareFetch.groupId(), tp, fetchId);
        });
    }

    // Visible for testing.
    Lock lock() {
        return lock;
    }

    // Visible for testing.
    PendingRemoteFetches pendingRemoteFetches() {
        return pendingRemoteFetchesOpt.orElse(null);
    }

    // Visible for testing.
    boolean outsidePurgatoryCallbackLock() {
        return outsidePurgatoryCallbackLock.get();
    }

    // Only used for testing purpose.
    void updatePartitionsAcquired(LinkedHashMap<TopicIdPartition, Long> partitionsAcquired) {
        this.partitionsAcquired = partitionsAcquired;
    }

    // Visible for testing.
    Meter expiredRequestMeter() {
        return expiredRequestMeter;
    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> maybePrepareRemoteStorageFetchInfo(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> replicaManagerReadResponse
    ) {
        LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap = new LinkedHashMap<>();
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : replicaManagerReadResponse.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            LogReadResult logReadResult = entry.getValue();
            if (logReadResult.info().delayedRemoteStorageFetch.isPresent()) {
                remoteStorageFetchInfoMap.put(topicIdPartition, logReadResult);
                partitionsAcquired.put(topicIdPartition, topicPartitionData.get(topicIdPartition));
            }
        }
        return remoteStorageFetchInfoMap;
    }

    private boolean maybeProcessRemoteFetch(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap
    ) {
        Set<TopicIdPartition> nonRemoteFetchTopicPartitions = new LinkedHashSet<>();
        topicPartitionData.keySet().forEach(topicIdPartition -> {
            // non-remote storage fetch topic partitions for which fetch would not be happening in this share fetch request.
            if (!remoteStorageFetchInfoMap.containsKey(topicIdPartition)) {
                nonRemoteFetchTopicPartitions.add(topicIdPartition);
            }
        });
        // Release fetch lock for the topic partitions that were acquired but were not a part of remote fetch and add
        // them to the delayed actions queue.
        releasePartitionLocksAndAddToActionQueue(nonRemoteFetchTopicPartitions);
        processRemoteFetchOrException(remoteStorageFetchInfoMap);
        // Check if remote fetch can be completed.
        return maybeCompletePendingRemoteFetch();
    }

    private boolean maybeRegisterCallbackPendingRemoteFetch() {
        log.trace("Registering callback pending remote fetch");
        PendingRemoteFetches pendingFetch = pendingRemoteFetchesOpt.get();
        if (!pendingFetch.isDone() && shareFetch.fetchParams().maxWaitMs < remoteFetchMaxWaitMs) {
            TimerTask timerTask = new PendingRemoteFetchTimerTask();
            pendingFetch.invokeCallbackOnCompletion(((ignored, throwable) -> {
                timerTask.cancel();
                log.trace("Invoked remote storage fetch callback for group {}, member {}, "
                        + "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                    partitionsAcquired.keySet());
                if (throwable != null) {
                    log.error("Remote storage fetch failed for group {}, member {}, topic partitions {}",
                        shareFetch.groupId(), shareFetch.memberId(), sharePartitions.keySet(), throwable);
                }
                completeRemoteShareFetchRequestOutsidePurgatory();
            }));
            replicaManager.addShareFetchTimerRequest(timerTask);
            return true;
        }
        return false;
    }

    /**
     * Throws an exception if a task for remote storage fetch could not be scheduled successfully else updates pendingRemoteFetchesOpt.
     * @param remoteStorageFetchInfoMap - The remote storage fetch information.
     */
    private void processRemoteFetchOrException(
        LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap
    ) {
        LinkedHashMap<TopicIdPartition, LogOffsetMetadata> fetchOffsetMetadataMap = new LinkedHashMap<>();
        remoteStorageFetchInfoMap.forEach((topicIdPartition, logReadResult) -> fetchOffsetMetadataMap.put(
            topicIdPartition,
            logReadResult.info().fetchOffsetMetadata
        ));

        List<RemoteFetch> remoteFetches = new ArrayList<>();
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : remoteStorageFetchInfoMap.entrySet()) {
            TopicIdPartition remoteFetchTopicIdPartition = entry.getKey();
            RemoteStorageFetchInfo remoteStorageFetchInfo = entry.getValue().info().delayedRemoteStorageFetch.get();

            Future<Void> remoteFetchTask;
            CompletableFuture<RemoteLogReadResult> remoteFetchResult = new CompletableFuture<>();
            try {
                remoteFetchTask = replicaManager.remoteLogManager().get().asyncRead(
                    remoteStorageFetchInfo,
                    result -> {
                        remoteFetchResult.complete(result);
                        replicaManager.completeDelayedShareFetchRequest(new DelayedShareFetchGroupKey(shareFetch.groupId(), remoteFetchTopicIdPartition.topicId(), remoteFetchTopicIdPartition.partition()));
                    }
                );
            } catch (Exception e) {
                // Cancel the already created remote fetch tasks in case an exception occurs.
                remoteFetches.forEach(this::cancelRemoteFetchTask);
                // Throw the error if any in scheduling the remote fetch task.
                remoteStorageFetchException = Optional.of(e);
                throw e;
            }
            remoteFetches.add(new RemoteFetch(remoteFetchTopicIdPartition, entry.getValue(), remoteFetchTask, remoteFetchResult, remoteStorageFetchInfo));
        }
        pendingRemoteFetchesOpt = Optional.of(new PendingRemoteFetches(remoteFetches, fetchOffsetMetadataMap));
    }

    /**
     * This function checks if the remote fetch can be completed or not. It should always be called once you confirm pendingRemoteFetchesOpt.isPresent().
     * The operation can be completed if:
     * Case a: The partition is in an offline log directory on this broker
     * Case b: This broker does not know the partition it tries to fetch
     * Case c: This broker is no longer the leader of the partition it tries to fetch
     * Case d: This broker is no longer the leader or follower of the partition it tries to fetch
     * Case e: All remote storage read requests completed
     * @return boolean representing whether the remote fetch is completed or not.
     */
    private boolean maybeCompletePendingRemoteFetch() {
        boolean canComplete = false;

        for (TopicIdPartition topicIdPartition : pendingRemoteFetchesOpt.get().fetchOffsetMetadataMap().keySet()) {
            try {
                Partition partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition());
                if (!partition.isLeader()) {
                    throw new NotLeaderException("Broker is no longer the leader of topicPartition: " + topicIdPartition);
                }
            } catch (KafkaStorageException e) { // Case a
                log.debug("TopicPartition {} is in an offline log directory, satisfy {} immediately", topicIdPartition, shareFetch.fetchParams());
                canComplete = true;
            } catch (UnknownTopicOrPartitionException e) { // Case b
                log.debug("Broker no longer knows of topicPartition {}, satisfy {} immediately", topicIdPartition, shareFetch.fetchParams());
                canComplete = true;
            } catch (NotLeaderException e) { // Case c
                log.debug("Broker is no longer the leader of topicPartition {}, satisfy {} immediately", topicIdPartition, shareFetch.fetchParams());
                canComplete = true;
            } catch (NotLeaderOrFollowerException e) { // Case d
                log.debug("Broker is no longer the leader or follower of topicPartition {}, satisfy {} immediately", topicIdPartition, shareFetch.fetchParams());
                canComplete = true;
            }
            if (canComplete)
                break;
        }

        if (canComplete || pendingRemoteFetchesOpt.get().isDone()) { // Case e
            return forceComplete();
        } else
            return false;
    }

    /**
     * This function completes a share fetch request for which we have identified erroneous remote storage fetch in tryComplete()
     * It should only be called when we know that there is remote fetch in-flight/completed.
     */
    private void completeErroneousRemoteShareFetchRequest() {
        try {
            handleFetchException(shareFetch, partitionsAcquired.keySet(), remoteStorageFetchException.get());
        } finally {
            releasePartitionLocksAndAddToActionQueue(partitionsAcquired.keySet());
        }

    }

    private void releasePartitionLocksAndAddToActionQueue(Set<TopicIdPartition> topicIdPartitions) {
        if (topicIdPartitions.isEmpty()) {
            return;
        }
        // Releasing the lock to move ahead with the next request in queue.
        releasePartitionLocks(topicIdPartitions);
        replicaManager.addToActionQueue(() -> topicIdPartitions.forEach(topicIdPartition -> {
            // If we have a fetch request completed for a share-partition, we release the locks for that partition,
            // then we should check if there is a pending share fetch request for the share-partition and complete it.
            // We add the action to delayed actions queue to avoid an infinite call stack, which could happen if
            // we directly call delayedShareFetchPurgatory.checkAndComplete.
            replicaManager.completeDelayedShareFetchRequest(
                new DelayedShareFetchGroupKey(shareFetch.groupId(), topicIdPartition.topicId(), topicIdPartition.partition()));
            // As DelayedShareFetch operation is watched over multiple keys, same operation might be
            // completed and can contain references to data fetched. Hence, if the operation is not
            // removed from other watched keys then there can be a memory leak. The removal of the
            // operation is dependent on the purge task by DelayedOperationPurgatory. Hence, this can
            // also be prevented by setting smaller value for configuration {@link ShareGroupConfig#SHARE_FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG}.
            // However, it's best to trigger the check on all the keys that are being watched which
            // should free the memory for the completed operation.
            replicaManager.completeDelayedShareFetchRequest(new DelayedShareFetchPartitionKey(topicIdPartition));
        }));
    }

    /**
     * This function completes a share fetch request for which we have identified remoteFetch during tryComplete()
     * Note - This function should only be called when we know that there is remote fetch.
     */
    private void completeRemoteStorageShareFetchRequest() {
        LinkedHashMap<TopicIdPartition, Long> acquiredNonRemoteFetchTopicPartitionData = new LinkedHashMap<>();
        try {
            List<ShareFetchPartitionData> shareFetchPartitionData = new ArrayList<>();
            int readableBytes = 0;
            for (RemoteFetch remoteFetch : pendingRemoteFetchesOpt.get().remoteFetches()) {
                if (remoteFetch.remoteFetchResult().isDone()) {
                    RemoteLogReadResult remoteLogReadResult = remoteFetch.remoteFetchResult().get();
                    if (remoteLogReadResult.error().isPresent()) {
                        // If there is any error for the remote fetch topic partition, we populate the error accordingly.
                        shareFetchPartitionData.add(
                            new ShareFetchPartitionData(
                                remoteFetch.topicIdPartition(),
                                partitionsAcquired.get(remoteFetch.topicIdPartition()),
                                new LogReadResult(Errors.forException(remoteLogReadResult.error().get())).toFetchPartitionData(false)
                            )
                        );
                    } else {
                        FetchDataInfo info = remoteLogReadResult.fetchDataInfo().get();
                        TopicIdPartition topicIdPartition = remoteFetch.topicIdPartition();
                        LogReadResult logReadResult = remoteFetch.logReadResult();
                        shareFetchPartitionData.add(
                            new ShareFetchPartitionData(
                                topicIdPartition,
                                partitionsAcquired.get(remoteFetch.topicIdPartition()),
                                new FetchPartitionData(
                                    logReadResult.error(),
                                    logReadResult.highWatermark(),
                                    logReadResult.leaderLogStartOffset(),
                                    info.records,
                                    Optional.empty(),
                                    logReadResult.lastStableOffset().isPresent() ? OptionalLong.of(logReadResult.lastStableOffset().getAsLong()) : OptionalLong.empty(),
                                    info.abortedTransactions,
                                    logReadResult.preferredReadReplica().isPresent() ? OptionalInt.of(logReadResult.preferredReadReplica().getAsInt()) : OptionalInt.empty(),
                                    false
                                )
                            )
                        );
                        readableBytes += info.records.sizeInBytes();
                    }
                } else {
                    cancelRemoteFetchTask(remoteFetch);
                }
            }

            // If remote fetch bytes  < shareFetch.fetchParams().maxBytes, then we will try for a local read.
            if (readableBytes < shareFetch.fetchParams().maxBytes) {
                // Get the local log read based topic partitions.
                LinkedHashMap<TopicIdPartition, SharePartition> nonRemoteFetchSharePartitions = new LinkedHashMap<>();
                sharePartitions.forEach((topicIdPartition, sharePartition) -> {
                    if (!partitionsAcquired.containsKey(topicIdPartition)) {
                        nonRemoteFetchSharePartitions.put(topicIdPartition, sharePartition);
                    }
                });
                acquiredNonRemoteFetchTopicPartitionData = acquirablePartitions(nonRemoteFetchSharePartitions);
                if (!acquiredNonRemoteFetchTopicPartitionData.isEmpty()) {
                    log.trace("Fetchable local share partitions for a remote share fetch request data: {} with groupId: {} fetch params: {}",
                        acquiredNonRemoteFetchTopicPartitionData, shareFetch.groupId(), shareFetch.fetchParams());

                    LinkedHashMap<TopicIdPartition, LogReadResult> responseData = readFromLog(
                        acquiredNonRemoteFetchTopicPartitionData,
                        partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes - readableBytes, acquiredNonRemoteFetchTopicPartitionData.keySet(), acquiredNonRemoteFetchTopicPartitionData.size()));
                    resetFetchOffsetMetadataForRemoteFetchPartitions(acquiredNonRemoteFetchTopicPartitionData, responseData);
                    for (Map.Entry<TopicIdPartition, LogReadResult> entry : responseData.entrySet()) {
                        if (entry.getValue().info().delayedRemoteStorageFetch.isEmpty()) {
                            shareFetchPartitionData.add(
                                new ShareFetchPartitionData(
                                    entry.getKey(),
                                    acquiredNonRemoteFetchTopicPartitionData.get(entry.getKey()),
                                    entry.getValue().toFetchPartitionData(false)
                                )
                            );
                        }
                    }
                }
            }

            // Update metric to record acquired to requested partitions.
            double acquiredRatio = (double) (partitionsAcquired.size() + acquiredNonRemoteFetchTopicPartitionData.size()) / shareFetch.topicIdPartitions().size();
            if (acquiredRatio > 0)
                shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), (int) (acquiredRatio * 100));

            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> remoteFetchResponse = ShareFetchUtils.processFetchResponse(
                shareFetch, shareFetchPartitionData, sharePartitions, replicaManager, exceptionHandler);
            shareFetch.maybeComplete(remoteFetchResponse);
            log.trace("Remote share fetch request completed successfully, response: {}", remoteFetchResponse);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception occurred in completing remote fetch {} for delayed share fetch request {}", pendingRemoteFetchesOpt.get(), e);
            handleExceptionInCompletingRemoteStorageShareFetchRequest(acquiredNonRemoteFetchTopicPartitionData.keySet(), e);
        } catch (Exception e) {
            log.error("Unexpected error in processing delayed share fetch request", e);
            handleExceptionInCompletingRemoteStorageShareFetchRequest(acquiredNonRemoteFetchTopicPartitionData.keySet(), e);
        } finally {
            Set<TopicIdPartition> topicIdPartitions = new LinkedHashSet<>(partitionsAcquired.keySet());
            topicIdPartitions.addAll(acquiredNonRemoteFetchTopicPartitionData.keySet());
            releasePartitionLocksAndAddToActionQueue(topicIdPartitions);
        }
    }

    private void handleExceptionInCompletingRemoteStorageShareFetchRequest(
        Set<TopicIdPartition> acquiredNonRemoteFetchTopicPartitions,
        Exception e
    ) {
        Set<TopicIdPartition> topicIdPartitions = new LinkedHashSet<>(partitionsAcquired.keySet());
        topicIdPartitions.addAll(acquiredNonRemoteFetchTopicPartitions);
        handleFetchException(shareFetch, topicIdPartitions, e);
    }

    /**
     * Cancel the remote storage read task, if it has not been executed yet and avoid interrupting the task if it is
     * already running as it may force closing opened/cached resources as transaction index.
     * Note - This function should only be called when we know that there is remote fetch.
     */
    private void cancelRemoteFetchTask(RemoteFetch remoteFetch) {
        boolean cancelled = remoteFetch.remoteFetchTask().cancel(false);
        if (!cancelled) {
            log.debug("Remote fetch task for RemoteStorageFetchInfo: {} could not be cancelled and its isDone value is {}",
                remoteFetch.remoteFetchInfo(), remoteFetch.remoteFetchTask().isDone());
        }
    }

    private void completeRemoteShareFetchRequestOutsidePurgatory() {
        if (outsidePurgatoryCallbackLock.compareAndSet(false, true)) {
            completeRemoteStorageShareFetchRequest();
        }
    }

    private class PendingRemoteFetchTimerTask extends TimerTask {

        public PendingRemoteFetchTimerTask() {
            super(remoteFetchMaxWaitMs - shareFetch.fetchParams().maxWaitMs);
        }

        @Override
        public void run() {
            log.trace("Expired remote storage fetch callback for group {}, member {}, "
                    + "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                partitionsAcquired.keySet());
            expiredRequestMeter.mark();
            completeRemoteShareFetchRequestOutsidePurgatory();
        }
    }
}
