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
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.purgatory.DelayedOperation;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.PartitionMetadataProvider;
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
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static kafka.server.share.PendingRemoteFetches.RemoteFetch;

/**
 * A delayed share fetch operation has been introduced in case there is a share fetch request which cannot be completed instantaneously.
 */
public class DelayedShareFetch extends DelayedOperation {

    private static final Logger log = LoggerFactory.getLogger(DelayedShareFetch.class);

    private static final String EXPIRES_PER_SEC = "ExpiresPerSec";

    private final ShareFetch shareFetch;
    private final ReplicaManager replicaManager;
    private final LogReader logReader;
    private final PartitionMetadataProvider metadataProvider;
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
    /** Holds the CompletableFuture for a pending async fetch operation.
     * Unlike remote fetches (which have one future per partition), async fetch has a single future
     * for all partitions. This is null when no async operation is pending. Partition locks are held
     * while this is non-null.
     */
    private CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pendingFetch;
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
            LogReader logReader,
            PartitionMetadataProvider metadataProvider,
            BiConsumer<SharePartitionKey, Throwable> exceptionHandler,
            LinkedHashMap<TopicIdPartition, SharePartition> sharePartitions,
            ShareGroupMetrics shareGroupMetrics,
            Time time,
            long remoteFetchMaxWaitMs
    ) {
        this(shareFetch,
            replicaManager,
            logReader,
            metadataProvider,
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
        LogReader logReader,
        PartitionMetadataProvider metadataProvider,
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
        this.logReader = logReader;
        this.metadataProvider = metadataProvider;
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

        // When the request reaches timeout, there could be an in-flight async read operation which
        // may or may not have completed. We need to check if the async read has completed to decide
        // whether to use its results or release the locks and complete with whatever data we have.
        if (pendingFetch != null) {
            completeShareFetchAsyncRequest();
        } else if (remoteStorageFetchException.isPresent()) {
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

    private void completeShareFetchAsyncRequest() {
        // If the async read is still not completed at timeout, rather than waiting further the request
        // should be completed without data.
        if (!pendingFetch.isDone()) {
            // Async read still in progress at timeout, cannot wait for it to complete. So release
            // locks and proceed without data. The new request will be created on next poll from client.
            completeAsyncRequestWithEmptyResponse(partitionsAcquired);
            return;
        }

        // Async read completed just before/at timeout. We can include its results in the response
        // without blocking since future is done. Do not proceed for tiered storage fetches
        // as request has already timed out.
        try {
            localPartitionsAlreadyFetched = pendingFetch.get();
        } catch (Exception e) {
            log.error("Error getting async fetch result for group {}, member {}",
                shareFetch.groupId(), shareFetch.memberId(), e);
            try {
                recordTopicPartitionsFetchRatioMetric(partitionsAcquired);
                handleFetchException(shareFetch, partitionsAcquired.keySet(), e);
            } finally {
                // Release the locks in a finally block so that a throw from the metric recording or the
                // exception handling above cannot leak the partition locks.
                releasePartitionLocks(partitionsAcquired.keySet());
            }
            return;
        } finally {
            // Clear the pending future regardless of completion status
            pendingFetch = null;
        }
        // If the code reaches here then that does mean the async pending fetch is completed successfully
        // and the data is retrieved in localPartitionsAlreadyFetched. Hence, we can proceed to complete
        // the request with the data. Skip the local log fetch re-try as the pending async fetch was
        // not null hence a call to fetch data has already been processed.
        completeLocalLogShareFetchRequest();
    }

    private void completeLocalLogShareFetchRequest() {
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData;
        // If tryComplete did not invoke forceComplete, so we need to check if we have any partitions to fetch.
        if (partitionsAcquired.isEmpty()) {
            topicPartitionData = acquirablePartitions(sharePartitions);
            // The TopicPartitionsAcquireTimeMs metric signifies the tension when acquiring the locks
            // for the share partition, hence if no partitions are yet acquired by tryComplete,
            // we record the metric here. Do not check if the request has successfully acquired any
            // partitions now or not, as then the upper bound of request timeout shall be recorded
            // for the metric.
            updateAcquireElapsedTimeMetric();
        } else {
            // tryComplete invoked forceComplete, so we can use the data from tryComplete. Or the async
            // fetch future was not completed, in either case some partitions should still be acquired.
            topicPartitionData = partitionsAcquired;
        }

        if (topicPartitionData.isEmpty()) {
            // No locks for share partitions could be acquired, so we complete the request with an empty response.
            shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), 0);
            shareFetch.maybeComplete(Map.of());
            return;
        } else {
            recordTopicPartitionsFetchRatioMetric(topicPartitionData);
        }
        log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
            topicPartitionData, shareFetch.groupId(), shareFetch.fetchParams());

        processAcquiredTopicPartitionsForLocalLogFetch(topicPartitionData);
    }

    private void processAcquiredTopicPartitionsForLocalLogFetch(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        // If localPartitionsAlreadyFetched is empty then the async fetch was never attempted, so read
        // from the log now. Otherwise, use the already fetched data and only read the remaining partitions.
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> readFuture;
        try {
            if (localPartitionsAlreadyFetched.isEmpty()) {
                readFuture = readFromLog(
                    topicPartitionData,
                    partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes,
                        topicPartitionData.keySet(), topicPartitionData.size())
                );
            } else {
                // Combine already-fetched data with new partitions that need fetching. Maybe only
                // use already fetched data from async fetch and never attempt for another retry as
                // data should have already been fetched for the partitions which were acquired in
                // tryComplete.
                readFuture = combineLogReadResponse(topicPartitionData, localPartitionsAlreadyFetched);
            }
        } catch (Exception e) {
            // Handle synchronous exceptions thrown by async read before future is returned
            log.error("Error initiating async fetch during request completion for group {}, member {}",
                shareFetch.groupId(), shareFetch.memberId(), e);
            handleFetchExceptionAndReleaseLocks(topicPartitionData.keySet(), e);
            return;
        }
        // If the future is successfully created then complete the request asynchronously. The completion
        // handler is registered from on-complete hence no 2 threads can run concurrently.
        readFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Async fetch failed during request completion for group {}, member {}",
                    shareFetch.groupId(), shareFetch.memberId(), throwable);
                handleFetchExceptionAndReleaseLocks(topicPartitionData.keySet(), throwable);
                return;
            }
            // Complete the share fetch request and release the locks as completion handler will complete
            // the request.
            processFetchResultAndComplete(result, topicPartitionData, shareFetchPartitionDataList ->
                shareFetch.maybeComplete(ShareFetchUtils.processFetchResponse(
                    shareFetch,
                    shareFetchPartitionDataList,
                    sharePartitions,
                    metadataProvider,
                    exceptionHandler
                )));
        });
    }

    private void processFetchResultAndComplete(
        LinkedHashMap<TopicIdPartition, LogReadResult> responseData,
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        Consumer<List<ShareFetchPartitionData>> completionConsumer
    ) {
        List<ShareFetchPartitionData> shareFetchPartitionDataList = new ArrayList<>();
        try {
            // Reset fetch offset metadata for partitions requiring remote fetch
            resetFetchOffsetMetadataForRemoteFetchPartitions(topicPartitionData, responseData);

            // Build response data excluding partitions needing remote fetch
            responseData.forEach((topicIdPartition, logReadResult) -> {
                if (logReadResult.info().delayedRemoteStorageFetch.isEmpty()) {
                    shareFetchPartitionDataList.add(new ShareFetchPartitionData(
                        topicIdPartition,
                        topicPartitionData.get(topicIdPartition),
                        logReadResult.toFetchPartitionData(false)
                    ));
                }
            });
            completionConsumer.accept(shareFetchPartitionDataList);
        } catch (Exception e) {
            log.error("Error processing delayed share fetch request for group {}, member {}",
                shareFetch.groupId(), shareFetch.memberId(), e);
            handleFetchException(shareFetch, topicPartitionData.keySet(), e);
        } finally {
            // Always release locks and update action queue.
            releasePartitionLocksAndAddToActionQueue(
                topicPartitionData.keySet(),
                partitionsWithData(shareFetchPartitionDataList)
            );
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
     * @param readResponse - Map containing the readFromLog response for the topic partitions.
     */
    private void resetFetchOffsetMetadataForRemoteFetchPartitions(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> readResponse
    ) {
        readResponse.forEach((topicIdPartition, logReadResult) -> {
            if (logReadResult.info().delayedRemoteStorageFetch.isPresent()) {
                SharePartition sharePartition = sharePartitions.get(topicIdPartition);
                sharePartition.updateFetchOffsetMetadata(
                    topicPartitionData.get(topicIdPartition),
                    null
                );
            }
        });
    }

    @Override
    public boolean tryComplete() {
        // Check if there's a pending fetch from a previous tryComplete invocation.
        // This happens when the async read didn't complete immediately and we stored the future.
        // If the async read is still in progress, stay in purgatory. Otherwise, process the result.
        if (pendingFetch != null) {
            if (!pendingFetch.isDone()) {
                // Async read still in flight - partition locks are still held.
                return false;
            }
            // Async read completed - retrieve result and process
            return maybeCompleteAsyncFetch(pendingFetch, partitionsAcquired);
        }

        // Check to see if the remote fetch is in flight. If there is an in flight remote fetch we want to resolve it first.
        // Remote fetch can only happen after async fetch succeeds.
        if (pendingRemoteFetchesOpt.isPresent()) {
            return maybeCompletePendingRemoteFetch();
        }

        // Try to acquire partition locks for all partitions in the request
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData = acquirablePartitions(sharePartitions);
        try {
            if (!topicPartitionData.isEmpty()) {
                // Successfully acquired locks for one or more partitions
                // Update the metric to record the time taken to acquire the locks for the share partitions.
                updateAcquireElapsedTimeMetric();
                // In case, fetch offset metadata doesn't exist for one or more topic partitions, we do an
                // async read to populate the offset metadata and update the fetch offset metadata for
                // those topic partitions. This uses readAsync with readRemote=false.
                CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> readFuture = maybeReadFromLog(topicPartitionData);
                // Check if future completed synchronously or needs async handling
                if (readFuture.isDone()) {
                    // Fast path, async read completed synchronously.
                    // Process the result immediately without storing the future or registering callbacks
                    return maybeCompleteAsyncFetch(readFuture, topicPartitionData);
                } else {
                    // Store the future and register a callback. Partition locks remain held during the async operation.
                    // When the async read completes, the callback will trigger a purgatory retry which will
                    // invoke tryComplete() again, where we'll process the result.
                    partitionsAcquired = topicPartitionData;
                    pendingFetch = readFuture;
                    // Register callback to be invoked when async read completes. This callback executes on the
                    // thread that completes the future (IO thread pool). Hence, rather than processing the result
                    // directly here, trigger a purgatory check so that tryComplete() is invoked again for
                    // this operation under the operation lock (via safeTryComplete). Processing the async
                    // result inside tryComplete() ensures the shared state mutations (pendingFetch,
                    // partitionsAcquired, localPartitionsAlreadyFetched) happen under the lock and avoids a
                    // race with a concurrent tryComplete() invoked by a purgatory watcher thread.
                    // Also, the operation is watched on the group key for each topic partition in the request,
                    // hence triggering a check on any one of them will re-invoke tryComplete()
                    // for this operation. Just take the first in the list.
                    TopicIdPartition topicIdPartition = topicPartitionData.keySet().iterator().next();
                    readFuture.whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof CancellationException) {
                                // The completion handler execution has been cancelled due to the operation
                                // being completed in onComplete.
                                log.trace("Async read was cancelled for group {}, member {}",
                                    shareFetch.groupId(), shareFetch.memberId());
                                return;
                            }
                            log.error("Async read failed for group {}, member {}",
                                shareFetch.groupId(), shareFetch.memberId(), throwable);
                            // Error is handled in maybeCompleteAsyncFetch() via get() exception handling
                            // when tryComplete() re-processes the completed future.
                        }
                        replicaManager.completeDelayedShareFetchRequest(
                            new DelayedShareFetchGroupKey(shareFetch.groupId(), topicIdPartition));
                    });
                    // Return false to keep request in purgatory - locks remain held
                    return false;
                }
            } else {
                // Could not acquire locks for any partitions (all locked by other requests or at capacity)
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
            // Similarly, if we have a pending async fetch, don't release locks yet.
            if (remoteStorageFetchException.isEmpty() && pendingFetch == null) {
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

    private CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> maybeReadFromLog(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData
    ) {
        LinkedHashMap<TopicIdPartition, Long> partitionsNotMatchingFetchOffsetMetadata = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            if (sharePartition.fetchOffsetMetadata(fetchOffset).isEmpty()) {
                partitionsNotMatchingFetchOffsetMetadata.put(topicIdPartition, fetchOffset);
            }
        });
        if (partitionsNotMatchingFetchOffsetMetadata.isEmpty()) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }

        // We fetch data from log reader corresponding to the topic partitions that have missing fetch offset metadata.
        // Although we are fetching partition max bytes for partitionsNotMatchingFetchOffsetMetadata,
        // we will take acquired partitions size = topicPartitionData.size() because we do not want to let the
        // leftover partitions to starve which will be fetched later.
        // readRemote=false ensures to skip remote reads as they are dealt separately outside the log
        // reader, currently.
        return readFromLog(
            partitionsNotMatchingFetchOffsetMetadata,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes, partitionsNotMatchingFetchOffsetMetadata.keySet(), topicPartitionData.size()));
    }

    private void maybeUpdateFetchOffsetMetadata(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                                LinkedHashMap<TopicIdPartition, LogReadResult> readResponseData) {
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : readResponseData.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            SharePartition sharePartition = sharePartitions.get(topicIdPartition);
            LogReadResult logReadResult = entry.getValue();
            if (logReadResult.error().code() != Errors.NONE.code()) {
                log.debug("Log read result {} errored out for topic partition {}",
                    logReadResult, topicIdPartition);
                continue;
            }
            sharePartition.updateFetchOffsetMetadata(
                topicPartitionData.get(topicIdPartition),
                logReadResult.info().fetchOffsetMetadata);
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
        FetchIsolation isolationType = shareFetch.fetchParams().isolation;
        return metadataProvider.endOffsetMetadata(topicIdPartition, isolationType);
    }

    private CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> readFromLog(LinkedHashMap<TopicIdPartition, Long> topicPartitionFetchOffsets,
                                                                                          LinkedHashMap<TopicIdPartition, Integer> partitionMaxBytes) {
        // Filter if there already exists any erroneous topic partition.
        Set<TopicIdPartition> partitionsToFetch = shareFetch.filterErroneousTopicPartitions(topicPartitionFetchOffsets.keySet());
        return logReader.readAsync(shareFetch.fetchParams(), partitionsToFetch, topicPartitionFetchOffsets, partitionMaxBytes, false);
    }

    private boolean anyPartitionHasLogReadError(LinkedHashMap<TopicIdPartition, LogReadResult> readResponse) {
        return readResponse.values().stream()
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
     * Surfaces the fetch exception for the given partitions and releases their fetch locks.
     *
     * @param topicIdPartitions The topic-partitions whose fetch failed and whose locks must be released.
     * @param throwable The exception that occurred while fetching messages.
     */
    private void handleFetchExceptionAndReleaseLocks(Set<TopicIdPartition> topicIdPartitions, Throwable throwable) {
        try {
            handleFetchException(shareFetch, topicIdPartitions, throwable);
        } finally {
            releasePartitionLocks(topicIdPartitions);
        }
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

    /**
     * Process the result of a completed async read future. The caller must ensure the future is
     * already completed (i.e. {@code completedFuture.isDone()} is true) before invoking this method,
     * as {@link CompletableFuture#get()} is called without any additional wait.
     */
    private boolean maybeCompleteAsyncFetch(
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> completedFuture,
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData
    ) {
        LinkedHashMap<TopicIdPartition, LogReadResult> readResponse;
        try {
            // Retrieve the result without blocking (safe because the caller ensured isDone() was true).
            // get() returns the result immediately since the future is complete, or throws
            // ExecutionException if the async operation failed.
            readResponse = completedFuture.get();
            // Remove pending fetch as the request is completed now.
            pendingFetch = null;
        } catch (Exception e) {
            // On the slow path pendingFetch is set, so forceComplete() -> onComplete() re-enters
            // completeShareFetchAsyncRequest(), which retrieves the data again (failing the same way),
            // surfaces the error and releases the partition locks held under partitionsAcquired.
            //
            // On the fast path (invoked directly from tryComplete() when the read completed
            // synchronously) set the partitionsAcquired so onComplete() can release the held partitions.
            if (pendingFetch == null) {
                log.error("Error retrieving async fetch result for group {}, member {}",
                    shareFetch.groupId(), shareFetch.memberId(), e);
                partitionsAcquired = topicPartitionData;
            }
            return forceComplete();
        }
        // Process the successful read response - check for remote fetch, minBytes, etc.
        return processAsyncReadResponse(topicPartitionData, readResponse);
    }

    private boolean processAsyncReadResponse(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> readResponse
    ) {
        // Check if any partitions need remote storage fetch
        // Store the remote fetch info for the topic partitions for which we need to perform remote fetch.
        // Remote fetch can only happen after local fetch completes successfully.
        LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap =
            maybePrepareRemoteStorageFetchInfo(topicPartitionData, readResponse);

        if (!remoteStorageFetchInfoMap.isEmpty()) {
            // Some data is in remote storage - initiate remote fetch
            // This will transition to remote fetch state (pendingRemoteFetchesOpt)
            return maybeProcessRemoteFetch(topicPartitionData, remoteStorageFetchInfoMap);
        }

        // All data is fetched - update cached metadata for future requests
        maybeUpdateFetchOffsetMetadata(topicPartitionData, readResponse);

        // Check if we can complete the request. If any partition has a log read error, we can complete
        // the request with an error response.
        if (anyPartitionHasLogReadError(readResponse) || isMinBytesSatisfied(topicPartitionData,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes,
                topicPartitionData.keySet(), topicPartitionData.size()))) {
            // Request can be completed - set state and force completion. Though on-slow path when
            // readFuture is not synchronously completed the partitionsAcquired will be already set
            // but on fast-path when readFuture is completed synchronously, we need to set the
            // partitionsAcquired.
            partitionsAcquired = topicPartitionData;
            localPartitionsAlreadyFetched = readResponse;
            return forceComplete();
        } else {
            // minBytes not satisfied - release locks and return to purgatory
            // Request will be retried when more data arrives or timeout occurs
            log.debug("minBytes is not satisfied for the share fetch request for group {}, member {}, " +
                    "topic partitions {}", shareFetch.groupId(), shareFetch.memberId(),
                sharePartitions.keySet());
            releasePartitionLocks(topicPartitionData.keySet());
            // Clear the per-request state so that a subsequent tryComplete()/onComplete() re-acquires the
            // partitions afresh. In the pending-async path topicPartitionData is partitionsAcquired, so
            // leaving it populated would make onComplete() assume the (already released) locks are still
            // held and process/release them again.
            partitionsAcquired.clear();
            localPartitionsAlreadyFetched.clear();
            return false;
        }
    }

    private void recordTopicPartitionsFetchRatioMetric(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        // Update metric to record acquired to requested partitions.
        double requestTopicToAcquired = (double) topicPartitionData.size() / shareFetch.topicIdPartitions().size();
        shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), (int) (requestTopicToAcquired * 100));
    }

    private void completeAsyncRequestWithEmptyResponse(LinkedHashMap<TopicIdPartition, Long> topicPartitionData) {
        try {
            log.warn("Completing async fetch for group {}, member {}, partitions {} with empty response",
                shareFetch.groupId(), shareFetch.memberId(), topicPartitionData.keySet());
            // Cancel the future invocation of pending fetch.
            pendingFetch.cancel(true);
            pendingFetch = null;
            // Update fetch ratio metric and complete the request with empty response.
            recordTopicPartitionsFetchRatioMetric(topicPartitionData);
            shareFetch.maybeComplete(Map.of());
        } finally {
            // Release locks for all partitions acquired for this request. The async fetch did not
            // complete, so no partition has data, hence pass an empty set as the partitions with data.
            releasePartitionLocks(topicPartitionData.keySet());
        }
    }

    // Visible for testing.
    CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> combineLogReadResponse(LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
                                                                                             LinkedHashMap<TopicIdPartition, LogReadResult> existingFetchedData) {
        LinkedHashMap<TopicIdPartition, Long> missingLogReadTopicPartitions = new LinkedHashMap<>();
        topicPartitionData.forEach((topicIdPartition, fetchOffset) -> {
            if (!existingFetchedData.containsKey(topicIdPartition)) {
                missingLogReadTopicPartitions.put(topicIdPartition, fetchOffset);
            }
        });
        if (missingLogReadTopicPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(existingFetchedData);
        }

        return readFromLog(
            missingLogReadTopicPartitions,
            partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes,
                missingLogReadTopicPartitions.keySet(), topicPartitionData.size())
        ).thenApply(missingTopicPartitionsLogReadResponse -> {
            missingTopicPartitionsLogReadResponse.putAll(existingFetchedData);
            return missingTopicPartitionsLogReadResponse;
        });
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

    // Visible for testing.
    CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> pendingFetch() {
        return pendingFetch;
    }

    private LinkedHashMap<TopicIdPartition, LogReadResult> maybePrepareRemoteStorageFetchInfo(
        LinkedHashMap<TopicIdPartition, Long> topicPartitionData,
        LinkedHashMap<TopicIdPartition, LogReadResult> readResponse
    ) {
        LinkedHashMap<TopicIdPartition, LogReadResult> remoteStorageFetchInfoMap = new LinkedHashMap<>();
        for (Map.Entry<TopicIdPartition, LogReadResult> entry : readResponse.entrySet()) {
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
        releasePartitionLocksAndAddToActionQueue(nonRemoteFetchTopicPartitions, nonRemoteFetchTopicPartitions);
        // Remove the just-released non-remote partitions from partitionsAcquired so it tracks only the
        // remote-fetch partitions whose locks are still held. On the slow path partitionsAcquired was
        // populated in tryComplete with ALL acquired partitions (remote and non-remote); without this
        // removal the remote completion path would later release these already-released locks a second
        // time (releaseFetchLock force-clears the lock, so this can steal a lock re-acquired by another
        // request) and would skip the additional local read for these partitions (they would still look
        // acquired). On the fast path partitionsAcquired only holds the remote partitions, so this is a
        // no-op there.
        nonRemoteFetchTopicPartitions.forEach(partitionsAcquired::remove);
        processRemoteFetchOrException(remoteStorageFetchInfoMap);
        // Check if remote fetch can be completed.
        return maybeCompletePendingRemoteFetch();
    }

    private boolean maybeRegisterCallbackPendingRemoteFetch() {
        log.trace("Registering callback pending remote fetch");
        PendingRemoteFetches pendingRemoteFetches = pendingRemoteFetchesOpt.get();
        if (!pendingRemoteFetches.isDone() && shareFetch.fetchParams().maxWaitMs < remoteFetchMaxWaitMs) {
            TimerTask timerTask = new PendingRemoteFetchTimerTask();
            pendingRemoteFetches.invokeCallbackOnCompletion(((ignored, throwable) -> {
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
            releasePartitionLocksAndAddToActionQueue(partitionsAcquired.keySet(), partitionsAcquired.keySet());
        }

    }

    private Set<TopicIdPartition> partitionsWithData(List<ShareFetchPartitionData> shareFetchPartitionDataList) {
        if (shareFetchPartitionDataList == null || shareFetchPartitionDataList.isEmpty()) {
            return Set.of();
        }
        Set<TopicIdPartition> partitionsWithData = new HashSet<>();
        shareFetchPartitionDataList.forEach(shareFetchPartitionData -> {
            if (shareFetchPartitionData.fetchPartitionData() != null &&
                shareFetchPartitionData.fetchPartitionData().records != null &&
                shareFetchPartitionData.fetchPartitionData().records.sizeInBytes() > 0) {
                partitionsWithData.add(shareFetchPartitionData.topicIdPartition());
            }
        });
        return partitionsWithData;
    }

    private void releasePartitionLocksAndAddToActionQueue(Set<TopicIdPartition> allAcquiredTopicIdPartitions,
                                                          Set<TopicIdPartition> topicIdPartitionsWithData) {
        if (allAcquiredTopicIdPartitions.isEmpty()) {
            // topicIdPartitionsWithData set should be a subset of allAcquiredTopicIdPartitions, hence it is safe to return.
            return;
        }
        // Releasing the lock to move ahead with the next request in queue.
        releasePartitionLocks(allAcquiredTopicIdPartitions);
        if (topicIdPartitionsWithData.isEmpty()) {
            return;
        }
        replicaManager.addToActionQueue(() -> topicIdPartitionsWithData.forEach(topicIdPartition -> {
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
        List<ShareFetchPartitionData> shareFetchPartitionDataList = new ArrayList<>();
        try {
            int readableBytes = 0;
            for (RemoteFetch remoteFetch : pendingRemoteFetchesOpt.get().remoteFetches()) {
                if (remoteFetch.remoteFetchResult().isDone()) {
                    RemoteLogReadResult remoteLogReadResult = remoteFetch.remoteFetchResult().get();
                    if (remoteLogReadResult.error().isPresent()) {
                        // If there is any error for the remote fetch topic partition, we populate the error accordingly.
                        shareFetchPartitionDataList.add(
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
                        shareFetchPartitionDataList.add(
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

            // If remote fetch bytes < shareFetch.fetchParams().maxBytes, then we will try for a local read.
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
                    LinkedHashMap<TopicIdPartition, Long> partitionsToFetch = acquiredNonRemoteFetchTopicPartitionData;
                    // Register for further read and request completion.
                    readAndProcessFetchResultCompletion(
                        acquiredNonRemoteFetchTopicPartitionData,
                        readableBytes,
                        addedShareFetchPartitionDataList -> {
                            shareFetchPartitionDataList.addAll(addedShareFetchPartitionDataList);
                            completeRemoteFetchRequest(shareFetchPartitionDataList, partitionsToFetch);
                        });
                    return;
                }
            }

            // Complete with remote data (and any local data if no additional async read was needed)
            completeRemoteFetchRequest(shareFetchPartitionDataList, acquiredNonRemoteFetchTopicPartitionData);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception occurred in completing remote fetch {} for delayed share fetch request {}", pendingRemoteFetchesOpt.get(), e);
            Set<TopicIdPartition> topicIdPartitions = new LinkedHashSet<>(partitionsAcquired.keySet());
            topicIdPartitions.addAll(acquiredNonRemoteFetchTopicPartitionData.keySet());
            handleFetchExceptionAndReleaseLocks(topicIdPartitions, e);
        } catch (Exception e) {
            log.error("Unexpected error in processing delayed share fetch request", e);
            Set<TopicIdPartition> topicIdPartitions = new LinkedHashSet<>(partitionsAcquired.keySet());
            topicIdPartitions.addAll(acquiredNonRemoteFetchTopicPartitionData.keySet());
            handleFetchExceptionAndReleaseLocks(topicIdPartitions, e);
        }
    }

    private void completeRemoteFetchRequest(
        List<ShareFetchPartitionData> shareFetchPartitionDataList,
        LinkedHashMap<TopicIdPartition, Long> acquiredNonRemoteFetchTopicPartitionData
    ) {
        try {
            // Update metric to record acquired to requested partitions.
            double acquiredRatio = (double) (partitionsAcquired.size() + acquiredNonRemoteFetchTopicPartitionData.size()) / shareFetch.topicIdPartitions().size();
            if (acquiredRatio > 0)
                shareGroupMetrics.recordTopicPartitionsFetchRatio(shareFetch.groupId(), (int) (acquiredRatio * 100));

            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> remoteFetchResponse = ShareFetchUtils.processFetchResponse(
                shareFetch, shareFetchPartitionDataList, sharePartitions, metadataProvider, exceptionHandler);
            shareFetch.maybeComplete(remoteFetchResponse);
            log.trace("Remote share fetch request completed successfully, response: {}", remoteFetchResponse);
        } catch (Exception e) {
            log.error("Unexpected error in processing delayed share fetch request", e);
            handleExceptionInCompletingRemoteStorageShareFetchRequest(acquiredNonRemoteFetchTopicPartitionData.keySet(), e);
        } finally {
            // The locks for acquiredNonRemoteFetchTopicPartitionData are released by
            // processFetchResultAndComplete (invoked via readAndProcessFetchResultCompletion). As the
            // locks are release in finally block in readAndProcessFetchResultCompletion hence share
            // fetch will be completed in the current try block prior locks getting released. Hence,
            // need to release the locks for the remote fetch partitions here.
            //
            // shareFetchPartitionDataList can also contain the local partitions read alongside the remote
            // fetch (added via the readAndProcessFetchResultCompletion callback), whose action queue
            // entries were already handled by processFetchResultAndComplete.
            Set<TopicIdPartition> remotePartitionsWithData = new HashSet<>(partitionsWithData(shareFetchPartitionDataList));
            remotePartitionsWithData.retainAll(partitionsAcquired.keySet());
            releasePartitionLocksAndAddToActionQueue(partitionsAcquired.keySet(), remotePartitionsWithData);
        }
    }

    private void readAndProcessFetchResultCompletion(
        LinkedHashMap<TopicIdPartition, Long> partitionsToFetch,
        int readBytes,
        Consumer<List<ShareFetchPartitionData>> completionConsumer
    ) {
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> readFuture;
        // Start async read for additional local partitions
        try {
            readFuture = readFromLog(
                partitionsToFetch,
                partitionMaxBytesStrategy.maxBytes(shareFetch.fetchParams().maxBytes - readBytes,
                    partitionsToFetch.keySet(),
                    partitionsToFetch.size())
            );
        } catch (Exception e) {
            // Handle synchronous exception from readFromLog()
            log.error("Error initiating async fetch in remote completion for group {}, member {}",
                shareFetch.groupId(), shareFetch.memberId(), e);
            // processFetchResultAndComplete won't be invoked in this error path, so it won't release the
            // locks for partitionsToFetch. Release them here to avoid leaking the local partition locks
            // before completing with just the remote data.
            releasePartitionLocks(partitionsToFetch.keySet());
            // Continue to complete with just remote data.
            completionConsumer.accept(List.of());
            return;
        }

        // Handle async read completion. This async completion is registered only from on-complete
        // hence multiple threads cannot execute at same time. It's safe to register further processing
        // in completion handler.
        readFuture.whenComplete((responseData, throwable) -> {
            if (throwable != null) {
                log.error("Async fetch failed in remote completion for group {}, member {}",
                    shareFetch.groupId(), shareFetch.memberId(), throwable);
                // processFetchResultAndComplete won't be invoked in this error path, so it won't release
                // the locks for partitionsToFetch. Release them here to avoid leaking the local partition
                // locks before completing with just the remote data.
                releasePartitionLocks(partitionsToFetch.keySet());
                // Continue to complete with just remote data.
                completionConsumer.accept(List.of());
                return;
            }
            processFetchResultAndComplete(responseData, partitionsToFetch, completionConsumer);
        });
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
