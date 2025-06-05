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

package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.FetchPartitionStatus;
import org.apache.kafka.server.LogReadResult;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A remote fetch operation that can be created by the replica manager and watched
 * in the remote fetch operation purgatory
 */
public class DelayedRemoteFetch extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedRemoteFetch.class);

    private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup("kafka.server", "DelayedRemoteFetchMetrics");

    static final Meter EXPIRED_REQUEST_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);

    private final Future<Void> remoteFetchTask;
    private final CompletableFuture<RemoteLogReadResult> remoteFetchResult;
    private final RemoteStorageFetchInfo remoteFetchInfo;
    private final Map<TopicIdPartition, List<FetchPartitionStatus>> fetchPartitionStatus;
    private final FetchParams fetchParams;
    private final Map<TopicIdPartition, List<LogReadResult>> localReadResults;
    private final Consumer<TopicPartition> partitionOrException;
    private final Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> responseCallback;

    public DelayedRemoteFetch(Future<Void> remoteFetchTask,
                              CompletableFuture<RemoteLogReadResult> remoteFetchResult,
                              RemoteStorageFetchInfo remoteFetchInfo,
                              long remoteFetchMaxWaitMs,
                              Map<TopicIdPartition, List<FetchPartitionStatus>> fetchPartitionStatus,
                              FetchParams fetchParams,
                              Map<TopicIdPartition, List<LogReadResult>> localReadResults,
                              Consumer<TopicPartition> partitionOrException,
                              Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> responseCallback) {
        super(remoteFetchMaxWaitMs);
        this.remoteFetchTask = remoteFetchTask;
        this.remoteFetchResult = remoteFetchResult;
        this.remoteFetchInfo = remoteFetchInfo;
        this.fetchPartitionStatus = fetchPartitionStatus;
        this.fetchParams = fetchParams;
        this.localReadResults = localReadResults;
        this.partitionOrException = partitionOrException;
        this.responseCallback = responseCallback;

        if (fetchParams.isFromFollower()) {
            throw new IllegalStateException("The follower should not invoke remote fetch. Fetch params are: " + fetchParams);
        }
    }

    /**
     * The operation can be completed if:
     *
     * Case a: This broker is no longer the leader of the partition it tries to fetch
     * Case b: This broker does not know the partition it tries to fetch
     * Case c: The remote storage read request completed (succeeded or failed)
     * Case d: The partition is in an offline log directory on this broker
     *
     * Upon completion, should return whatever data is available for each valid partition
     */
    @Override
    public boolean tryComplete() {
        for (Map.Entry<TopicIdPartition, List<FetchPartitionStatus>> entry : fetchPartitionStatus.entrySet()) {
            TopicIdPartition topicPartition = entry.getKey();
            List<FetchPartitionStatus> fetchStatusList = entry.getValue();
            for (FetchPartitionStatus fetchStatus : fetchStatusList) {
                LogOffsetMetadata fetchOffset = fetchStatus.startOffsetMetadata();
                try {
                    if (!fetchOffset.equals(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)) {
                        partitionOrException.accept(topicPartition.topicPartition());
                    }
                } catch (KafkaStorageException e) { // Case d
                    LOG.debug("Partition {} is in an offline log directory, satisfy {} immediately.", topicPartition, fetchParams);
                    return forceComplete();
                } catch (UnknownTopicOrPartitionException e) { // Case b
                    LOG.debug("Broker no longer knows of partition {}, satisfy {} immediately", topicPartition, fetchParams);
                    return forceComplete();
                } catch (NotLeaderOrFollowerException e) { // Case a
                    LOG.debug("Broker is no longer the leader or follower of {}, satisfy {} immediately", topicPartition, fetchParams);
                    return forceComplete();
                }
            }
        }

        if (remoteFetchResult.isDone()) { // Case c
            return forceComplete();
        }
        return false;
    }

    @Override
    public void onExpiration() {
        // cancel the remote storage read task, if it has not been executed yet and
        // avoid interrupting the task if it is already running as it may force closing opened/cached resources as transaction index.
        boolean cancelled = remoteFetchTask.cancel(false);
        if (!cancelled) {
            LOG.debug("Remote fetch task for RemoteStorageFetchInfo: {} could not be cancelled and its isDone value is {}.", remoteFetchInfo, remoteFetchTask.isDone());
        }

        EXPIRED_REQUEST_METER.mark();
    }

    /**
     * Upon completion, read whatever data is available and pass to the complete callback
     */
    @Override
    public void onComplete() {
        Map<TopicIdPartition, List<FetchPartitionData>> fetchPartitionData = new HashMap<>();

        try {
            for (Map.Entry<TopicIdPartition, List<LogReadResult>> entry : localReadResults.entrySet()) {
                TopicIdPartition topicIdPartition = entry.getKey();
                List<LogReadResult> results = entry.getValue();
                List<FetchPartitionData> partitionDataList = fetchPartitionData.computeIfAbsent(topicIdPartition,
                    k -> new ArrayList<>());

                for (LogReadResult result : results) {
                    if (topicIdPartition.topicPartition().equals(remoteFetchInfo.topicPartition)
                        && remoteFetchResult.isDone() && result.error() == Errors.NONE
                        && result.info().delayedRemoteStorageFetch.isPresent()) {

                        if (remoteFetchResult.get().error.isPresent()) {
                            partitionDataList.add(
                                new LogReadResult(remoteFetchResult.get().error.get()).toFetchPartitionData(false));
                        } else {
                            FetchDataInfo info = remoteFetchResult.get().fetchDataInfo.get();
                            partitionDataList.add(
                                new FetchPartitionData(
                                    result.error(),
                                    result.highWatermark(),
                                    result.leaderLogStartOffset(),
                                    info.records,
                                    Optional.empty(),
                                    result.lastStableOffset(),
                                    info.abortedTransactions,
                                    result.preferredReadReplica(),
                                    false));
                        }
                    } else {
                        partitionDataList.add(result.toFetchPartitionData(false));
                    }
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        responseCallback.accept(fetchPartitionData);
    }

    // Visible for testing
    public static Meter expiredRequestMeter() {
        return EXPIRED_REQUEST_METER;
    }
}