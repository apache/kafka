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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchPartitionStatus;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A remote fetch operation that can be created by the replica manager and watched
 * in the remote fetch operation purgatory
 */
public class DelayedRemoteFetch extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedRemoteFetch.class);

    // For compatibility, metrics are defined to be under `kafka.server.DelayedRemoteFetchMetrics` class
    private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup("kafka.server", "DelayedRemoteFetchMetrics");

    private static final Meter EXPIRED_REQUEST_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);

    private final Map<TopicIdPartition, Future<Void>> remoteFetchTasks;
    private final Map<TopicIdPartition, CompletableFuture<RemoteLogReadResult>> remoteFetchResults;
    private final Map<TopicIdPartition, RemoteStorageFetchInfo> remoteFetchInfos;
    private final Map<TopicIdPartition, FetchPartitionStatus> fetchPartitionStatus;
    private final FetchParams fetchParams;
    private final Map<TopicIdPartition, LogReadResult> localReadResults;
    private final Consumer<TopicPartition> partitionOrException;
    private final Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback;

    public DelayedRemoteFetch(Map<TopicIdPartition, Future<Void>> remoteFetchTasks,
                              Map<TopicIdPartition, CompletableFuture<RemoteLogReadResult>> remoteFetchResults,
                              Map<TopicIdPartition, RemoteStorageFetchInfo> remoteFetchInfos,
                              long remoteFetchMaxWaitMs,
                              Map<TopicIdPartition, FetchPartitionStatus> fetchPartitionStatus,
                              FetchParams fetchParams,
                              Map<TopicIdPartition, LogReadResult> localReadResults,
                              Consumer<TopicPartition> partitionOrException,
                              Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback) {
        super(remoteFetchMaxWaitMs);
        this.remoteFetchTasks = remoteFetchTasks;
        this.remoteFetchResults = remoteFetchResults;
        this.remoteFetchInfos = remoteFetchInfos;
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
     * <p>
     * Case a: This broker is no longer the leader of the partition it tries to fetch
     * <p>
     * Case b: This broker does not know the partition it tries to fetch
     * <p>
     * Case c: All the remote storage read requests completed (succeeded or failed)
     * <p>
     * Case d: The partition is in an offline log directory on this broker
     *
     * Upon completion, should return whatever data is available for each valid partition
     */
    @Override
    public boolean tryComplete() {
        for (Map.Entry<TopicIdPartition, FetchPartitionStatus> entry : fetchPartitionStatus.entrySet()) {
            TopicIdPartition topicPartition = entry.getKey();
            FetchPartitionStatus fetchStatus = entry.getValue();
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

        // Case c
        if (remoteFetchResults.values().stream().allMatch(CompletableFuture::isDone)) {
            return forceComplete();
        }
        return false;
    }

    @Override
    public void onExpiration() {
        // cancel the remote storage read task, if it has not been executed yet and
        // avoid interrupting the task if it is already running as it may force closing opened/cached resources as transaction index.
        remoteFetchTasks.forEach((topicIdPartition, task) -> {
            if (task != null && !task.isDone() && !task.cancel(false)) {
                LOG.debug("Remote fetch task for remoteFetchInfo: {} could not be cancelled.", remoteFetchInfos.get(topicIdPartition));
            }
        });

        EXPIRED_REQUEST_METER.mark();
    }

    /**
     * Upon completion, read whatever data is available and pass to the complete callback
     */
    @Override
    public void onComplete() {
        Map<TopicIdPartition, FetchPartitionData> fetchPartitionData = new LinkedHashMap<>();
        localReadResults.forEach((tpId, result) -> {
            CompletableFuture<RemoteLogReadResult> remoteFetchResult = remoteFetchResults.get(tpId);
            if (remoteFetchResults.containsKey(tpId)
                && remoteFetchResult.isDone()
                && result.error() == Errors.NONE
                && result.info().delayedRemoteStorageFetch.isPresent()) {

                if (remoteFetchResult.join().error().isPresent()) {
                    fetchPartitionData.put(tpId,
                        new LogReadResult(Errors.forException(remoteFetchResult.join().error().get())).toFetchPartitionData(false));
                } else {
                    FetchDataInfo info = remoteFetchResult.join().fetchDataInfo().get();
                    fetchPartitionData.put(tpId,
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
                fetchPartitionData.put(tpId, result.toFetchPartitionData(false));
            }
        });

        responseCallback.accept(fetchPartitionData);
    }

    // Visible for testing
    public static long expiredRequestCount() {
        return EXPIRED_REQUEST_METER.count();
    }
}
