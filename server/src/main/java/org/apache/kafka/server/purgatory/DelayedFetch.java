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
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset;
import org.apache.kafka.common.metrics.internals.MetricsUtils;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.server.log.remote.TopicPartitionLog;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.quota.ReplicaQuota;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchPartitionStatus;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;
import org.apache.kafka.storage.internals.log.LogReadResult;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory.
 */
public class DelayedFetch extends DelayedOperation {
    private static final Logger LOG = LoggerFactory.getLogger(DelayedFetch.class);

    // Changing the package or class name may cause incompatibility with existing code and metrics configuration.
    private static final KafkaMetricsGroup METRICS_GROUP =
        new KafkaMetricsGroup("kafka.server", "DelayedFetchMetrics");
    private static final String FETCHER_TYPE_KEY = "fetcherType";
    private static final Meter FOLLOWER_EXPIRED_REQUEST_METER = METRICS_GROUP.newMeter(
        "ExpiresPerSec",
        "requests",
        TimeUnit.SECONDS,
        MetricsUtils.getTags(FETCHER_TYPE_KEY, "follower")
    );
    private static final Meter CONSUMER_EXPIRED_REQUEST_METER = METRICS_GROUP.newMeter(
        "ExpiresPerSec",
        "requests",
        TimeUnit.SECONDS,
        MetricsUtils.getTags(FETCHER_TYPE_KEY, "consumer")
    );

    private final FetchParams params;
    private final LinkedHashMap<TopicIdPartition, FetchPartitionStatus> fetchPartitionStatus;
    private final ReplicaManagerAdapter replicaManager;
    private final ReplicaQuota quota;
    private final Consumer<LinkedHashMap<TopicIdPartition, FetchPartitionData>> responseCallback;

    public DelayedFetch(FetchParams params,
                        LinkedHashMap<TopicIdPartition, FetchPartitionStatus> fetchPartitionStatus,
                        ReplicaManagerAdapter replicaManager,
                        ReplicaQuota quota,
                        Consumer<LinkedHashMap<TopicIdPartition, FetchPartitionData>> responseCallback) {
        super(params.maxWaitMs);
        this.params = params;
        this.fetchPartitionStatus = fetchPartitionStatus;
        this.replicaManager = replicaManager;
        this.quota = quota;
        this.responseCallback = responseCallback;
    }

    @Override
    public String toString() {
        return "DelayedFetch(params=" + params +
            ", numPartitions=" + fetchPartitionStatus.size() +
            ")";
    }

    /**
     * The operation can be completed if:
     *
     * Case A: This broker is no longer the leader for some partitions it tries to fetch
     * Case B: The replica is no longer available on this broker
     * Case C: This broker does not know of some partitions it tries to fetch
     * Case D: The partition is in an offline log directory on this broker
     * Case E: This broker is the leader, but the requested epoch is now fenced
     * Case F: The fetch offset locates not on the last segment of the log
     * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
     * Case H: A diverging epoch was found, return response to trigger truncation
     * Upon completion, should return whatever data is available for each valid partition.
     */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    @Override
    public boolean tryComplete() {
        int accumulatedSize = 0;
        for (Map.Entry<TopicIdPartition, FetchPartitionStatus> entry : fetchPartitionStatus.entrySet()) {
            TopicIdPartition topicIdPartition = entry.getKey();
            FetchPartitionStatus fetchStatus = entry.getValue();
            LogOffsetMetadata fetchOffset = fetchStatus.startOffsetMetadata();
            Optional<Integer> fetchLeaderEpoch = fetchStatus.fetchInfo().currentLeaderEpoch;

            try {
                if (!fetchOffset.equals(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA)) {
                    TopicPartitionLog partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition());
                    LogOffsetSnapshot offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, params.fetchOnlyLeader());

                    LogOffsetMetadata endOffset = switch (params.isolation) {
                        case LOG_END -> offsetSnapshot.logEndOffset();
                        case HIGH_WATERMARK -> offsetSnapshot.highWatermark();
                        case TXN_COMMITTED -> offsetSnapshot.lastStableOffset();
                    };

                    // Go directly to the check for Case G if the message offsets are the same. If the log segment
                    // has just rolled, then the high watermark offset will remain the same but be on the old segment,
                    // which would incorrectly be seen as an instance of Case F.
                    if (fetchOffset.messageOffset > endOffset.messageOffset) {
                        // Case F, this can happen when the new fetch operation is on a truncated leader.
                        LOG.debug("Satisfying fetch {} since it is fetching later segments of partition {}.", this, topicIdPartition);
                        return forceComplete();
                    } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                        if (fetchOffset.onOlderSegment(endOffset)) {
                            // Case F, this can happen when the fetch operation is falling behind the current segment
                            // or the partition has just rolled a new segment.
                            LOG.debug("Satisfying fetch {} immediately since it is fetching older segments.", this);
                            // We will not force complete the fetch request if a replica should be throttled.
                            if (!params.isFromFollower() || !replicaManager.shouldLeaderThrottle(quota, partition, params.replicaId)) {
                                return forceComplete();
                            }
                        } else if (fetchOffset.onSameSegment(endOffset)) {
                            // We take the partition fetch size as upper bound when accumulating the bytes
                            // (skip if a throttled partition).
                            int bytesAvailable = Math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo().maxBytes);
                            if (!params.isFromFollower() || !replicaManager.shouldLeaderThrottle(quota, partition, params.replicaId)) {
                                accumulatedSize += bytesAvailable;
                            }
                        }
                    }

                    // Case H: If truncation has caused diverging epoch while this request was in purgatory,
                    // return to trigger truncation.
                    Optional<Integer> lastFetchedEpoch = fetchStatus.fetchInfo().lastFetchedEpoch;
                    if (lastFetchedEpoch.isPresent()) {
                        int fetchEpoch = lastFetchedEpoch.get();
                        EpochEndOffset epochEndOffset = partition.lastOffsetForLeaderEpoch(
                            fetchLeaderEpoch,
                            fetchEpoch,
                            false
                        );
                        if (epochEndOffset.errorCode() != Errors.NONE.code()
                                || epochEndOffset.endOffset() == UNDEFINED_EPOCH_OFFSET
                                || epochEndOffset.leaderEpoch() == UNDEFINED_EPOCH) {
                            LOG.debug(
                                "Could not obtain last offset for leader epoch for partition {}, epochEndOffset={}.",
                                topicIdPartition,
                                epochEndOffset
                            );
                            return forceComplete();
                        } else if (epochEndOffset.leaderEpoch() < fetchEpoch
                                || epochEndOffset.endOffset() < fetchStatus.fetchInfo().fetchOffset) {
                            LOG.debug(
                                "Satisfying fetch {} since it has diverging epoch requiring truncation for partition " +
                                    "{} epochEndOffset={} fetchEpoch={} fetchOffset={}.",
                                this,
                                topicIdPartition,
                                epochEndOffset,
                                fetchEpoch,
                                fetchStatus.fetchInfo().fetchOffset
                            );
                            return forceComplete();
                        }
                    }
                }
            } catch (NotLeaderOrFollowerException e) { // Case A or Case B
                LOG.debug("Broker is no longer the leader or follower of {}, satisfy {} immediately", topicIdPartition, this);
                return forceComplete();
            } catch (UnknownTopicOrPartitionException e) { // Case C
                LOG.debug("Broker no longer knows of partition {}, satisfy {} immediately", topicIdPartition, this);
                return forceComplete();
            } catch (KafkaStorageException e) { // Case D
                LOG.debug("Partition {} is in an offline log directory, satisfy {} immediately", topicIdPartition, this);
                return forceComplete();
            } catch (FencedLeaderEpochException e) { // Case E
                LOG.debug(
                    "Broker is the leader of partition {}, but the requested epoch {} is fenced by the latest leader epoch, " +
                        "satisfy {} immediately",
                    topicIdPartition,
                    fetchLeaderEpoch,
                    this
                );
                return forceComplete();
            }
        }

        // Case G
        if (accumulatedSize >= params.minBytes) {
            return forceComplete();
        }
        return false;
    }

    @Override
    public void onExpiration() {
        if (params.isFromFollower()) {
            FOLLOWER_EXPIRED_REQUEST_METER.mark();
        } else {
            CONSUMER_EXPIRED_REQUEST_METER.mark();
        }
    }

    /**
     * Upon completion, read whatever data is available and pass to the complete callback.
     */
    @Override
    public void onComplete() {
        LinkedHashMap<TopicIdPartition, PartitionData> fetchInfos = new LinkedHashMap<>();
        fetchPartitionStatus.forEach((topicIdPartition, status) -> fetchInfos.put(topicIdPartition, status.fetchInfo()));

        LinkedHashMap<TopicIdPartition, LogReadResult> logReadResults = replicaManager.readFromLogByPurgatory(
            params,
            fetchInfos,
            quota
        );

        LinkedHashMap<TopicIdPartition, FetchPartitionData> fetchPartitionData = new LinkedHashMap<>();
        logReadResults.forEach((topicIdPartition, result) -> {
            boolean isReassignmentFetch = params.isFromFollower()
                && replicaManager.isAddingReplica(topicIdPartition.topicPartition(), params.replicaId);
            fetchPartitionData.put(topicIdPartition, result.toFetchPartitionData(isReassignmentFetch));
        });

        responseCallback.accept(fetchPartitionData);
    }

}
