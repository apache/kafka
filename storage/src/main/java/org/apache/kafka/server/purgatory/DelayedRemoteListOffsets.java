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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class DelayedRemoteListOffsets extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedRemoteListOffsets.class);

    // For compatibility, metrics are defined to be under `kafka.server.DelayedRemoteListOffsetsMetrics` class
    private static final KafkaMetricsGroup METRICS_GROUP = new KafkaMetricsGroup("kafka.server", "DelayedRemoteListOffsetsMetrics");
    static final Meter AGGREGATE_EXPIRATION_METER = METRICS_GROUP.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);
    static final Map<TopicPartition, Meter> PARTITION_EXPIRATION_METERS = new ConcurrentHashMap<>();

    private final int version;
    private final Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition;
    private final Consumer<TopicPartition> partitionOrException;
    private final Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback;

    public DelayedRemoteListOffsets(long delayMs,
                                    int version,
                                    Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition,
                                    Consumer<TopicPartition> partitionOrException,
                                    Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback) {
        super(delayMs);
        this.version = version;
        this.statusByPartition = statusByPartition;
        this.partitionOrException = partitionOrException;
        this.responseCallback = responseCallback;
        // Mark the status as completed, if there is no async task to track.
        // If there is a task to track, then build the response as REQUEST_TIMED_OUT by default.
        statusByPartition.forEach((topicPartition, status) -> {
            status.completed(status.futureHolderOpt().isEmpty());
            if (status.futureHolderOpt().isPresent()) {
                status.responseOpt(Optional.of(buildErrorResponse(Errors.REQUEST_TIMED_OUT, topicPartition.partition())));
            }
            LOG.trace("Initial partition status for {} is {}", topicPartition, status);
        });
    }

    /**
     * Call-back to execute when a delayed operation gets expired and hence forced to complete.
     */
    @Override
    public void onExpiration() {
        statusByPartition.forEach((topicPartition, status) -> {
            if (!status.completed()) {
                LOG.debug("Expiring list offset request for partition {} with status {}", topicPartition, status);
                status.futureHolderOpt().ifPresent(futureHolder -> futureHolder.jobFuture().cancel(true));
                recordExpiration(topicPartition);
            }
        });
    }

    /**
     * Process for completing an operation; This function needs to be defined
     * in subclasses and will be called exactly once in forceComplete()
     */
    @Override
    public void onComplete() {
        Map<String, ListOffsetsResponseData.ListOffsetsTopicResponse> groupedByTopic = new HashMap<>();
        statusByPartition.forEach((tp, status) -> {
            ListOffsetsResponseData.ListOffsetsTopicResponse response = groupedByTopic.computeIfAbsent(tp.topic(), k ->
                    new ListOffsetsResponseData.ListOffsetsTopicResponse().setName(tp.topic()));
            status.responseOpt().ifPresent(res -> response.partitions().add(res));
        });
        responseCallback.accept(groupedByTopic.values());
    }

    /**
     * Try to complete the delayed operation by first checking if the operation
     * can be completed by now. If yes execute the completion logic by calling
     * forceComplete() and return true iff forceComplete returns true; otherwise return false
     */
    @Override
    public boolean tryComplete() {
        AtomicBoolean completable = new AtomicBoolean(true);
        statusByPartition.forEach((partition, status) -> {
            if (!status.completed()) {
                try {
                    partitionOrException.accept(partition);
                } catch (ApiException e) {
                    status.futureHolderOpt().ifPresent(futureHolder -> {
                        futureHolder.jobFuture().cancel(false);
                        futureHolder.taskFuture().complete(new OffsetResultHolder.FileRecordsOrError(Optional.of(e), Optional.empty()));
                    });
                }

                status.futureHolderOpt().ifPresent(futureHolder -> {
                    if (futureHolder.taskFuture().isDone()) {
                        ListOffsetsResponseData.ListOffsetsPartitionResponse response;
                        try {
                            OffsetResultHolder.FileRecordsOrError taskFuture = futureHolder.taskFuture().get();
                            if (taskFuture.hasException()) {
                                response = buildErrorResponse(Errors.forException(taskFuture.exception().get()), partition.partition());
                            } else if (!taskFuture.hasTimestampAndOffset()) {
                                Errors error = status.maybeOffsetsError()
                                        .map(e -> version >= 5 ? Errors.forException(e) : Errors.LEADER_NOT_AVAILABLE)
                                        .orElse(Errors.NONE);
                                response = buildErrorResponse(error, partition.partition());
                            } else {
                                ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse = buildErrorResponse(Errors.NONE, partition.partition());
                                FileRecords.TimestampAndOffset found = taskFuture.timestampAndOffset().get();
                                if (status.lastFetchableOffset().isPresent() && found.offset >= status.lastFetchableOffset().get()) {
                                    if (status.maybeOffsetsError().isPresent()) {
                                        Errors error = version >= 5 ? Errors.forException(status.maybeOffsetsError().get()) : Errors.LEADER_NOT_AVAILABLE;
                                        partitionResponse.setErrorCode(error.code());
                                    }
                                } else {
                                    partitionResponse = new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                            .setPartitionIndex(partition.partition())
                                            .setErrorCode(Errors.NONE.code())
                                            .setTimestamp(found.timestamp)
                                            .setOffset(found.offset);

                                    if (found.leaderEpoch.isPresent() && version >= 4) {
                                        partitionResponse.setLeaderEpoch(found.leaderEpoch.get());
                                    }
                                }
                                response = partitionResponse;
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            response = buildErrorResponse(Errors.forException(e), partition.partition());
                        }
                        status.responseOpt(Optional.of(response));
                        status.completed(true);
                    }
                    completable.set(completable.get() && futureHolder.taskFuture().isDone());
                });
            }
        });
        if (completable.get()) {
            return forceComplete();
        } else {
            return false;
        }
    }

    private ListOffsetsResponseData.ListOffsetsPartitionResponse buildErrorResponse(Errors e, int partitionIndex) {
        return new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                        .setPartitionIndex(partitionIndex)
                        .setErrorCode(e.code())
                        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
                        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET);
    }

    private static void recordExpiration(TopicPartition partition) {
        AGGREGATE_EXPIRATION_METER.mark();
        PARTITION_EXPIRATION_METERS.computeIfAbsent(partition, tp -> METRICS_GROUP.newMeter("ExpiresPerSec",
                "requests",
                TimeUnit.SECONDS,
                mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))).mark();
    }
}
