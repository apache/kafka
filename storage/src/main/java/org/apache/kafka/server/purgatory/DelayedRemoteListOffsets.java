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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class DelayedRemoteListOffsets extends DelayedOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DelayedRemoteListOffsets.class);

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "DelayedRemoteListOffsetsMetrics");
    final Meter aggregateExpirationMeter = metricsGroup.newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS);
    final Map<TopicPartition, Meter> partitionExpirationMeters = new ConcurrentHashMap<>();
    private final int version;
    private final Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition;
    private final PartitionChecker partitionChecker;
    private final Consumer<List<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback;

    public DelayedRemoteListOffsets(long delayMs,
                                    int version,
                                    Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition,
                                    PartitionChecker partitionChecker,
                                    Consumer<List<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback) {
        super(delayMs);
        this.version = version;
        this.statusByPartition = statusByPartition;
        this.partitionChecker = partitionChecker;
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
        Map<String, List<ListOffsetsResponseData.ListOffsetsPartitionResponse>> groupedByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsPartitionStatus> entry : statusByPartition.entrySet()) {
            List<ListOffsetsResponseData.ListOffsetsPartitionResponse> partitions =
                    groupedByTopic.computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>());
            if (entry.getValue().responseOpt().isPresent()) {
                partitions.add(entry.getValue().responseOpt().get());
            }
        }
        List<ListOffsetsResponseData.ListOffsetsTopicResponse> response = new ArrayList<>();
        for (Map.Entry<String, List<ListOffsetsResponseData.ListOffsetsPartitionResponse>> entry : groupedByTopic.entrySet()) {
            response.add(new ListOffsetsResponseData.ListOffsetsTopicResponse()
                    .setName(entry.getKey())
                    .setPartitions(entry.getValue()));
        }
        responseCallback.accept(response);
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
                    partitionChecker.existsOrThrow(partition);
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

    private void recordExpiration(TopicPartition partition) {
        aggregateExpirationMeter.mark();
        partitionExpirationMeters.computeIfAbsent(partition, tp -> metricsGroup.newMeter("ExpiresPerSec",
                "requests",
                TimeUnit.SECONDS,
                Map.of("topic", tp.topic(), "partition", String.valueOf(tp.partition())))).mark();
    }
}
