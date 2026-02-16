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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.OffsetResultHolder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class DelayedRemoteListOffsetsTest {

    private final int delayMs = 10;
    private final MockTimer timer = new MockTimer();
    private final Consumer<TopicPartition> partitionOrException = mock(Consumer.class);
    private final DelayedOperationPurgatory<DelayedRemoteListOffsets> purgatory =
            new DelayedOperationPurgatory<>("test-purgatory", timer, 0, 10, true, true);

    @AfterEach
    public void afterEach() throws Exception {
        purgatory.shutdown();
    }

    @Test
    public void testResponseOnRequestExpiration() throws InterruptedException {
        AtomicInteger numResponse = new AtomicInteger(0);
        Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback = response ->
            response.forEach(topic ->
                topic.partitions().forEach(partition -> {
                    assertEquals(Errors.REQUEST_TIMED_OUT.code(), partition.errorCode());
                    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp());
                    assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset());
                    assertEquals(-1, partition.leaderEpoch());
                    numResponse.incrementAndGet();
                })
            );

        AtomicInteger cancelledCount = new AtomicInteger(0);
        CompletableFuture<Void> jobFuture = mock(CompletableFuture.class);
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> holder = mock(AsyncOffsetReadFutureHolder.class);
        when(holder.taskFuture()).thenAnswer(f -> new CompletableFuture<>());
        when(holder.jobFuture()).thenReturn(jobFuture);
        when(jobFuture.cancel(anyBoolean())).thenAnswer(f -> {
            cancelledCount.incrementAndGet();
            return true;
        });

        Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition = Map.of(
            new TopicPartition("test", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test", 1), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test1", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build()
        );

        DelayedRemoteListOffsets delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, 5, statusByPartition, partitionOrException, responseCallback);
        List<TopicPartitionOperationKey> listOffsetsRequestKeys = statusByPartition.keySet().stream().map(TopicPartitionOperationKey::new).toList();
        assertEquals(0, DelayedRemoteListOffsets.AGGREGATE_EXPIRATION_METER.count());
        assertEquals(0, DelayedRemoteListOffsets.PARTITION_EXPIRATION_METERS.size());
        purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys);

        Thread.sleep(100);
        assertEquals(3, listOffsetsRequestKeys.size());
        assertEquals(cancelledCount.get(), listOffsetsRequestKeys.size());
        assertEquals(numResponse.get(), listOffsetsRequestKeys.size());
        assertEquals(listOffsetsRequestKeys.size(), DelayedRemoteListOffsets.AGGREGATE_EXPIRATION_METER.count());
        listOffsetsRequestKeys.forEach(key -> {
            TopicPartition tp = new TopicPartition(key.topic(), key.partition());
            assertEquals(1, DelayedRemoteListOffsets.PARTITION_EXPIRATION_METERS.get(tp).count());
        });
    }

    @Test
    public void testResponseOnSuccess() {
        AtomicInteger numResponse = new AtomicInteger(0);
        Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback = response ->
            response.forEach(topic ->
                topic.partitions().forEach(partition -> {
                    assertEquals(Errors.NONE.code(), partition.errorCode());
                    assertEquals(100L, partition.timestamp());
                    assertEquals(100L, partition.offset());
                    assertEquals(50, partition.leaderEpoch());
                    numResponse.incrementAndGet();
                })
            );

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 100L, Optional.of(50));
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> taskFuture = new CompletableFuture<>();
        taskFuture.complete(new OffsetResultHolder.FileRecordsOrError(Optional.empty(), Optional.of(timestampAndOffset)));

        AtomicInteger cancelledCount = new AtomicInteger(0);
        CompletableFuture<Void> jobFuture = mock(CompletableFuture.class);
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> holder = mock(AsyncOffsetReadFutureHolder.class);
        when(holder.taskFuture()).thenAnswer(f -> taskFuture);
        when(holder.jobFuture()).thenReturn(jobFuture);
        when(jobFuture.cancel(anyBoolean())).thenAnswer(f -> {
            cancelledCount.incrementAndGet();
            return true;
        });

        Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition = Map.of(
            new TopicPartition("test", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test", 1), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test1", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build()
        );

        DelayedRemoteListOffsets delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, 5, statusByPartition, partitionOrException, responseCallback);
        List<TopicPartitionOperationKey> listOffsetsRequestKeys = statusByPartition.keySet().stream().map(TopicPartitionOperationKey::new).toList();
        purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys);

        assertEquals(0, cancelledCount.get());
        assertEquals(numResponse.get(), listOffsetsRequestKeys.size());
    }

    @Test
    public void testResponseOnPartialError() {
        AtomicInteger numResponse = new AtomicInteger(0);
        Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback = response ->
            response.forEach(topic ->
                topic.partitions().forEach(partition -> {
                    if (topic.name().equals("test1")) {
                        assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), partition.errorCode());
                        assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp());
                        assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset());
                        assertEquals(-1, partition.leaderEpoch());
                    } else {
                        assertEquals(Errors.NONE.code(), partition.errorCode());
                        assertEquals(100L, partition.timestamp());
                        assertEquals(100L, partition.offset());
                        assertEquals(50, partition.leaderEpoch());
                    }
                    numResponse.incrementAndGet();
                })
            );

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 100L, Optional.of(50));
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> taskFuture = new CompletableFuture<>();
        taskFuture.complete(new OffsetResultHolder.FileRecordsOrError(Optional.empty(), Optional.of(timestampAndOffset)));

        AtomicInteger cancelledCount = new AtomicInteger(0);
        CompletableFuture<Void> jobFuture = mock(CompletableFuture.class);
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> holder = mock(AsyncOffsetReadFutureHolder.class);
        when(holder.taskFuture()).thenAnswer(f -> taskFuture);
        when(holder.jobFuture()).thenReturn(jobFuture);
        when(jobFuture.cancel(anyBoolean())).thenAnswer(f -> {
            cancelledCount.incrementAndGet();
            return true;
        });

        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> errorFutureHolder = mock(AsyncOffsetReadFutureHolder.class);
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> errorTaskFuture = new CompletableFuture<>();
        errorTaskFuture.complete(new OffsetResultHolder.FileRecordsOrError(Optional.of(new TimeoutException("Timed out!")), Optional.empty()));
        when(errorFutureHolder.taskFuture()).thenAnswer(f -> errorTaskFuture);
        when(errorFutureHolder.jobFuture()).thenReturn(jobFuture);

        Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition = Map.of(
            new TopicPartition("test", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test", 1), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test1", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(errorFutureHolder)).build()
        );

        DelayedRemoteListOffsets delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, 5, statusByPartition, partitionOrException, responseCallback);
        List<TopicPartitionOperationKey> listOffsetsRequestKeys = statusByPartition.keySet().stream().map(TopicPartitionOperationKey::new).toList();
        purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys);

        assertEquals(0, cancelledCount.get());
        assertEquals(numResponse.get(), listOffsetsRequestKeys.size());
    }

    @Test
    public void testPartialResponseWhenNotLeaderOrFollowerExceptionOnOnePartition() {
        AtomicInteger numResponse = new AtomicInteger(0);
        Consumer<Collection<ListOffsetsResponseData.ListOffsetsTopicResponse>> responseCallback = response ->
            response.forEach(topic ->
                topic.partitions().forEach(partition -> {
                    if (topic.name().equals("test1") && partition.partitionIndex() == 0) {
                        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code(), partition.errorCode());
                        assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partition.timestamp());
                        assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partition.offset());
                        assertEquals(-1, partition.leaderEpoch());
                    } else {
                        assertEquals(Errors.NONE.code(), partition.errorCode());
                        assertEquals(100L, partition.timestamp());
                        assertEquals(100L, partition.offset());
                        assertEquals(50, partition.leaderEpoch());
                    }
                    numResponse.incrementAndGet();
                })
            );

        FileRecords.TimestampAndOffset timestampAndOffset = new FileRecords.TimestampAndOffset(100L, 100L, Optional.of(50));
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> taskFuture = new CompletableFuture<>();
        taskFuture.complete(new OffsetResultHolder.FileRecordsOrError(Optional.empty(), Optional.of(timestampAndOffset)));

        AtomicInteger cancelledCount = new AtomicInteger(0);
        CompletableFuture<Void> jobFuture = mock(CompletableFuture.class);
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> holder = mock(AsyncOffsetReadFutureHolder.class);
        when(holder.taskFuture()).thenAnswer(f -> taskFuture);
        when(holder.jobFuture()).thenReturn(jobFuture);
        when(jobFuture.cancel(anyBoolean())).thenAnswer(f -> {
            cancelledCount.incrementAndGet();
            return true;
        });

        doThrow(new NotLeaderOrFollowerException("Not leader or follower!"))
                .when(partitionOrException).accept(new TopicPartition("test1", 0));
        AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> errorFutureHolder = mock(AsyncOffsetReadFutureHolder.class);
        CompletableFuture<OffsetResultHolder.FileRecordsOrError> errorTaskFuture = new CompletableFuture<>();
        when(errorFutureHolder.taskFuture()).thenAnswer(f -> errorTaskFuture);
        when(errorFutureHolder.jobFuture()).thenReturn(jobFuture);

        Map<TopicPartition, ListOffsetsPartitionStatus> statusByPartition = Map.of(
            new TopicPartition("test", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test", 1), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build(),
            new TopicPartition("test1", 0), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(errorFutureHolder)).build(),
            new TopicPartition("test1", 1), ListOffsetsPartitionStatus.builder().futureHolderOpt(Optional.of(holder)).build()
        );

        DelayedRemoteListOffsets delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, 5, statusByPartition, partitionOrException, responseCallback);
        List<TopicPartitionOperationKey> listOffsetsRequestKeys = statusByPartition.keySet().stream().map(TopicPartitionOperationKey::new).toList();
        purgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys);

        assertEquals(1, cancelledCount.get());
        assertEquals(numResponse.get(), listOffsetsRequestKeys.size());
    }
}
