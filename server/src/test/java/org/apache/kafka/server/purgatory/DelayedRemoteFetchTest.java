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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.FetchPartitionStatus;
import org.apache.kafka.server.LogReadResult;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DelayedRemoteFetchTest {
    private final int maxBytes = 1024;
    private final Consumer<TopicPartition> partitionOrException = mock(Consumer.class);
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private final long fetchOffset = 500L;
    private final long logStartOffset = 0L;
    private final Optional<Integer> currentLeaderEpoch = Optional.of(10);
    private final int remoteFetchMaxWaitMs = 500;

    private final FetchPartitionStatus fetchStatus = new FetchPartitionStatus(
        new LogOffsetMetadata(fetchOffset),
        new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch)
    );
    private final FetchParams fetchParams = buildFetchParams(-1, 500);

    @Test
    public void testFetch() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, List<FetchPartitionData>> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue().get(0));
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(null);

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false,
            topicIdPartition.topicPartition(), null, null);
        int highWatermark = 100;
        int leaderLogStartOffset = 10;
        LogReadResult logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            null,
            future,
            fetchInfo,
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, List.of(fetchStatus)),
            fetchParams,
            Map.of(topicIdPartition, List.of(logReadInfo)),
            partitionOrException,
            callback
        );

        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());
        assertNotNull(actualTopicPartition.get());
        assertEquals(topicIdPartition, actualTopicPartition.get());
        assertNotNull(fetchResultOpt.get());

        FetchPartitionData fetchResult = fetchResultOpt.get();
        assertEquals(Errors.NONE, fetchResult.error);
        assertEquals(highWatermark, fetchResult.highWatermark);
        assertEquals(leaderLogStartOffset, fetchResult.logStartOffset);
    }

    @Test
    public void testFollowerFetch() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, List<FetchPartitionData>> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue().get(0));
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(null);
        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false,
            new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()), null, null);
        LogReadResult logReadInfo = buildReadResult(Errors.NONE, 100, 10);

        assertThrows(IllegalStateException.class, () ->
            new DelayedRemoteFetch(
                null,
                future,
                fetchInfo,
                remoteFetchMaxWaitMs,
                Map.of(topicIdPartition, List.of(fetchStatus)),
                buildFetchParams(1, 500),
                Map.of(topicIdPartition, List.of(logReadInfo)),
                partitionOrException,
                callback
            ));
    }

    @Test
    public void testNotLeaderOrFollower() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, List<FetchPartitionData>> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue().get(0));
        };

        // throw exception while getPartition
        doThrow(new NotLeaderOrFollowerException(String.format("Replica for %s not available", topicIdPartition)))
            .when(partitionOrException).accept(topicIdPartition.topicPartition());

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false,
            new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()), null, null);

        LogReadResult logReadInfo = buildReadResult(Errors.NONE);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            null,
            future,
            fetchInfo,
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, List.of(fetchStatus)),
            fetchParams,
            Map.of(topicIdPartition, List.of(logReadInfo)),
            partitionOrException,
            callback);

        // delayed remote fetch should still be able to complete
        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());
        assertEquals(topicIdPartition, actualTopicPartition.get());
        assertNotNull(fetchResultOpt.get());
    }

    @Test
    public void testErrorLogReadInfo() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, List<FetchPartitionData>> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue().get(0));
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(null);

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false,
            new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()), null, null);

        // build a read result with error
        LogReadResult logReadInfo = buildReadResult(Errors.FENCED_LEADER_EPOCH);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            null,
            future,
            fetchInfo,
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, List.of(fetchStatus)),
            fetchParams,
            Map.of(topicIdPartition, List.of(logReadInfo)),
            partitionOrException,
            callback
        );

        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());
        assertEquals(topicIdPartition, actualTopicPartition.get());
        assertNotNull(fetchResultOpt.get());
        assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResultOpt.get().error);
    }

    @Test
    public void testRequestExpiry() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, List<FetchPartitionData>>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, List<FetchPartitionData>> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue().get(0));
        };

        int highWatermark = 100;
        int leaderLogStartOffset = 10;

        Future<Void> remoteFetchTask = mock(Future.class);
        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false,
            new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()), null, null);
        LogReadResult logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            remoteFetchTask,
            future,
            fetchInfo,
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, List.of(fetchStatus)),
            fetchParams,
            Map.of(topicIdPartition, List.of(logReadInfo)),
            partitionOrException,
            callback
        );

        // Force the delayed remote fetch to expire
        delayedRemoteFetch.run();

        // Check that the task was cancelled and force-completed
        verify(remoteFetchTask).cancel(false);
        assertTrue(delayedRemoteFetch.isCompleted());

        long metricsCount = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
            .filter(m -> m.getMBeanName().equals("kafka.server:type=DelayedRemoteFetchMetrics,name=ExpiresPerSec"))
            .count();
        assertEquals(1, metricsCount);

        // Fetch results should still include local read results
        assertNotNull(actualTopicPartition.get());
        assertEquals(topicIdPartition, actualTopicPartition.get());
        assertNotNull(fetchResultOpt.get());

        FetchPartitionData fetchResult = fetchResultOpt.get();
        assertEquals(Errors.NONE, fetchResult.error);
        assertEquals(highWatermark, fetchResult.highWatermark);
        assertEquals(leaderLogStartOffset, fetchResult.logStartOffset);
    }

    private FetchParams buildFetchParams(int replicaId, int maxWaitMs) {
        return new FetchParams(
            replicaId,
            1,
            maxWaitMs,
            1,
            maxBytes,
            FetchIsolation.LOG_END,
            Optional.empty()
        );
    }

    private LogReadResult buildReadResult(Errors error) {
        return buildReadResult(error, 0, 0);
    }

    private LogReadResult buildReadResult(Errors error, int highWatermark, int leaderLogStartOffset) {
        return new LogReadResult(
            new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            highWatermark,
            leaderLogStartOffset,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            error != Errors.NONE ? Optional.of(error.exception()) : Optional.empty());
    }
}