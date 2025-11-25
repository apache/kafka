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
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchPartitionStatus;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelayedRemoteFetchTest {
    private final int maxBytes = 1024;
    private final Consumer<TopicPartition> partitionOrException = mock(Consumer.class);
    private final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private final TopicIdPartition topicIdPartition2 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic2");
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

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, FetchPartitionData> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue());
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(buildRemoteReadResult(Errors.NONE));

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);
        long highWatermark = 100L;
        long leaderLogStartOffset = 10L;
        LogReadResult logReadInfo = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            Map.of(),
            Map.of(topicIdPartition, future),
            Map.of(topicIdPartition, fetchInfo),
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo),
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
        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses -> {
            assertEquals(1, responses.size());
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(buildRemoteReadResult(Errors.NONE));
        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);
        LogReadResult logReadInfo = buildReadResult(Errors.NONE, 100L, 10L);

        assertThrows(IllegalStateException.class, () ->
            new DelayedRemoteFetch(
                Map.of(),
                Map.of(topicIdPartition, future),
                Map.of(topicIdPartition, fetchInfo),
                remoteFetchMaxWaitMs,
                Map.of(topicIdPartition, fetchStatus),
                buildFetchParams(1, 500),
                Map.of(topicIdPartition, logReadInfo),
                partitionOrException,
                callback
            ));
    }

    @Test
    public void testNotLeaderOrFollower() {
        AtomicReference<TopicIdPartition> actualTopicPartition = new AtomicReference<>();
        AtomicReference<FetchPartitionData> fetchResultOpt = new AtomicReference<>();

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, FetchPartitionData> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue());
        };

        // throw exception while getPartition
        doThrow(new NotLeaderOrFollowerException(String.format("Replica for %s not available", topicIdPartition)))
            .when(partitionOrException).accept(topicIdPartition.topicPartition());

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);

        LogReadResult logReadInfo = buildReadResult(Errors.NONE);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            Map.of(),
            Map.of(topicIdPartition, future),
            Map.of(topicIdPartition, fetchInfo),
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo),
            partitionOrException,
            callback
        );

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

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses -> {
            assertEquals(1, responses.size());
            Map.Entry<TopicIdPartition, FetchPartitionData> entry = responses.entrySet().iterator().next();
            actualTopicPartition.set(entry.getKey());
            fetchResultOpt.set(entry.getValue());
        };

        CompletableFuture<RemoteLogReadResult> future = new CompletableFuture<>();
        future.complete(buildRemoteReadResult(Errors.NONE));

        RemoteStorageFetchInfo fetchInfo = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);

        // build a read result with error
        LogReadResult logReadInfo = buildReadResult(Errors.FENCED_LEADER_EPOCH);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            Map.of(),
            Map.of(topicIdPartition, future),
            Map.of(topicIdPartition, fetchInfo),
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo),
            partitionOrException,
            callback
        );

        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());
        assertEquals(topicIdPartition, actualTopicPartition.get());
        assertNotNull(fetchResultOpt.get());
        assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResultOpt.get().error);
    }

    private long expiresPerSecValue() {
        Map<MetricName, Metric> allMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics();
        return allMetrics.entrySet()
            .stream()
            .filter(e -> e.getKey().getMBeanName().endsWith("kafka.server:type=DelayedRemoteFetchMetrics,name=ExpiresPerSec"))
            .findFirst()
            .map(Map.Entry::getValue)
            .filter(Meter.class::isInstance)
            .map(Meter.class::cast)
            .map(Meter::count)
            .orElse(0L);
    }

    @Test
    public void testRequestExpiry() {
        Map<TopicIdPartition, FetchPartitionData> responses = new HashMap<>();

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses::putAll;

        Future<Void> remoteFetchTaskExpired = mock(Future.class);
        Future<Void> remoteFetchTask2 = mock(Future.class);
        // complete the 2nd task, and keep the 1st one expired
        when(remoteFetchTask2.isDone()).thenReturn(true);

        // Create futures - one completed, one not
        CompletableFuture<RemoteLogReadResult> future1 = new CompletableFuture<>();
        CompletableFuture<RemoteLogReadResult> future2 = new CompletableFuture<>();
        // Only complete one remote fetch
        future2.complete(buildRemoteReadResult(Errors.NONE));

        RemoteStorageFetchInfo fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);
        RemoteStorageFetchInfo fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition2, null, null);

        long highWatermark = 100L;
        long leaderLogStartOffset = 10L;

        LogReadResult logReadInfo1 = buildReadResult(Errors.NONE, highWatermark, leaderLogStartOffset);
        LogReadResult logReadInfo2 = buildReadResult(Errors.NONE);

        FetchPartitionStatus fetchStatus1 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch));

        FetchPartitionStatus fetchStatus2 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset + 100L),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100L, logStartOffset, maxBytes, currentLeaderEpoch));

        // Set up maps for multiple partitions
        Map<TopicIdPartition, Future<Void>> remoteFetchTasks = Map.of(topicIdPartition, remoteFetchTaskExpired, topicIdPartition2, remoteFetchTask2);
        Map<TopicIdPartition, CompletableFuture<RemoteLogReadResult>> remoteFetchResults = Map.of(topicIdPartition, future1, topicIdPartition2, future2);
        Map<TopicIdPartition, RemoteStorageFetchInfo> remoteFetchInfos = Map.of(topicIdPartition, fetchInfo1, topicIdPartition2, fetchInfo2);

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            remoteFetchTasks,
            remoteFetchResults,
            remoteFetchInfos,
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus1, topicIdPartition2, fetchStatus2),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo1, topicIdPartition2, logReadInfo2),
            partitionOrException,
            callback
        );

        // Verify that the ExpiresPerSec metric is zero before fetching
        long existingMetricVal = expiresPerSecValue();
        // Verify the delayedRemoteFetch is not completed yet
        assertFalse(delayedRemoteFetch.isCompleted());

        // Force the delayed remote fetch to expire
        delayedRemoteFetch.run();

        // Check that the expired task was cancelled and force-completed
        verify(remoteFetchTaskExpired).cancel(anyBoolean());
        verify(remoteFetchTask2, never()).cancel(anyBoolean());
        assertTrue(delayedRemoteFetch.isCompleted());

        // Check that the ExpiresPerSec metric was incremented
        assertTrue(expiresPerSecValue() > existingMetricVal);

        // Fetch results should include 2 results and the expired one should return local read results
        assertEquals(2, responses.size());
        assertTrue(responses.containsKey(topicIdPartition));
        assertTrue(responses.containsKey(topicIdPartition2));

        assertEquals(Errors.NONE, responses.get(topicIdPartition).error);
        assertEquals(highWatermark, responses.get(topicIdPartition).highWatermark);
        assertEquals(leaderLogStartOffset, responses.get(topicIdPartition).logStartOffset);

        assertEquals(Errors.NONE, responses.get(topicIdPartition2).error);
    }

    @Test
    public void testMultiplePartitions() {
        Map<TopicIdPartition, FetchPartitionData> responses = new HashMap<>();

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses::putAll;

        // Create futures - one completed, one not
        CompletableFuture<RemoteLogReadResult> future1 = new CompletableFuture<>();
        CompletableFuture<RemoteLogReadResult> future2 = new CompletableFuture<>();
        // Only complete one remote fetch
        future1.complete(buildRemoteReadResult(Errors.NONE));

        RemoteStorageFetchInfo fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);
        RemoteStorageFetchInfo fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);

        long highWatermark1 = 100L;
        long leaderLogStartOffset1 = 10L;
        long highWatermark2 = 200L;
        long leaderLogStartOffset2 = 20L;

        LogReadResult logReadInfo1 = buildReadResult(Errors.NONE, highWatermark1, leaderLogStartOffset1);
        LogReadResult logReadInfo2 = buildReadResult(Errors.NONE, highWatermark2, leaderLogStartOffset2);

        FetchPartitionStatus fetchStatus1 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch));

        FetchPartitionStatus fetchStatus2 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset + 100L),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100L, logStartOffset, maxBytes, currentLeaderEpoch));

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            Map.of(),
            Map.of(topicIdPartition, future1, topicIdPartition2, future2),
            Map.of(topicIdPartition, fetchInfo1, topicIdPartition2, fetchInfo2),
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus1, topicIdPartition2, fetchStatus2),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo1, topicIdPartition2, logReadInfo2),
            partitionOrException,
            callback
        );

        // Should not complete since future2 is not done
        assertFalse(delayedRemoteFetch.tryComplete());
        assertFalse(delayedRemoteFetch.isCompleted());

        // Complete future2
        future2.complete(buildRemoteReadResult(Errors.NONE));

        // Now it should complete
        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());

        // Verify both partitions were processed without error
        assertEquals(2, responses.size());
        assertTrue(responses.containsKey(topicIdPartition));
        assertTrue(responses.containsKey(topicIdPartition2));

        assertEquals(Errors.NONE, responses.get(topicIdPartition).error);
        assertEquals(highWatermark1, responses.get(topicIdPartition).highWatermark);
        assertEquals(leaderLogStartOffset1, responses.get(topicIdPartition).logStartOffset);

        assertEquals(Errors.NONE, responses.get(topicIdPartition2).error);
        assertEquals(highWatermark2, responses.get(topicIdPartition2).highWatermark);
        assertEquals(leaderLogStartOffset2, responses.get(topicIdPartition2).logStartOffset);
    }

    @Test
    public void testMultiplePartitionsWithFailedResults() {
        Map<TopicIdPartition, FetchPartitionData> responses = new HashMap<>();

        Consumer<Map<TopicIdPartition, FetchPartitionData>> callback = responses::putAll;

        // Create futures - one successful, one with error
        CompletableFuture<RemoteLogReadResult> future1 = new CompletableFuture<>();
        CompletableFuture<RemoteLogReadResult> future2 = new CompletableFuture<>();

        // Created 1 successful result and 1 failed result
        future1.complete(buildRemoteReadResult(Errors.NONE));
        future2.complete(buildRemoteReadResult(Errors.UNKNOWN_SERVER_ERROR));

        RemoteStorageFetchInfo fetchInfo1 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);
        RemoteStorageFetchInfo fetchInfo2 = new RemoteStorageFetchInfo(0, false, topicIdPartition, null, null);

        LogReadResult logReadInfo1 = buildReadResult(Errors.NONE, 100, 10);
        LogReadResult logReadInfo2 = buildReadResult(Errors.NONE, 100, 10);

        FetchPartitionStatus fetchStatus1 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch));

        FetchPartitionStatus fetchStatus2 = new FetchPartitionStatus(
            new LogOffsetMetadata(fetchOffset + 100),
            new FetchRequest.PartitionData(Uuid.ZERO_UUID, fetchOffset + 100, logStartOffset, maxBytes, currentLeaderEpoch));

        DelayedRemoteFetch delayedRemoteFetch = new DelayedRemoteFetch(
            Map.of(),
            Map.of(topicIdPartition, future1, topicIdPartition2, future2),
            Map.of(topicIdPartition, fetchInfo1, topicIdPartition2, fetchInfo2),
            remoteFetchMaxWaitMs,
            Map.of(topicIdPartition, fetchStatus1, topicIdPartition2, fetchStatus2),
            fetchParams,
            Map.of(topicIdPartition, logReadInfo1, topicIdPartition2, logReadInfo2),
            partitionOrException,
            callback
        );

        assertTrue(delayedRemoteFetch.tryComplete());
        assertTrue(delayedRemoteFetch.isCompleted());

        // Verify both partitions were processed
        assertEquals(2, responses.size());
        assertTrue(responses.containsKey(topicIdPartition));
        assertTrue(responses.containsKey(topicIdPartition2));

        // First partition should be successful
        FetchPartitionData fetchResult1 = responses.get(topicIdPartition);
        assertEquals(Errors.NONE, fetchResult1.error);

        // Second partition should have an error due to remote fetch failure
        FetchPartitionData fetchResult2 = responses.get(topicIdPartition2);
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, fetchResult2.error);
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

    private LogReadResult buildReadResult(Errors error, long highWatermark, long leaderLogStartOffset) {
        return new LogReadResult(
            new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY, false, Optional.empty(),
            Optional.of(mock(RemoteStorageFetchInfo.class))),
            Optional.empty(),
            highWatermark,
            leaderLogStartOffset,
            -1L,
            -1L,
            -1L,
            OptionalLong.empty(),
            error);
    }

    private RemoteLogReadResult buildRemoteReadResult(Errors error) {
        return new RemoteLogReadResult(
            Optional.of(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY)),
            error != Errors.NONE ? Optional.of(error.exception()) : Optional.empty());
    }
}
