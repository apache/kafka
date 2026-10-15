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

import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogReadResult;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaManagerLogReaderTest {

    private static final TopicIdPartition TOPIC_ID_PARTITION =
        new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private static final TopicIdPartition TOPIC_ID_PARTITION_2 =
        new TopicIdPartition(Uuid.randomUuid(), 1, "topic");
    // A real config with defaults; readRemote() reads remote.fetch.max.wait.ms from it for its timeout.
    private static final RemoteLogManagerConfig REMOTE_LOG_MANAGER_CONFIG =
        new RemoteLogManagerConfig(new AbstractConfig(RemoteLogManagerConfig.configDef(), Map.of(), false));

    // A ReplicaManager mock whose config() chain resolves remote.fetch.max.wait.ms, as readRemote() needs.
    private static ReplicaManager mockReplicaManager() {
        ReplicaManager replicaManager = mock(ReplicaManager.class);
        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        when(kafkaConfig.remoteLogManagerConfig()).thenReturn(REMOTE_LOG_MANAGER_CONFIG);
        when(replicaManager.config()).thenReturn(kafkaConfig);
        return replicaManager;
    }

    private static RemoteStorageFetchInfo remoteStorageFetchInfo() {
        return new RemoteStorageFetchInfo(
            1024,
            true,
            TOPIC_ID_PARTITION,
            new FetchRequest.PartitionData(TOPIC_ID_PARTITION.topicId(), 0L, 0L, 1024, Optional.empty()),
            FetchIsolation.HIGH_WATERMARK);
    }

    private static FetchParams fetchParams() {
        return new FetchParams(
            FetchRequest.CONSUMER_REPLICA_ID, -1, 0L, 1, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
    }

    private static void stubReadFromLog(ReplicaManager replicaManager, List<Tuple2<TopicIdPartition, LogReadResult>> results) {
        when(replicaManager.readFromLog(any(), any(), any(), anyBoolean()))
            .thenReturn(CollectionConverters.asScala(results).toSeq());
    }

    private static LogReadResult localReadResult(FetchDataInfo info, Errors error) {
        LogReadResult logReadResult = mock(LogReadResult.class);
        when(logReadResult.info()).thenReturn(info);
        when(logReadResult.error()).thenReturn(error);
        return logReadResult;
    }

    private static FetchDataInfo localData() {
        return new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY);
    }

    private static FetchDataInfo tieredData(RemoteStorageFetchInfo remoteStorageFetchInfo) {
        return new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY, false,
            Optional.empty(), Optional.of(remoteStorageFetchInfo));
    }

    private static LinkedHashMap<TopicIdPartition, Long> offsets(TopicIdPartition... partitions) {
        LinkedHashMap<TopicIdPartition, Long> offsets = new LinkedHashMap<>();
        for (TopicIdPartition partition : partitions) {
            offsets.put(partition, 0L);
        }
        return offsets;
    }

    private static LinkedHashMap<TopicIdPartition, Integer> maxBytes(TopicIdPartition... partitions) {
        LinkedHashMap<TopicIdPartition, Integer> maxBytes = new LinkedHashMap<>();
        for (TopicIdPartition partition : partitions) {
            maxBytes.put(partition, 1024);
        }
        return maxBytes;
    }

    @Test
    public void testReadReturnsEmptyWhenNoPartitionsToFetch() {
        ReplicaManager replicaManager = mockReplicaManager();

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.read(fetchParams(), Set.of(), new LinkedHashMap<>(), new LinkedHashMap<>());

        assertTrue(result.isEmpty());
        verify(replicaManager, never()).readFromLog(any(), any(), any(), anyBoolean());
    }

    @Test
    public void testReadReturnsResultsFromReplicaManager() {
        ReplicaManager replicaManager = mockReplicaManager();
        LogReadResult logReadResult = mock(LogReadResult.class);
        Seq<Tuple2<TopicIdPartition, LogReadResult>> readFromLogResult =
            CollectionConverters.asScala(List.of(new Tuple2<>(TOPIC_ID_PARTITION, logReadResult))).toSeq();
        when(replicaManager.readFromLog(any(), any(), any(), anyBoolean())).thenReturn(readFromLogResult);

        LinkedHashMap<TopicIdPartition, Long> offsets = new LinkedHashMap<>();
        offsets.put(TOPIC_ID_PARTITION, 5L);
        LinkedHashMap<TopicIdPartition, Integer> maxBytes = new LinkedHashMap<>();
        maxBytes.put(TOPIC_ID_PARTITION, 1024);

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.read(fetchParams(), Set.of(TOPIC_ID_PARTITION), offsets, maxBytes);

        assertEquals(1, result.size());
        assertSame(logReadResult, result.get(TOPIC_ID_PARTITION));
    }

    @Test
    public void testReadRemoteCompletesExceptionallyWhenRemoteLogManagerNotConfigured() {
        ReplicaManager replicaManager = mockReplicaManager();
        when(replicaManager.remoteLogManager()).thenReturn(Option.empty());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(remoteStorageFetchInfo());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, exception.getCause());
    }

    @Test
    public void testReadRemoteCompletesWithFetchedData() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        FetchDataInfo fetchDataInfo = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY);
        RemoteStorageFetchInfo fetchInfo = remoteStorageFetchInfo();
        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.of(fetchDataInfo), Optional.empty()));
            return null;
        }).when(remoteLogManager).asyncRead(eq(fetchInfo), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(fetchInfo);

        assertSame(fetchDataInfo, future.get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testReadRemoteCompletesExceptionallyWhenReadResultHasError() {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        RuntimeException readError = new RuntimeException("remote read failed");
        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.empty(), Optional.of(readError)));
            return null;
        }).when(remoteLogManager).asyncRead(any(), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(remoteStorageFetchInfo());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertSame(readError, exception.getCause());
    }

    @Test
    public void testReadRemoteCompletesExceptionallyWhenReadResultIsEmpty() {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.empty(), Optional.empty()));
            return null;
        }).when(remoteLogManager).asyncRead(any(), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(remoteStorageFetchInfo());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, exception.getCause());
    }

    @Test
    public void testReadRemoteCompletesExceptionallyWhenSchedulingRejected() {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        RejectedExecutionException rejected = new RejectedExecutionException("reader pool shutting down");
        doThrow(rejected).when(remoteLogManager).asyncRead(any(), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(remoteStorageFetchInfo());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertSame(rejected, exception.getCause());
    }

    @Test
    public void testReadRemoteTimesOutWhenRemoteReadNeverCompletes() {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));
        // asyncRead never invokes the callback, so the remote read never completes on its own.
        // Simulate the timer wheel firing by running the scheduled timeout task immediately.
        doAnswer(invocation -> {
            invocation.<TimerTask>getArgument(0).run();
            return null;
        }).when(replicaManager).addShareFetchTimerRequest(any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<FetchDataInfo> future = logReader.readRemote(remoteStorageFetchInfo());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    @Test
    public void testReadAsyncReturnsEmptyWhenNoPartitionsToFetch() {
        ReplicaManager replicaManager = mockReplicaManager();

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(), new LinkedHashMap<>(), new LinkedHashMap<>(), true).join();

        assertTrue(result.isEmpty());
        verify(replicaManager, never()).readFromLog(any(), any(), any(), anyBoolean());
    }

    @Test
    public void testReadAsyncReturnsLocalDataWhenNotTiered() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        FetchDataInfo localData = localData();
        stubReadFromLog(replicaManager,
            List.of(new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(localData, Errors.NONE))));

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), true).get(10, TimeUnit.SECONDS);

        assertEquals(Set.of(TOPIC_ID_PARTITION), result.keySet());
        LogReadResult logReadResult = result.get(TOPIC_ID_PARTITION);
        assertSame(localData, logReadResult.info());
        assertEquals(Errors.NONE, logReadResult.error());
        // Local data, so no remote read should have been attempted.
        verify(replicaManager, never()).remoteLogManager();
    }

    @Test
    public void testReadAsyncFollowsRemoteWhenTieredAndReadRemoteTrue() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        RemoteStorageFetchInfo remoteStorageFetchInfo = remoteStorageFetchInfo();
        stubReadFromLog(replicaManager,
            List.of(new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(tieredData(remoteStorageFetchInfo), Errors.NONE))));

        FetchDataInfo remoteData = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY);
        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.of(remoteData), Optional.empty()));
            return null;
        }).when(remoteLogManager).asyncRead(eq(remoteStorageFetchInfo), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), true).get(10, TimeUnit.SECONDS);

        LogReadResult logReadResult = result.get(TOPIC_ID_PARTITION);
        assertSame(remoteData, logReadResult.info());
        assertEquals(Errors.NONE, logReadResult.error());
        verify(remoteLogManager).asyncRead(eq(remoteStorageFetchInfo), any());
    }

    @Test
    public void testReadAsyncSkipsRemoteWhenReadRemoteFalse() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        FetchDataInfo tieredData = tieredData(remoteStorageFetchInfo());
        stubReadFromLog(replicaManager,
            List.of(new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(tieredData, Errors.NONE))));

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), false).get(10, TimeUnit.SECONDS);

        LogReadResult logReadResult = result.get(TOPIC_ID_PARTITION);
        // Tiered data is skipped (local read returned as-is), and the remote tier is never consulted.
        assertSame(tieredData, logReadResult.info());
        assertEquals(Errors.NONE, logReadResult.error());
        verify(replicaManager, never()).remoteLogManager();
    }

    @Test
    public void testReadAsyncReturnsErrorFromLocalRead() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        FetchDataInfo localData = localData();
        stubReadFromLog(replicaManager,
            List.of(new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(localData, Errors.UNKNOWN_SERVER_ERROR))));

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), true).get(10, TimeUnit.SECONDS);

        LogReadResult logReadResult = result.get(TOPIC_ID_PARTITION);
        assertSame(localData, logReadResult.info());
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, logReadResult.error());
        verify(replicaManager, never()).remoteLogManager();
    }

    @Test
    public void testReadAsyncCompletesExceptionallyWhenLocalReadThrows() {
        ReplicaManager replicaManager = mockReplicaManager();
        RuntimeException readError = new RuntimeException("read failed");
        when(replicaManager.readFromLog(any(), any(), any(), anyBoolean())).thenThrow(readError);

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        CompletableFuture<LinkedHashMap<TopicIdPartition, LogReadResult>> future =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), true);

        // readAsync must surface the failure via the returned future rather than throwing synchronously.
        assertTrue(future.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertSame(readError, exception.getCause());
    }

    @Test
    public void testReadAsyncReturnsNoneErrorWhenRemoteReadFailsButLocalSucceeds() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        FetchDataInfo tieredData = tieredData(remoteStorageFetchInfo());
        stubReadFromLog(replicaManager,
            List.of(new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(tieredData, Errors.NONE))));

        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.empty(), Optional.of(new RuntimeException("remote read failed"))));
            return null;
        }).when(remoteLogManager).asyncRead(any(), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION),
                offsets(TOPIC_ID_PARTITION), maxBytes(TOPIC_ID_PARTITION), true).get(10, TimeUnit.SECONDS);

        LogReadResult logReadResult = result.get(TOPIC_ID_PARTITION);
        // Partial-data tolerant: the read completes (does not throw) with the local info and the error.
        assertSame(tieredData, logReadResult.info());
        assertEquals(Errors.NONE, logReadResult.error());
    }

    @Test
    public void testReadAsyncResolvesPartitionsIndependently() throws Exception {
        ReplicaManager replicaManager = mockReplicaManager();
        RemoteLogManager remoteLogManager = mock(RemoteLogManager.class);
        when(replicaManager.remoteLogManager()).thenReturn(Option.apply(remoteLogManager));

        // Partition 1 is tiered (resolved from remote), partition 2 is available locally.
        RemoteStorageFetchInfo remoteStorageFetchInfo = remoteStorageFetchInfo();
        FetchDataInfo localData = localData();
        stubReadFromLog(replicaManager, List.of(
            new Tuple2<>(TOPIC_ID_PARTITION, localReadResult(tieredData(remoteStorageFetchInfo), Errors.NONE)),
            new Tuple2<>(TOPIC_ID_PARTITION_2, localReadResult(localData, Errors.NONE))));

        FetchDataInfo remoteData = new FetchDataInfo(new LogOffsetMetadata(0L), MemoryRecords.EMPTY);
        doAnswer(invocation -> {
            Consumer<RemoteLogReadResult> callback = invocation.getArgument(1);
            callback.accept(new RemoteLogReadResult(Optional.of(remoteData), Optional.empty()));
            return null;
        }).when(remoteLogManager).asyncRead(eq(remoteStorageFetchInfo), any());

        ReplicaManagerLogReader logReader = new ReplicaManagerLogReader(replicaManager);
        LinkedHashMap<TopicIdPartition, LogReadResult> result =
            logReader.readAsync(fetchParams(), Set.of(TOPIC_ID_PARTITION, TOPIC_ID_PARTITION_2),
                offsets(TOPIC_ID_PARTITION, TOPIC_ID_PARTITION_2),
                maxBytes(TOPIC_ID_PARTITION, TOPIC_ID_PARTITION_2), true).get(10, TimeUnit.SECONDS);

        assertEquals(Set.of(TOPIC_ID_PARTITION, TOPIC_ID_PARTITION_2), result.keySet());
        assertSame(remoteData, result.get(TOPIC_ID_PARTITION).info());
        assertSame(localData, result.get(TOPIC_ID_PARTITION_2).info());
    }
}
