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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState.ACTIVE;
import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState.CLOSED;
import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState.FAILED;
import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState.INITIAL;
import static org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime.CoordinatorState.LOADING;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorRuntimeTest {
    private static final TopicPartition TP = new TopicPartition("__consumer_offsets", 0);
    private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(5);

    /**
     * A CoordinatorEventProcessor that directly executes the operations. This is
     * useful in unit tests where execution in threads is not required.
     */
    private static class DirectEventProcessor implements CoordinatorEventProcessor {
        @Override
        public void enqueue(CoordinatorEvent event) throws RejectedExecutionException {
            try {
                event.run();
            } catch (Throwable ex) {
                event.complete(ex);
            }
        }

        @Override
        public void close() throws Exception {}
    }

    /**
     * A CoordinatorEventProcessor that queues event and execute the next one
     * when poll() is called.
     */
    private static class ManualEventProcessor implements CoordinatorEventProcessor {
        private Queue<CoordinatorEvent> queue = new LinkedList<>();

        @Override
        public void enqueue(CoordinatorEvent event) throws RejectedExecutionException {
            queue.add(event);
        }

        public boolean poll() {
            CoordinatorEvent event = queue.poll();
            if (event == null) return false;

            try {
                event.run();
            } catch (Throwable ex) {
                event.complete(ex);
            }

            return true;
        }

        public int size() {
            return queue.size();
        }

        @Override
        public void close() throws Exception {

        }
    }

    /**
     * A CoordinatorLoader that always succeeds.
     */
    private static class MockCoordinatorLoader implements CoordinatorLoader<String> {
        private final LoadSummary summary;
        private final List<Long> lastWrittenOffsets;
        private final List<Long> lastCommittedOffsets;

        public MockCoordinatorLoader(
            LoadSummary summary,
            List<Long> lastWrittenOffsets,
            List<Long> lastCommittedOffsets
        ) {
            this.summary = summary;
            this.lastWrittenOffsets = lastWrittenOffsets;
            this.lastCommittedOffsets = lastCommittedOffsets;
        }

        public MockCoordinatorLoader() {
            this(null, Collections.emptyList(), Collections.emptyList());
        }

        @Override
        public CompletableFuture<LoadSummary> load(
            TopicPartition tp,
            CoordinatorPlayback<String> replayable
        ) {
            lastWrittenOffsets.forEach(replayable::updateLastWrittenOffset);
            lastCommittedOffsets.forEach(replayable::updateLastCommittedOffset);
            return CompletableFuture.completedFuture(summary);
        }

        @Override
        public void close() throws Exception { }
    }

    /**
     * An in-memory partition writer that accepts a maximum number of writes.
     */
    private static class MockPartitionWriter extends InMemoryPartitionWriter<String> {
        private final int maxRecordsInBatch;
        private final boolean failEndMarker;

        public MockPartitionWriter() {
            this(Integer.MAX_VALUE, false);
        }

        public MockPartitionWriter(int maxRecordsInBatch) {
            this(maxRecordsInBatch, false);
        }

        public MockPartitionWriter(boolean failEndMarker) {
            this(Integer.MAX_VALUE, failEndMarker);
        }

        public MockPartitionWriter(int maxRecordsInBatch, boolean failEndMarker) {
            super(false);
            this.maxRecordsInBatch = maxRecordsInBatch;
            this.failEndMarker = failEndMarker;
        }

        @Override
        public void registerListener(TopicPartition tp, Listener listener) {
            super.registerListener(tp, listener);
        }

        @Override
        public void deregisterListener(TopicPartition tp, Listener listener) {
            super.deregisterListener(tp, listener);
        }

        @Override
        public long append(
            TopicPartition tp,
            long producerId,
            short producerEpoch,
            VerificationGuard verificationGuard,
            List<String> records
        ) throws KafkaException {
            if (records.size() <= maxRecordsInBatch) {
                return super.append(
                    tp,
                    producerId,
                    producerEpoch,
                    verificationGuard,
                    records
                );
            } else {
                throw new KafkaException(String.format("Number of records %d greater than the maximum allowed %d.",
                    records.size(), maxRecordsInBatch));
            }
        }

        @Override
        public long appendEndTransactionMarker(
            TopicPartition tp,
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            TransactionResult result
        ) throws KafkaException {
            if (failEndMarker) {
                throw new KafkaException("Can't write end marker.");
            }
            return super.appendEndTransactionMarker(
                tp,
                producerId,
                producerEpoch,
                coordinatorEpoch,
                result
            );
        }
    }

    /**
     * A simple Coordinator implementation that stores the records into a set.
     */
    static class MockCoordinatorShard implements CoordinatorShard<String> {
        private final SnapshotRegistry snapshotRegistry;
        private final TimelineHashSet<String> records;
        private final TimelineHashMap<Long, TimelineHashSet<String>> pendingRecords;
        private final CoordinatorTimer<Void, String> timer;

        MockCoordinatorShard(
            SnapshotRegistry snapshotRegistry,
            CoordinatorTimer<Void, String> timer
        ) {
            this.snapshotRegistry = snapshotRegistry;
            this.records = new TimelineHashSet<>(snapshotRegistry, 0);
            this.pendingRecords = new TimelineHashMap<>(snapshotRegistry, 0);
            this.timer = timer;
        }

        @Override
        public void replay(
            long offset,
            long producerId,
            short producerEpoch,
            String record
        ) throws RuntimeException {
            if (producerId == RecordBatch.NO_PRODUCER_ID) {
                records.add(record);
            } else {
                pendingRecords
                    .computeIfAbsent(producerId, __ -> new TimelineHashSet<>(snapshotRegistry, 0))
                    .add(record);
            }
        }

        @Override
        public void replayEndTransactionMarker(
            long producerId,
            short producerEpoch,
            TransactionResult result
        ) throws RuntimeException {
            if (result == TransactionResult.COMMIT) {
                TimelineHashSet<String> pending = pendingRecords.remove(producerId);
                if (pending == null) return;
                records.addAll(pending);
            } else {
                pendingRecords.remove(producerId);
            }
        }

        Set<String> pendingRecords(long producerId) {
            TimelineHashSet<String> pending = pendingRecords.get(producerId);
            if (pending == null) return Collections.emptySet();
            return Collections.unmodifiableSet(new HashSet<>(pending));
        }

        Set<String> records() {
            return Collections.unmodifiableSet(new HashSet<>(records));
        }

        CoordinatorTimer<Void, String> timer() {
            return timer;
        }
    }

    /**
     * A CoordinatorBuilder that creates a MockCoordinator.
     */
    private static class MockCoordinatorShardBuilder implements CoordinatorShardBuilder<MockCoordinatorShard, String> {
        private SnapshotRegistry snapshotRegistry;
        private CoordinatorTimer<Void, String> timer;

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withSnapshotRegistry(
            SnapshotRegistry snapshotRegistry
        ) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withLogContext(
            LogContext logContext
        ) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withTime(
            Time time
        ) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withTimer(
            CoordinatorTimer<Void, String> timer
        ) {
            this.timer = timer;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            return this;
        }

        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withTopicPartition(
            TopicPartition topicPartition
        ) {
            return this;
        }

        @Override
        public MockCoordinatorShard build() {
            return new MockCoordinatorShard(
                Objects.requireNonNull(this.snapshotRegistry),
                Objects.requireNonNull(this.timer)
            );
        }
    }

    /**
     * A CoordinatorBuilderSupplier that returns a MockCoordinatorBuilder.
     */
    private static class MockCoordinatorShardBuilderSupplier implements CoordinatorShardBuilderSupplier<MockCoordinatorShard, String> {
        @Override
        public CoordinatorShardBuilder<MockCoordinatorShard, String> get() {
            return new MockCoordinatorShardBuilder();
        }
    }

    @Test
    public void testScheduleLoading() {
        MockTimer timer = new MockTimer();
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);

        // Getting the coordinator context fails because the coordinator
        // does not exist until scheduleLoadOperation is called.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the coordinator context succeeds now.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // The coordinator is loading.
        assertEquals(LOADING, ctx.state);
        assertEquals(0, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // When the loading completes, the coordinator transitions to active.
        future.complete(null);
        assertEquals(ACTIVE, ctx.state);

        // Verify that onLoaded is called.
        verify(coordinator, times(1)).onLoaded(MetadataImage.EMPTY);

        // Verify that the listener is registered.
        verify(writer, times(1)).registerListener(
            eq(TP),
            any(PartitionWriter.Listener.class)
        );

        // Verify that the builder got all the expected objects.
        verify(builder, times(1)).withSnapshotRegistry(eq(ctx.coordinator.snapshotRegistry()));
        verify(builder, times(1)).withLogContext(eq(ctx.logContext));
        verify(builder, times(1)).withTime(eq(timer.time()));
        verify(builder, times(1)).withTimer(eq(ctx.timer));
    }

    @Test
    public void testScheduleLoadingWithFailure() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(0, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // When the loading fails, the coordinator transitions to failed.
        future.completeExceptionally(new Exception("failure"));
        assertEquals(FAILED, ctx.state);

        // Verify that onUnloaded is called.
        verify(coordinator, times(1)).onUnloaded();

        // Verify that the listener is deregistered.
        verify(writer, times(1)).deregisterListener(
            eq(TP),
            any(PartitionWriter.Listener.class)
        );
    }

    @Test
    public void testScheduleLoadingWithStalePartitionEpoch() {
        MockTimer timer = new MockTimer();
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(10, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // When the loading completes, the coordinator transitions to active.
        future.complete(null);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);

        // Loading with a previous epoch is a no-op. The coordinator stays
        // in active state with the correct epoch.
        runtime.scheduleLoadOperation(TP, 0);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);
    }

    @Test
    public void testScheduleLoadingAfterLoadingFailure() {
        MockTimer timer = new MockTimer();
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(10, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // When the loading fails, the coordinator transitions to failed.
        future.completeExceptionally(new Exception("failure"));
        assertEquals(FAILED, ctx.state);

        // Verify that onUnloaded is called.
        verify(coordinator, times(1)).onUnloaded();

        // Create a new coordinator.
        coordinator = mock(MockCoordinatorShard.class);
        when(builder.build()).thenReturn(coordinator);

        // Schedule the reloading.
        future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);
        runtime.scheduleLoadOperation(TP, 11);

        // Getting the context succeeds and the coordinator should be in loading.
        ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(11, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator.coordinator());

        // Complete the loading.
        future.complete(null);

        // Verify the state.
        assertEquals(ACTIVE, ctx.state);
    }

    @Test
    public void testScheduleUnloading() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Loads the coordinator. It directly transitions to active.
        runtime.scheduleLoadOperation(TP, 10);
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);

        // Schedule the unloading.
        runtime.scheduleUnloadOperation(TP, OptionalInt.of(ctx.epoch + 1));
        assertEquals(CLOSED, ctx.state);

        // Verify that onUnloaded is called.
        verify(coordinator, times(1)).onUnloaded();

        // Verify that the listener is deregistered.
        verify(writer, times(1)).deregisterListener(
            eq(TP),
            any(PartitionWriter.Listener.class)
        );

        // Getting the coordinator context fails because it no longer exists.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));
    }

    @Test
    public void testScheduleUnloadingWithEmptyEpoch() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Loads the coordinator. It directly transitions to active.
        runtime.scheduleLoadOperation(TP, 10);
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);

        // Schedule the unloading.
        runtime.scheduleUnloadOperation(TP, OptionalInt.empty());
        assertEquals(CLOSED, ctx.state);

        // Verify that onUnloaded is called.
        verify(coordinator, times(1)).onUnloaded();

        // Verify that the listener is deregistered.
        verify(writer, times(1)).deregisterListener(
            eq(TP),
            any(PartitionWriter.Listener.class)
        );

        // Getting the coordinator context fails because it no longer exists.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));
    }

    @Test
    public void testScheduleUnloadingWhenContextDoesntExist() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // No loading is scheduled. This is to check the case when a follower that was never a coordinator
        // is asked to unload its state. The unload event is skipped in this case.

        // Schedule the unloading.
        runtime.scheduleUnloadOperation(TP, OptionalInt.of(11));

        // Verify that onUnloaded is not called.
        verify(coordinator, times(0)).onUnloaded();

        // Getting the coordinator context fails because it doesn't exist.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));
    }

    @Test
    public void testScheduleUnloadingWithStalePartitionEpoch() {
        MockTimer timer = new MockTimer();
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Loads the coordinator. It directly transitions to active.
        runtime.scheduleLoadOperation(TP, 10);
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);

        // Unloading with a previous epoch is a no-op. The coordinator stays
        // in active with the correct epoch.
        runtime.scheduleUnloadOperation(TP, OptionalInt.of(0));
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);
    }

    @Test
    public void testScheduleWriteOp() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1")
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // The last written offset is updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().records());
        // Records have been written to the log.
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value("record1"),
            InMemoryPartitionWriter.LogEntry.value("record2")
        ), writer.entries(TP));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record3"), "response2"));

        // Verify that the write is not committed yet.
        assertFalse(write2.isDone());

        // The last written offset is updated.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(mkSet("record1", "record2", "record3"), ctx.coordinator.coordinator().records());
        // Records have been written to the log.
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value("record1"),
            InMemoryPartitionWriter.LogEntry.value("record2"),
            InMemoryPartitionWriter.LogEntry.value("record3")
        ), writer.entries(TP));

        // Write #3 but without any records.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response3"));

        // Verify that the write is not committed yet.
        assertFalse(write3.isDone());

        // The state does not change.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2", "record3"), ctx.coordinator.coordinator().records());
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value("record1"),
            InMemoryPartitionWriter.LogEntry.value("record2"),
            InMemoryPartitionWriter.LogEntry.value("record3")
        ), writer.entries(TP));

        // Commit write #1.
        writer.commit(TP, 2);

        // The write is completed.
        assertTrue(write1.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));

        // The last committed offset is updated.
        assertEquals(2L, ctx.coordinator.lastCommittedOffset());
        // The snapshot is cleaned up.
        assertEquals(Arrays.asList(2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());

        // Commit write #2.
        writer.commit(TP, 3);

        // The writes are completed.
        assertTrue(write2.isDone());
        assertTrue(write3.isDone());
        assertEquals("response2", write2.get(5, TimeUnit.SECONDS));
        assertEquals("response3", write3.get(5, TimeUnit.SECONDS));

        // The last committed offset is updated.
        assertEquals(3L, ctx.coordinator.lastCommittedOffset());
        // The snapshot is cleaned up.
        assertEquals(Collections.singletonList(3L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #4 but without records.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response4"));

        // It is completed immediately because the state is fully committed.
        assertTrue(write4.isDone());
        assertEquals("response4", write4.get(5, TimeUnit.SECONDS));
        assertEquals(Collections.singletonList(3L), ctx.coordinator.snapshotRegistry().epochsList());
    }

    @Test
    public void testScheduleWriteOpWhenInactive() {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Scheduling a write fails with a NotCoordinatorException because the coordinator
        // does not exist.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response1"));
        assertFutureThrows(write, NotCoordinatorException.class);
    }

    @Test
    public void testScheduleWriteOpWhenOpFails() {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Scheduling a write that fails when the operation is called. The exception
        // is used to complete the future.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP, DEFAULT_WRITE_TIMEOUT, state -> {
            throw new KafkaException("error");
        });
        assertFutureThrows(write, KafkaException.class);
    }

    @Test
    public void testScheduleWriteOpWhenReplayFails() {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Override the coordinator with a coordinator that throws
        // an exception when replay is called.
        SnapshotRegistry snapshotRegistry = ctx.coordinator.snapshotRegistry();
        ctx.coordinator = new SnapshottableCoordinator<>(
            new LogContext(),
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, ctx.timer) {
                @Override
                public void replay(
                    long offset,
                    long producerId,
                    short producerEpoch,
                    String record
                ) throws RuntimeException {
                    throw new IllegalArgumentException("error");
                }
            },
            TP
        );

        // Write. It should fail.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));
        assertFutureThrows(write, IllegalArgumentException.class);

        // Verify that the state has not changed.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());
    }

    @Test
    public void testScheduleWriteOpWhenWriteFails() {
        MockTimer timer = new MockTimer();
        // The partition writer only accept on write.
        MockPartitionWriter writer = new MockPartitionWriter(2);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().records());

        // Write #2. It should fail because the writer is configured to only
        // accept 2 records per batch.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4", "record5"), "response2"));
        assertFutureThrows(write2, KafkaException.class);

        // Verify that the state has not changed.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().records());
    }

    @Test
    public void testScheduleWriteOpWhenWriteTimesOut() throws InterruptedException {
        MockTimer timer = new MockTimer();
        // The partition writer only accept on write.
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. We should get a TimeoutException because the HWM will not advance.
        CompletableFuture<String> timedOutWrite = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(3),
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        timer.advanceClock(4);

        assertFutureThrows(timedOutWrite, org.apache.kafka.common.errors.TimeoutException.class);
    }

    @Test
    public void testScheduleWriteAllOperation() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        TopicPartition coordinator0 = new TopicPartition("__consumer_offsets", 0);
        TopicPartition coordinator1 = new TopicPartition("__consumer_offsets", 1);
        TopicPartition coordinator2 = new TopicPartition("__consumer_offsets", 2);

        // Load coordinators.
        runtime.scheduleLoadOperation(coordinator0, 10);
        runtime.scheduleLoadOperation(coordinator1, 10);
        runtime.scheduleLoadOperation(coordinator2, 10);

        // Writes.
        AtomicInteger cnt = new AtomicInteger(0);
        List<CompletableFuture<List<String>>> writes = runtime.scheduleWriteAllOperation("write", DEFAULT_WRITE_TIMEOUT, state -> {
            int counter = cnt.getAndIncrement();
            return new CoordinatorResult<>(
                Collections.singletonList("record#" + counter),
                Collections.singletonList("response#" + counter)
            );
        });

        assertEquals(1L, runtime.contextOrThrow(coordinator0).coordinator.lastWrittenOffset());
        assertEquals(1L, runtime.contextOrThrow(coordinator1).coordinator.lastWrittenOffset());
        assertEquals(1L, runtime.contextOrThrow(coordinator2).coordinator.lastWrittenOffset());

        assertEquals(Collections.singletonList(InMemoryPartitionWriter.LogEntry.value("record#0")), writer.entries(coordinator0));
        assertEquals(Collections.singletonList(InMemoryPartitionWriter.LogEntry.value("record#1")), writer.entries(coordinator1));
        assertEquals(Collections.singletonList(InMemoryPartitionWriter.LogEntry.value("record#2")), writer.entries(coordinator2));

        // Commit.
        writer.commit(coordinator0);
        writer.commit(coordinator1);
        writer.commit(coordinator2);

        // Verify.
        assertEquals(
            Arrays.asList("response#0", "response#1", "response#2"),
            FutureUtils.combineFutures(writes, ArrayList::new, List::addAll).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testScheduleTransactionalWriteOp() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        MockCoordinatorShardBuilder shardBuilder = new MockCoordinatorShardBuilder() {
            @Override
            public MockCoordinatorShard build() {
                return coordinator;
            }
        };
        MockCoordinatorShardBuilderSupplier shardBuilderSupplier = new MockCoordinatorShardBuilderSupplier() {
            @Override
            public CoordinatorShardBuilder<MockCoordinatorShard, String> get() {
                return shardBuilder;
            }
        };

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(shardBuilderSupplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify that the listener was registered.
        verify(writer, times(1)).registerListener(eq(TP), any());

        // Prepare the transaction verification.
        VerificationGuard guard = new VerificationGuard();
        when(writer.maybeStartTransactionVerification(
            TP,
            "transactional-id",
            100L,
            (short) 50
        )).thenReturn(CompletableFuture.completedFuture(guard));

        // Schedule a transactional write.
        runtime.scheduleTransactionalWriteOperation(
            "tnx-write",
            TP,
            "transactional-id",
            100L,
            (short) 50,
            Duration.ofMillis(5000),
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response")
        );

        // Verify that the writer got the records with the correct
        // producer id and producer epoch.
        verify(writer, times(1)).append(
            eq(TP),
            eq(100L),
            eq((short) 50),
            eq(guard),
            eq(Arrays.asList("record1", "record2"))
        );

        // Verify that the coordinator got the records with the correct
        // producer id and producer epoch.
        verify(coordinator, times(1)).replay(
            eq(0L),
            eq(100L),
            eq((short) 50),
            eq("record1")
        );
        verify(coordinator, times(1)).replay(
            eq(1L),
            eq(100L),
            eq((short) 50),
            eq("record2")
        );
    }

    @Test
    public void testScheduleTransactionalWriteOpWhenVerificationFails() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        MockCoordinatorShardBuilder shardBuilder = new MockCoordinatorShardBuilder() {
            @Override
            public MockCoordinatorShard build() {
                return coordinator;
            }
        };
        MockCoordinatorShardBuilderSupplier shardBuilderSupplier = new MockCoordinatorShardBuilderSupplier() {
            @Override
            public CoordinatorShardBuilder<MockCoordinatorShard, String> get() {
                return shardBuilder;
            }
        };

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(shardBuilderSupplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify that the listener was registered.
        verify(writer, times(1)).registerListener(eq(TP), any());

        // Fail the transaction verification.
        when(writer.maybeStartTransactionVerification(
            TP,
            "transactional-id",
            100L,
            (short) 50
        )).thenReturn(FutureUtils.failedFuture(Errors.NOT_ENOUGH_REPLICAS.exception()));

        // Schedule a transactional write.
        CompletableFuture<String> future = runtime.scheduleTransactionalWriteOperation(
            "tnx-write",
            TP,
            "transactional-id",
            100L,
            (short) 50,
            Duration.ofMillis(5000),
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response")
        );

        // Verify that the future is failed with the expected exception.
        assertFutureThrows(future, NotEnoughReplicasException.class);

        // Verify that the writer is not called.
        verify(writer, times(0)).append(
            any(),
            anyLong(),
            anyShort(),
            any(),
            any()
        );
    }

    @ParameterizedTest
    @EnumSource(value = TransactionResult.class)
    public void testScheduleTransactionCompletion(TransactionResult result) throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Transactional write #1.
        CompletableFuture<String> write1 = runtime.scheduleTransactionalWriteOperation(
            "write#1",
            TP,
            "transactional-id",
            100L,
            (short) 5,
            DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1")
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // The last written offset is updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator. They are stored in
        // the pending set for now.
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(
            100L
        ));
        // Records have been written to the log.
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record1"),
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record2")
        ), writer.entries(TP));

        // Complete transaction #1.
        CompletableFuture<Void> complete1 = runtime.scheduleTransactionCompletion(
            "complete#1",
            TP,
            100L,
            (short) 5,
            10,
            result,
            DEFAULT_WRITE_TIMEOUT
        );

        // Verify that the completion is not committed yet.
        assertFalse(complete1.isDone());

        // The last written offset is updated.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        if (result == TransactionResult.COMMIT) {
            // They are now in the records set if committed.
            assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().records());
        } else {
            // Or they are gone if aborted.
            assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        }

        // Records have been written to the log.
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record1"),
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record2"),
            InMemoryPartitionWriter.LogEntry.control(100L, (short) 5, 10, result)
        ), writer.entries(TP));

        // Commit write #1.
        writer.commit(TP, 2);

        // The write is completed.
        assertTrue(write1.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));

        // Commit completion #1.
        writer.commit(TP, 3);

        // The transaction is completed.
        assertTrue(complete1.isDone());
        assertNull(complete1.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleTransactionCompletionWhenWriteTimesOut() throws InterruptedException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Complete #1. We should get a TimeoutException because the HWM will not advance.
        CompletableFuture<Void> timedOutCompletion = runtime.scheduleTransactionCompletion(
            "complete#1",
            TP,
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            Duration.ofMillis(3)
        );

        // Verify that the state has been updated.
        assertEquals(1L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 1L), ctx.coordinator.snapshotRegistry().epochsList());

        // Advance clock to timeout Complete #1.
        timer.advanceClock(4);

        assertFutureThrows(timedOutCompletion, org.apache.kafka.common.errors.TimeoutException.class);

        // Verify that the state is still the same. We don't revert when the
        // operation timeouts because the record has been written to the log.
        assertEquals(1L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 1L), ctx.coordinator.snapshotRegistry().epochsList());
    }

    @Test
    public void testScheduleTransactionCompletionWhenWriteFails() {
        MockTimer timer = new MockTimer();
        // The partition writer accepts records but fails on markers.
        MockPartitionWriter writer = new MockPartitionWriter(true);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleTransactionalWriteOperation(
            "write#1",
            TP,
            "transactional-id",
            100L,
            (short) 5,
            DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());

        // Complete transaction #1. It should fail.
        CompletableFuture<Void> complete1 = runtime.scheduleTransactionCompletion(
            "complete#1",
            TP,
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            DEFAULT_WRITE_TIMEOUT
        );
        assertFutureThrows(complete1, KafkaException.class);

        // Verify that the state has not changed.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
    }

    @Test
    public void testScheduleTransactionCompletionWhenReplayFails() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Collections.singletonList(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Override the coordinator with a coordinator that throws
        // an exception when replayEndTransactionMarker is called.
        SnapshotRegistry snapshotRegistry = ctx.coordinator.snapshotRegistry();
        ctx.coordinator = new SnapshottableCoordinator<>(
            new LogContext(),
            snapshotRegistry,
            new MockCoordinatorShard(snapshotRegistry, ctx.timer) {
                @Override
                public void replayEndTransactionMarker(
                    long producerId,
                    short producerEpoch,
                    TransactionResult result
                ) throws RuntimeException {
                    throw new IllegalArgumentException("error");
                }
            },
            TP
        );

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleTransactionalWriteOperation(
            "write#1",
            TP,
            "transactional-id",
            100L,
            (short) 5,
            DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record1"),
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record2")
        ), writer.entries(TP));

        // Complete transaction #1. It should fail.
        CompletableFuture<Void> complete1 = runtime.scheduleTransactionCompletion(
            "complete#1",
            TP,
            100L,
            (short) 5,
            10,
            TransactionResult.COMMIT,
            DEFAULT_WRITE_TIMEOUT
        );
        assertFutureThrows(complete1, IllegalArgumentException.class);

        // Verify that the state has not changed.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(Arrays.asList(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        assertEquals(Arrays.asList(
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record1"),
            InMemoryPartitionWriter.LogEntry.value(100L, (short) 5, "record2")
        ), writer.entries(TP));
    }

    @Test
    public void testScheduleReadOp() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Commit write #1.
        writer.commit(TP, 2);

        // Write #1 is completed.
        assertTrue(write1.isDone());

        // Write #2 is not.
        assertFalse(write2.isDone());

        // The last written and committed offsets are updated.
        assertEquals(4, ctx.coordinator.lastWrittenOffset());
        assertEquals(2, ctx.coordinator.lastCommittedOffset());

        // Read.
        CompletableFuture<String> read = runtime.scheduleReadOperation("read", TP, (state, offset) -> {
            // The read operation should be given the last committed offset.
            assertEquals(ctx.coordinator.lastCommittedOffset(), offset);
            return "read-response";
        });

        // The read is completed immediately.
        assertTrue(read.isDone());
        assertEquals("read-response", read.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleReadOpWhenPartitionInactive() {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Schedule a read. It fails because the coordinator does not exist.
        CompletableFuture<String> read = runtime.scheduleReadOperation("read", TP,
            (state, offset) -> "read-response");
        assertFutureThrows(read, NotCoordinatorException.class);
    }

    @Test
    public void testScheduleReadOpWhenOpsFails() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Commit write #1.
        writer.commit(TP, 2);

        // Read. It fails with an exception that is used to complete the future.
        CompletableFuture<String> read = runtime.scheduleReadOperation("read", TP, (state, offset) -> {
            assertEquals(ctx.coordinator.lastCommittedOffset(), offset);
            throw new IllegalArgumentException("error");
        });
        assertFutureThrows(read, IllegalArgumentException.class);
    }

    @Test
    public void testScheduleReadAllOp() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        TopicPartition coordinator0 = new TopicPartition("__consumer_offsets", 0);
        TopicPartition coordinator1 = new TopicPartition("__consumer_offsets", 1);
        TopicPartition coordinator2 = new TopicPartition("__consumer_offsets", 2);

        // Loads the coordinators.
        runtime.scheduleLoadOperation(coordinator0, 10);
        runtime.scheduleLoadOperation(coordinator1, 10);
        runtime.scheduleLoadOperation(coordinator2, 10);

        // Writes
        runtime.scheduleWriteOperation("write#0", coordinator0, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.singletonList("record0"), "response0"));
        runtime.scheduleWriteOperation("write#1", coordinator1, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.singletonList("record1"), "response1"));
        runtime.scheduleWriteOperation("write#2", coordinator2, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.singletonList("record2"), "response2"));

        // Commit writes.
        writer.commit(coordinator0);
        writer.commit(coordinator1);
        writer.commit(coordinator2);

        // Read.
        List<CompletableFuture<List<String>>> responses = runtime.scheduleReadAllOperation(
            "read",
            (state, offset) -> new ArrayList<>(state.records)
        );

        assertEquals(
            Arrays.asList("record0", "record1", "record2"),
            FutureUtils.combineFutures(responses, ArrayList::new, List::addAll).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testClose() throws Exception {
        MockCoordinatorLoader loader = spy(new MockCoordinatorLoader());
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Writes are inflight.
        assertFalse(write1.isDone());
        assertFalse(write2.isDone());

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());

        // Timer #1. This is never executed.
        ctx.timer.schedule("timer-1", 10, TimeUnit.SECONDS, true,
            () -> new CoordinatorResult<>(Arrays.asList("record5", "record6"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Close the runtime.
        runtime.close();

        // All the pending operations are completed with NotCoordinatorException.
        assertFutureThrows(write1, NotCoordinatorException.class);
        assertFutureThrows(write2, NotCoordinatorException.class);

        // Verify that the loader was closed.
        verify(loader).close();

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testOnNewMetadataImage() {
        TopicPartition tp0 = new TopicPartition("__consumer_offsets", 0);
        TopicPartition tp1 = new TopicPartition("__consumer_offsets", 1);

        MockTimer timer = new MockTimer();
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        MockCoordinatorShard coordinator0 = mock(MockCoordinatorShard.class);
        MockCoordinatorShard coordinator1 = mock(MockCoordinatorShard.class);

        when(supplier.get()).thenReturn(builder);
        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.build())
            .thenReturn(coordinator0)
            .thenReturn(coordinator1);

        CompletableFuture<CoordinatorLoader.LoadSummary> future0 = new CompletableFuture<>();
        when(loader.load(eq(tp0), argThat(coordinatorMatcher(runtime, tp0)))).thenReturn(future0);

        CompletableFuture<CoordinatorLoader.LoadSummary> future1 = new CompletableFuture<>();
        when(loader.load(eq(tp1), argThat(coordinatorMatcher(runtime, tp1)))).thenReturn(future1);

        runtime.scheduleLoadOperation(tp0, 0);
        runtime.scheduleLoadOperation(tp1, 0);

        assertEquals(coordinator0, runtime.contextOrThrow(tp0).coordinator.coordinator());
        assertEquals(coordinator1, runtime.contextOrThrow(tp1).coordinator.coordinator());

        // Coordinator 0 is loaded. It should get the current image
        // that is the empty one.
        future0.complete(null);
        verify(coordinator0).onLoaded(MetadataImage.EMPTY);

        // Publish a new image.
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        MetadataImage newImage = delta.apply(MetadataProvenance.EMPTY);
        runtime.onNewMetadataImage(newImage, delta);

        // Coordinator 0 should be notified about it.
        verify(coordinator0).onNewMetadataImage(newImage, delta);

        // Coordinator 1 is loaded. It should get the current image
        // that is the new image.
        future1.complete(null);
        verify(coordinator1).onLoaded(newImage);
    }

    @Test
    public void testScheduleTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(30))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), null));

        // Timer #2.
        ctx.timer.schedule("timer-2", 20, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), null));

        // The coordinator timer should have two pending tasks.
        assertEquals(2, ctx.timer.size());

        // Advance time to fire timer #1,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.coordinator().records());
        assertEquals(1, ctx.timer.size());

        // Advance time to fire timer #2,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(mkSet("record1", "record2", "record3", "record4"), ctx.coordinator.coordinator().records());
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testRescheduleTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Poll twice to process the pending events related to the loading.
        processor.poll();
        processor.poll();

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // The processor should be empty.
        assertEquals(0, processor.size());

        // Timer #1.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record2"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Schedule a third timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record3"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // Another event should be waiting in the processor.
        assertEquals(2, processor.size());

        // Poll twice to execute the two pending events.
        assertTrue(processor.poll());
        assertTrue(processor.poll());

        // Verify that the correct operation was executed. Only the third
        // instance should have been executed here.
        assertEquals(mkSet("record3"), ctx.coordinator.coordinator().records());
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testCancelTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Poll twice to process the pending events related to the loading.
        processor.poll();
        processor.poll();

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // The processor should be empty.
        assertEquals(0, processor.size());

        // Timer #1.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record2"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Cancel the timer.
        ctx.timer.cancel("timer-1");

        // The coordinator timer have no pending timers.
        assertEquals(0, ctx.timer.size());

        // Advance time to fire the cancelled timer.
        timer.advanceClock(10 + 1);

        // No new event expected because the timer was cancelled before
        // it expired.
        assertEquals(1, processor.size());

        // Poll to execute the pending event.
        assertTrue(processor.poll());

        // Verify that no operation was executed.
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testRetryableTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true, () -> {
            cnt.incrementAndGet();
            throw new KafkaException("error");
        });

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(1, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the retry backoff.
        timer.advanceClock(500 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(2, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the retry backoff.
        timer.advanceClock(500 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(3, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Cancel Timer #1.
        ctx.timer.cancel("timer-1");
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testRetryableTimerWithCustomBackoff() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true, 1000, () -> {
            cnt.incrementAndGet();
            throw new KafkaException("error");
        });

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(1, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the default retry backoff.
        timer.advanceClock(500 + 1);

        // The timer should not have been called yet.
        assertEquals(1, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the custom retry.
        timer.advanceClock(500 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(2, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the default retry backoff.
        timer.advanceClock(500 + 1);

        // The timer should not have been called yet.
        assertEquals(2, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Advance past the custom retry.
        timer.advanceClock(500 + 1);

        // The timer should have been called and the timer should have one pending task.
        assertEquals(3, cnt.get());
        assertEquals(1, ctx.timer.size());

        // Cancel Timer #1.
        ctx.timer.cancel("timer-1");
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testNonRetryableTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, false, () -> {
            cnt.incrementAndGet();
            throw new KafkaException("error");
        });

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // The timer should have been called and the timer should have no pending tasks
        // because the timer is not retried.
        assertEquals(1, cnt.get());
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testStateChanges() throws Exception {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorLoader loader = mock(MockCoordinatorLoader.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        GroupCoordinatorRuntimeMetrics runtimeMetrics = mock(GroupCoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(loader)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(eq(TP), argThat(coordinatorMatcher(runtime, TP)))).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        verify(runtimeMetrics, times(1)).recordPartitionStateChange(INITIAL, LOADING);

        // When the loading fails, the coordinator transitions to failed.
        future.completeExceptionally(new Exception("failure"));
        assertEquals(FAILED, ctx.state);
        verify(runtimeMetrics, times(1)).recordPartitionStateChange(LOADING, FAILED);

        // Start loading a new topic partition.
        TopicPartition tp = new TopicPartition("__consumer_offsets", 1);
        future = new CompletableFuture<>();
        when(loader.load(eq(tp), argThat(coordinatorMatcher(runtime, tp)))).thenReturn(future);
        // Schedule the loading.
        runtime.scheduleLoadOperation(tp, 0);
        // Getting the context succeeds and the coordinator should be in loading.
        ctx = runtime.contextOrThrow(tp);
        assertEquals(coordinator, ctx.coordinator.coordinator());
        assertEquals(LOADING, ctx.state);
        verify(runtimeMetrics, times(2)).recordPartitionStateChange(INITIAL, LOADING);

        // When the loading completes, the coordinator transitions to active.
        future.complete(null);
        assertEquals(ACTIVE, ctx.state);
        verify(runtimeMetrics, times(1)).recordPartitionStateChange(LOADING, ACTIVE);

        runtime.close();
        verify(runtimeMetrics, times(1)).recordPartitionStateChange(FAILED, CLOSED);
        verify(runtimeMetrics, times(1)).recordPartitionStateChange(ACTIVE, CLOSED);
    }

    @Test
    public void testPartitionLoadSensor() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        GroupCoordinatorRuntimeMetrics runtimeMetrics = mock(GroupCoordinatorRuntimeMetrics.class);

        long startTimeMs = timer.time().milliseconds();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader(
                    new CoordinatorLoader.LoadSummary(
                        startTimeMs,
                        startTimeMs + 1000,
                        startTimeMs + 500,
                        30,
                        3000),
                    Collections.emptyList(),
                    Collections.emptyList()))
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Getting the coordinator context fails because the coordinator
        // does not exist until scheduleLoadOperation is called.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the coordinator context succeeds now.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);

        // When the loading completes, the coordinator transitions to active.
        assertEquals(ACTIVE, ctx.state);

        verify(runtimeMetrics, times(1)).recordPartitionLoadSensor(startTimeMs, startTimeMs + 1000);
    }

    @Test
    public void testPartitionLoadGeneratesSnapshotAtHighWatermark() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        GroupCoordinatorRuntimeMetrics runtimeMetrics = mock(GroupCoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(Time.SYSTEM)
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader(
                    new CoordinatorLoader.LoadSummary(
                        1000,
                        2000,
                        1500,
                        30,
                        3000),
                    Arrays.asList(5L, 15L, 27L),
                    Arrays.asList(5L, 15L)))
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the coordinator context succeeds now.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);

        // When the loading completes, the coordinator transitions to active.
        assertEquals(ACTIVE, ctx.state);

        assertEquals(27L, ctx.coordinator.lastWrittenOffset());
        assertEquals(15L, ctx.coordinator.lastCommittedOffset());
        assertFalse(ctx.coordinator.snapshotRegistry().hasSnapshot(0L));
        assertFalse(ctx.coordinator.snapshotRegistry().hasSnapshot(5L));
        assertTrue(ctx.coordinator.snapshotRegistry().hasSnapshot(15L));
        assertTrue(ctx.coordinator.snapshotRegistry().hasSnapshot(27L));
    }

    @Test
    public void testPartitionLoadGeneratesSnapshotAtHighWatermarkNoRecordsLoaded() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = mock(MockPartitionWriter.class);
        MockCoordinatorShardBuilderSupplier supplier = mock(MockCoordinatorShardBuilderSupplier.class);
        MockCoordinatorShardBuilder builder = mock(MockCoordinatorShardBuilder.class);
        MockCoordinatorShard coordinator = mock(MockCoordinatorShard.class);
        GroupCoordinatorRuntimeMetrics runtimeMetrics = mock(GroupCoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(Time.SYSTEM)
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader(
                    new CoordinatorLoader.LoadSummary(
                        1000,
                        2000,
                        1500,
                        30,
                        3000),
                    Collections.emptyList(),
                    Collections.emptyList()))
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(GroupCoordinatorMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the coordinator context succeeds now.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);

        // When the loading completes, the coordinator transitions to active.
        assertEquals(ACTIVE, ctx.state);

        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertTrue(ctx.coordinator.snapshotRegistry().hasSnapshot(0L));
    }

    private static <S extends CoordinatorShard<U>, U> ArgumentMatcher<CoordinatorPlayback<U>> coordinatorMatcher(
        CoordinatorRuntime<S, U> runtime,
        TopicPartition tp
    ) {
        return c -> c.equals(runtime.contextOrThrow(tp).coordinator);
    }
}
