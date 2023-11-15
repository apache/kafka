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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorRuntimeMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorRuntimeTest {
    private static final TopicPartition TP = new TopicPartition("__consumer_offsets", 0);

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

        public MockCoordinatorLoader(LoadSummary summary) {
            this.summary = summary;
        }

        public MockCoordinatorLoader() {
            this(null);
        }

        @Override
        public CompletableFuture<LoadSummary> load(TopicPartition tp, CoordinatorPlayback<String> replayable) {
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

        public MockPartitionWriter() {
            this(Integer.MAX_VALUE);
        }

        public MockPartitionWriter(int maxRecordsInBatch) {
            super(false);
            this.maxRecordsInBatch = maxRecordsInBatch;
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
        public long append(TopicPartition tp, List<String> records) throws KafkaException {
            if (records.size() <= maxRecordsInBatch) {
                return super.append(tp, records);
            } else {
                throw new KafkaException(String.format("Number of records %d greater than the maximum allowed %d.",
                    records.size(), maxRecordsInBatch));
            }
        }
    }

    /**
     * A simple Coordinator implementation that stores the records into a set.
     */
    private static class MockCoordinatorShard implements CoordinatorShard<String> {
        private final TimelineHashSet<String> records;
        private final CoordinatorTimer<Void, String> timer;

        MockCoordinatorShard(
            SnapshotRegistry snapshotRegistry,
            CoordinatorTimer<Void, String> timer
        ) {
            this.records = new TimelineHashSet<>(snapshotRegistry, 0);
            this.timer = timer;
        }

        @Override
        public void replay(String record) throws RuntimeException {
            records.add(record);
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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(TP, coordinator)).thenReturn(future);

        // Getting the coordinator context fails because the coordinator
        // does not exist until scheduleLoadOperation is called.
        assertThrows(NotCoordinatorException.class, () -> runtime.contextOrThrow(TP));

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the coordinator context succeeds now.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);

        // The coordinator is loading.
        assertEquals(LOADING, ctx.state);
        assertEquals(0, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator);

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
        verify(builder, times(1)).withSnapshotRegistry(eq(ctx.snapshotRegistry));
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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(TP, coordinator)).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 0);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(0, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator);

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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(TP, coordinator)).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(10, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator);

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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(TP, coordinator)).thenReturn(future);

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Getting the context succeeds and the coordinator should be in loading.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(10, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator);

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
        when(loader.load(TP, coordinator)).thenReturn(future);
        runtime.scheduleLoadOperation(TP, 11);

        // Getting the context succeeds and the coordinator should be in loading.
        ctx = runtime.contextOrThrow(TP);
        assertEquals(LOADING, ctx.state);
        assertEquals(11, ctx.epoch);
        assertEquals(coordinator, ctx.coordinator);

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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);

        // Loads the coordinator. It directly transitions to active.
        runtime.scheduleLoadOperation(TP, 10);
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(10, ctx.epoch);

        // Schedule the unloading.
        runtime.scheduleUnloadOperation(TP, ctx.epoch + 1);
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
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
        runtime.scheduleUnloadOperation(TP, 11);

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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
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
        runtime.scheduleUnloadOperation(TP, 0);
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Collections.singletonList(0L), ctx.snapshotRegistry.epochsList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // The last written offset is updated.
        assertEquals(2L, ctx.lastWrittenOffset);
        // The last committed offset does not change.
        assertEquals(0L, ctx.lastCommittedOffset);
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L), ctx.snapshotRegistry.epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.records());
        // Records have been written to the log.
        assertEquals(Arrays.asList("record1", "record2"), writer.records(TP));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record3"), "response2"));

        // Verify that the write is not committed yet.
        assertFalse(write2.isDone());

        // The last written offset is updated.
        assertEquals(3L, ctx.lastWrittenOffset);
        // The last committed offset does not change.
        assertEquals(0L, ctx.lastCommittedOffset);
        // A new snapshot is created.
        assertEquals(Arrays.asList(0L, 2L, 3L), ctx.snapshotRegistry.epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(mkSet("record1", "record2", "record3"), ctx.coordinator.records());
        // Records have been written to the log.
        assertEquals(Arrays.asList("record1", "record2", "record3"), writer.records(TP));

        // Write #3 but without any records.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response3"));

        // Verify that the write is not committed yet.
        assertFalse(write3.isDone());

        // The state does not change.
        assertEquals(3L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Arrays.asList(0L, 2L, 3L), ctx.snapshotRegistry.epochsList());
        assertEquals(mkSet("record1", "record2", "record3"), ctx.coordinator.records());
        assertEquals(Arrays.asList("record1", "record2", "record3"), writer.records(TP));

        // Commit write #1.
        writer.commit(TP, 2);

        // The write is completed.
        assertTrue(write1.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));

        // The last committed offset is updated.
        assertEquals(2L, ctx.lastCommittedOffset);
        // The snapshot is cleaned up.
        assertEquals(Arrays.asList(2L, 3L), ctx.snapshotRegistry.epochsList());

        // Commit write #2.
        writer.commit(TP, 3);

        // The writes are completed.
        assertTrue(write2.isDone());
        assertTrue(write3.isDone());
        assertEquals("response2", write2.get(5, TimeUnit.SECONDS));
        assertEquals("response3", write3.get(5, TimeUnit.SECONDS));

        // The last committed offset is updated.
        assertEquals(3L, ctx.lastCommittedOffset);
        // The snapshot is cleaned up.
        assertEquals(Collections.singletonList(3L), ctx.snapshotRegistry.epochsList());

        // Write #4 but without records.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response4"));

        // It is completed immediately because the state is fully committed.
        assertTrue(write4.isDone());
        assertEquals("response4", write4.get(5, TimeUnit.SECONDS));
        assertEquals(Collections.singletonList(3L), ctx.snapshotRegistry.epochsList());
    }

    @Test
    public void testScheduleWriteOpWhenInactive() {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Scheduling a write fails with a NotCoordinatorException because the coordinator
        // does not exist.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP,
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Scheduling a write that fails when the operation is called. The exception
        // is used to complete the future.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP, state -> {
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Collections.singletonList(0L), ctx.snapshotRegistry.epochsList());

        // Override the coordinator with a coordinator that throws
        // an exception when replay is called.
        ctx.coordinator = new MockCoordinatorShard(ctx.snapshotRegistry, ctx.timer) {
            @Override
            public void replay(String record) throws RuntimeException {
                throw new IllegalArgumentException("error");
            }
        };

        // Write. It should fail.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));
        assertFutureThrows(write, IllegalArgumentException.class);

        // Verify that the state has not changed.
        assertEquals(0L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Collections.singletonList(0L), ctx.snapshotRegistry.epochsList());
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.lastWrittenOffset);
        assertEquals(0, ctx.lastCommittedOffset);
        assertEquals(Collections.singletonList(0L), ctx.snapshotRegistry.epochsList());

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleWriteOperation("write#1", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Verify that the state has been updated.
        assertEquals(2L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Arrays.asList(0L, 2L), ctx.snapshotRegistry.epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.records());

        // Write #2. It should fail because the writer is configured to only
        // accept 2 records per batch.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4", "record5"), "response2"));
        assertFutureThrows(write2, KafkaException.class);

        // Verify that the state has not changed.
        assertEquals(2L, ctx.lastWrittenOffset);
        assertEquals(0L, ctx.lastCommittedOffset);
        assertEquals(Arrays.asList(0L, 2L), ctx.snapshotRegistry.epochsList());
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.records());
    }

    @Test
    public void testScheduleReadOp() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.lastWrittenOffset);
        assertEquals(0, ctx.lastCommittedOffset);

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Commit write #1.
        writer.commit(TP, 2);

        // Write #1 is completed.
        assertTrue(write1.isDone());

        // Write #2 is not.
        assertFalse(write2.isDone());

        // The last written and committed offsets are updated.
        assertEquals(4, ctx.lastWrittenOffset);
        assertEquals(2, ctx.lastCommittedOffset);

        // Read.
        CompletableFuture<String> read = runtime.scheduleReadOperation("read", TP, (state, offset) -> {
            // The read operation should be given the last committed offset.
            assertEquals(ctx.lastCommittedOffset, offset);
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.lastWrittenOffset);
        assertEquals(0, ctx.lastCommittedOffset);

        // Write #1.
        runtime.scheduleWriteOperation("write#1", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        runtime.scheduleWriteOperation("write#2", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Commit write #1.
        writer.commit(TP, 2);

        // Read. It fails with an exception that is used to complete the future.
        CompletableFuture<String> read = runtime.scheduleReadOperation("read", TP, (state, offset) -> {
            assertEquals(ctx.lastCommittedOffset, offset);
            throw new IllegalArgumentException("error");
        });
        assertFutureThrows(read, IllegalArgumentException.class);
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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.lastWrittenOffset);
        assertEquals(0, ctx.lastCommittedOffset);

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP,
            state -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), "response2"));

        // Writes are inflight.
        assertFalse(write1.isDone());
        assertFalse(write2.isDone());

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());

        // Timer #1. This is never executed.
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.SECONDS, true,
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
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        MockCoordinatorShard coordinator0 = mock(MockCoordinatorShard.class);
        MockCoordinatorShard coordinator1 = mock(MockCoordinatorShard.class);

        when(supplier.get()).thenReturn(builder);
        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.build())
            .thenReturn(coordinator0)
            .thenReturn(coordinator1);

        CompletableFuture<CoordinatorLoader.LoadSummary> future0 = new CompletableFuture<>();
        when(loader.load(tp0, coordinator0)).thenReturn(future0);

        CompletableFuture<CoordinatorLoader.LoadSummary> future1 = new CompletableFuture<>();
        when(loader.load(tp1, coordinator1)).thenReturn(future1);

        runtime.scheduleLoadOperation(tp0, 0);
        runtime.scheduleLoadOperation(tp1, 0);

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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.lastWrittenOffset);
        assertEquals(0, ctx.lastCommittedOffset);

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Arrays.asList("record1", "record2"), null));

        // Timer #2.
        ctx.coordinator.timer.schedule("timer-2", 20, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Arrays.asList("record3", "record4"), null));

        // The coordinator timer should have two pending tasks.
        assertEquals(2, ctx.timer.size());

        // Advance time to fire timer #1,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(mkSet("record1", "record2"), ctx.coordinator.records());
        assertEquals(1, ctx.timer.size());

        // Advance time to fire timer #2,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(mkSet("record1", "record2", "record3", "record4"), ctx.coordinator.records());
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
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
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record2"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Schedule a third timer with the same key.
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
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
        assertEquals(mkSet("record3"), ctx.coordinator.records());
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
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
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
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(Collections.singletonList("record2"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Cancel the timer.
        ctx.coordinator.timer.cancel("timer-1");

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
        assertEquals(Collections.emptySet(), ctx.coordinator.records());
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testRetryableTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true, () -> {
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
        ctx.coordinator.timer.cancel("timer-1");
        assertEquals(0, ctx.timer.size());
    }

    @Test
    public void testNonRetryableTimer() throws InterruptedException {
        MockTimer timer = new MockTimer();
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(GroupCoordinatorRuntimeMetrics.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.coordinator.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, false, () -> {
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
                .withLoader(loader)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.build()).thenReturn(coordinator);
        when(supplier.get()).thenReturn(builder);
        CompletableFuture<CoordinatorLoader.LoadSummary> future = new CompletableFuture<>();
        when(loader.load(TP, coordinator)).thenReturn(future);

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
        when(loader.load(tp, coordinator)).thenReturn(future);
        // Schedule the loading.
        runtime.scheduleLoadOperation(tp, 0);
        // Getting the context succeeds and the coordinator should be in loading.
        ctx = runtime.contextOrThrow(tp);
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
                .withLoader(new MockCoordinatorLoader(
                    new CoordinatorLoader.LoadSummary(
                        startTimeMs,
                        startTimeMs + 1000,
                        30,
                        3000)))
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
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
}
