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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatcher;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.ACTIVE;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.CLOSED;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.FAILED;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.INITIAL;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.LOADING;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.HighWatermarkListener.NO_OFFSET;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.MIN_BUFFER_SIZE;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("checkstyle:JavaNCSS")
public class CoordinatorRuntimeTest {
    private static final TopicPartition TP = new TopicPartition("__consumer_offsets", 0);
    private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(5);

    private static final short TXN_OFFSET_COMMIT_LATEST_VERSION = ApiKeys.TXN_OFFSET_COMMIT.latestVersion();

    private static class StringSerializer implements Serializer<String> {
        @Override
        public byte[] serializeKey(String record) {
            return null;
        }

        @Override
        public byte[] serializeValue(String record) {
            return record.getBytes(Charset.defaultCharset());
        }
    }

    /**
     * A CoordinatorEventProcessor that directly executes the operations. This is
     * useful in unit tests where execution in threads is not required.
     */
    private static class DirectEventProcessor implements CoordinatorEventProcessor {
        @Override
        public void enqueueLast(CoordinatorEvent event) throws RejectedExecutionException {
            try {
                event.run();
            } catch (Throwable ex) {
                event.complete(ex);
            }
        }

        @Override
        public void enqueueFirst(CoordinatorEvent event) throws RejectedExecutionException {
            try {
                event.run();
            } catch (Throwable ex) {
                event.complete(ex);
            }
        }

        @Override
        public void close() {}
    }

    /**
     * A CoordinatorEventProcessor that queues event and execute the next one
     * when poll() is called.
     */
    private static class ManualEventProcessor implements CoordinatorEventProcessor {
        private final Deque<CoordinatorEvent> queue = new LinkedList<>();

        @Override
        public void enqueueLast(CoordinatorEvent event) throws RejectedExecutionException {
            queue.addLast(event);
        }

        @Override
        public void enqueueFirst(CoordinatorEvent event) throws RejectedExecutionException {
            queue.addFirst(event);
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
        public void close() {

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
        public void close() { }
    }

    /**
     * An in-memory partition writer that accepts a maximum number of writes.
     */
    private static class MockPartitionWriter extends InMemoryPartitionWriter {
        private final Time time;
        private final int maxWrites;
        private final boolean failEndMarker;
        private final AtomicInteger writeCount = new AtomicInteger(0);

        public MockPartitionWriter() {
            this(new MockTime(), Integer.MAX_VALUE, false);
        }

        public MockPartitionWriter(int maxWrites) {
            this(new MockTime(), maxWrites, false);
        }

        public MockPartitionWriter(boolean failEndMarker) {
            this(new MockTime(), Integer.MAX_VALUE, failEndMarker);
        }

        public MockPartitionWriter(Time time, int maxWrites, boolean failEndMarker) {
            super(false);
            this.time = time;
            this.maxWrites = maxWrites;
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
            VerificationGuard verificationGuard,
            MemoryRecords batch
        ) {
            if (batch.sizeInBytes() > config(tp).maxMessageSize())
                throw new RecordTooLargeException("Batch is larger than the max message size");

            if (writeCount.incrementAndGet() > maxWrites)
                throw new KafkaException("Maximum number of writes reached");

            if (failEndMarker && batch.firstBatch().isControlBatch())
                throw new KafkaException("Couldn't write end marker.");

            time.sleep(10);
            return super.append(tp, verificationGuard, batch);
        }
    }

    /**
     * A simple Coordinator implementation that stores the records into a set.
     */
    static class MockCoordinatorShard implements CoordinatorShard<String> {
        static class RecordAndMetadata {
            public final long offset;
            public final long producerId;
            public final short producerEpoch;
            public final String record;

            public RecordAndMetadata(
                long offset,
                String record
            ) {
                this(
                    offset,
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    record
                );
            }

            public RecordAndMetadata(
                long offset,
                long producerId,
                short producerEpoch,
                String record
            ) {
                this.offset = offset;
                this.producerId = producerId;
                this.producerEpoch = producerEpoch;
                this.record = record;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                RecordAndMetadata that = (RecordAndMetadata) o;

                if (offset != that.offset) return false;
                if (producerId != that.producerId) return false;
                if (producerEpoch != that.producerEpoch) return false;
                return Objects.equals(record, that.record);
            }

            @Override
            public int hashCode() {
                int result = (int) (offset ^ (offset >>> 32));
                result = 31 * result + (int) (producerId ^ (producerId >>> 32));
                result = 31 * result + (int) producerEpoch;
                result = 31 * result + (record != null ? record.hashCode() : 0);
                return result;
            }

            @Override
            public String toString() {
                return "RecordAndMetadata(" +
                    "offset=" + offset +
                    ", producerId=" + producerId +
                    ", producerEpoch=" + producerEpoch +
                    ", record='" + record.substring(0, Math.min(10, record.length())) + '\'' +
                    ')';
            }
        }

        private final SnapshotRegistry snapshotRegistry;
        private final TimelineHashSet<RecordAndMetadata> records;
        private final TimelineHashMap<Long, TimelineHashSet<RecordAndMetadata>> pendingRecords;
        private final CoordinatorTimer<Void, String> timer;
        private final CoordinatorExecutor<String> executor;

        MockCoordinatorShard(
            SnapshotRegistry snapshotRegistry,
            CoordinatorTimer<Void, String> timer
        ) {
            this(snapshotRegistry, timer, null);
        }

        MockCoordinatorShard(
            SnapshotRegistry snapshotRegistry,
            CoordinatorTimer<Void, String> timer,
            CoordinatorExecutor<String> executor
        ) {
            this.snapshotRegistry = snapshotRegistry;
            this.records = new TimelineHashSet<>(snapshotRegistry, 0);
            this.pendingRecords = new TimelineHashMap<>(snapshotRegistry, 0);
            this.timer = timer;
            this.executor = executor;
        }

        @Override
        public void replay(
            long offset,
            long producerId,
            short producerEpoch,
            String record
        ) throws RuntimeException {
            RecordAndMetadata recordAndMetadata = new RecordAndMetadata(
                offset,
                producerId,
                producerEpoch,
                record
            );

            if (producerId == RecordBatch.NO_PRODUCER_ID) {
                records.add(recordAndMetadata);
            } else {
                pendingRecords
                    .computeIfAbsent(producerId, __ -> new TimelineHashSet<>(snapshotRegistry, 0))
                    .add(recordAndMetadata);
            }
        }

        @Override
        public void replayEndTransactionMarker(
            long producerId,
            short producerEpoch,
            TransactionResult result
        ) throws RuntimeException {
            if (result == TransactionResult.COMMIT) {
                TimelineHashSet<RecordAndMetadata> pending = pendingRecords.remove(producerId);
                if (pending == null) return;
                records.addAll(pending);
            } else {
                pendingRecords.remove(producerId);
            }
        }

        Set<String> pendingRecords(long producerId) {
            TimelineHashSet<RecordAndMetadata> pending = pendingRecords.get(producerId);
            if (pending == null) return Collections.emptySet();
            return pending.stream().map(record -> record.record).collect(Collectors.toUnmodifiableSet());
        }

        Set<String> records() {
            return records.stream().map(record -> record.record).collect(Collectors.toUnmodifiableSet());
        }

        List<RecordAndMetadata> fullRecords() {
            return records
                .stream()
                .sorted(Comparator.comparingLong(record -> record.offset))
                .collect(Collectors.toList());
        }
    }

    /**
     * A CoordinatorBuilder that creates a MockCoordinator.
     */
    private static class MockCoordinatorShardBuilder implements CoordinatorShardBuilder<MockCoordinatorShard, String> {
        private SnapshotRegistry snapshotRegistry;
        private CoordinatorTimer<Void, String> timer;
        private CoordinatorExecutor<String> executor;

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
        public CoordinatorShardBuilder<MockCoordinatorShard, String> withExecutor(
            CoordinatorExecutor<String> executor
        ) {
            this.executor = executor;
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
                Objects.requireNonNull(this.timer),
                Objects.requireNonNull(this.executor)
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

    private static MemoryRecords records(
        long timestamp,
        String... records
    ) {
        return records(timestamp, Arrays.stream(records).collect(Collectors.toList()));
    }

    private static MemoryRecords records(
        long timestamp,
        List<String> records
    ) {
        if (records.isEmpty())
            return MemoryRecords.EMPTY;

        List<SimpleRecord> simpleRecords = records.stream().map(record ->
            new SimpleRecord(timestamp, record.getBytes(Charset.defaultCharset()))
        ).collect(Collectors.toList());

        int sizeEstimate = AbstractRecords.estimateSizeInBytes(
            RecordVersion.current().value,
            CompressionType.NONE,
            simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            RecordVersion.current().value,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0L,
            timestamp,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            0,
            false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        simpleRecords.forEach(builder::append);

        return builder.build();
    }

    private static MemoryRecords transactionalRecords(
        long producerId,
        short producerEpoch,
        long timestamp,
        String... records
    ) {
        return transactionalRecords(
            producerId,
            producerEpoch,
            timestamp,
            Arrays.stream(records).collect(Collectors.toList())
        );
    }

    private static MemoryRecords transactionalRecords(
        long producerId,
        short producerEpoch,
        long timestamp,
        List<String> records
    ) {
        if (records.isEmpty())
            return MemoryRecords.EMPTY;

        List<SimpleRecord> simpleRecords = records.stream().map(record ->
            new SimpleRecord(timestamp, record.getBytes(Charset.defaultCharset()))
        ).collect(Collectors.toList());

        int sizeEstimate = AbstractRecords.estimateSizeInBytes(
            RecordVersion.current().value,
            CompressionType.NONE,
            simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(sizeEstimate);

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer,
            RecordVersion.current().value,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            0L,
            timestamp,
            producerId,
            producerEpoch,
            0,
            true,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );

        simpleRecords.forEach(builder::append);

        return builder.build();
    }

    private static MemoryRecords endTransactionMarker(
        long producerId,
        short producerEpoch,
        long timestamp,
        int coordinatorEpoch,
        ControlRecordType result
    ) {
        return MemoryRecords.withEndTransactionMarker(
            timestamp,
            producerId,
            producerEpoch,
            new EndTransactionMarker(
                result,
                coordinatorEpoch
            )
        );
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1")
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // The last written offset is updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().records());
        // Records have been written to the log.
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record1", "record2")
        ), writer.entries(TP));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record3"), "response2"));

        // Verify that the write is not committed yet.
        assertFalse(write2.isDone());

        // The last written offset is updated.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(List.of(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        assertEquals(Set.of("record1", "record2", "record3"), ctx.coordinator.coordinator().records());
        // Records have been written to the log.
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record1", "record2"),
            records(timer.time().milliseconds(), "record3")
        ), writer.entries(TP));

        // Write #3 but without any records.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response3"));

        // Verify that the write is not committed yet.
        assertFalse(write3.isDone());

        // The state does not change.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2", "record3"), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record1", "record2"),
            records(timer.time().milliseconds(), "record3")
        ), writer.entries(TP));

        // Commit write #1.
        writer.commit(TP, 2);

        // The write is completed.
        assertTrue(write1.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));

        // The last committed offset is updated.
        assertEquals(2L, ctx.coordinator.lastCommittedOffset());
        // The snapshot is cleaned up.
        assertEquals(List.of(2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());

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
        assertEquals(List.of(3L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #4 but without records.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(Collections.emptyList(), "response4"));

        // It is completed immediately because the state is fully committed.
        assertTrue(write4.isDone());
        assertEquals("response4", write4.get(5, TimeUnit.SECONDS));
        assertEquals(List.of(3L), ctx.coordinator.snapshotRegistry().epochsList());
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

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
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));
        assertFutureThrows(write, IllegalArgumentException.class);

        // Verify that the state has not changed.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
    }

    @Test
    public void testScheduleWriteOpWhenWriteFails() {
        MockTimer timer = new MockTimer();
        // The partition writer only accept one write.
        MockPartitionWriter writer = new MockPartitionWriter(1);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().records());

        // Write #2. It should fail because the writer is configured to only
        // accept 1 write.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record3", "record4", "record5"), "response2"));
        assertFutureThrows(write2, KafkaException.class);

        // Verify that the state has not changed.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().records());
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. We should get a TimeoutException because the HWM will not advance.
        CompletableFuture<String> timedOutWrite = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(3),
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));

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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                List.of("record#" + counter),
                List.of("response#" + counter)
            );
        });

        assertEquals(1L, runtime.contextOrThrow(coordinator0).coordinator.lastWrittenOffset());
        assertEquals(1L, runtime.contextOrThrow(coordinator1).coordinator.lastWrittenOffset());
        assertEquals(1L, runtime.contextOrThrow(coordinator2).coordinator.lastWrittenOffset());

        assertEquals(List.of(records(timer.time().milliseconds(), "record#0")), writer.entries(coordinator0));
        assertEquals(List.of(records(timer.time().milliseconds(), "record#1")), writer.entries(coordinator1));
        assertEquals(List.of(records(timer.time().milliseconds(), "record#2")), writer.entries(coordinator2));

        // Commit.
        writer.commit(coordinator0);
        writer.commit(coordinator1);
        writer.commit(coordinator2);

        // Verify.
        assertEquals(
            List.of("response#0", "response#1", "response#2"),
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify that the listener was registered.
        verify(writer, times(1)).registerListener(eq(TP), any());

        // Prepare the log config.
        when(writer.config(TP)).thenReturn(new LogConfig(Collections.emptyMap()));

        // Prepare the transaction verification.
        VerificationGuard guard = new VerificationGuard();
        when(writer.maybeStartTransactionVerification(
            TP,
            "transactional-id",
            100L,
            (short) 50,
            TXN_OFFSET_COMMIT_LATEST_VERSION
        )).thenReturn(CompletableFuture.completedFuture(guard));

        // Schedule a transactional write.
        runtime.scheduleTransactionalWriteOperation(
            "tnx-write",
            TP,
            "transactional-id",
            100L,
            (short) 50,
            Duration.ofMillis(5000),
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the writer got the records with the correct
        // producer id and producer epoch.
        verify(writer, times(1)).append(
            eq(TP),
            eq(guard),
            eq(transactionalRecords(
                100L,
                (short) 50,
                timer.time().milliseconds(),
                "record1",
                "record2"
            ))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
            (short) 50,
            TXN_OFFSET_COMMIT_LATEST_VERSION
        )).thenReturn(FutureUtils.failedFuture(Errors.NOT_ENOUGH_REPLICAS.exception()));

        // Schedule a transactional write.
        CompletableFuture<String> future = runtime.scheduleTransactionalWriteOperation(
            "tnx-write",
            TP,
            "transactional-id",
            100L,
            (short) 50,
            Duration.ofMillis(5000),
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the future is failed with the expected exception.
        assertFutureThrows(future, NotEnoughReplicasException.class);

        // Verify that the writer is not called.
        verify(writer, times(0)).append(
            any(),
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Transactional write #1.
        CompletableFuture<String> write1 = runtime.scheduleTransactionalWriteOperation(
            "write#1",
            TP,
            "transactional-id",
            100L,
            (short) 5,
            DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // The last written offset is updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        // The last committed offset does not change.
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        // A new snapshot is created.
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator. They are stored in
        // the pending set for now.
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(
            100L
        ));
        // Records have been written to the log.
        assertEquals(List.of(
            transactionalRecords(100L, (short) 5, timer.time().milliseconds(), "record1", "record2")
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
        assertEquals(List.of(0L, 2L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        // Records have been replayed to the coordinator.
        ControlRecordType expectedType;
        if (result == TransactionResult.COMMIT) {
            // They are now in the records set if committed.
            assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().records());
            expectedType = ControlRecordType.COMMIT;
        } else {
            // Or they are gone if aborted.
            assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
            expectedType = ControlRecordType.ABORT;
        }

        // Records have been written to the log.
        assertEquals(List.of(
            transactionalRecords(100L, (short) 5, timer.time().milliseconds(), "record1", "record2"),
            endTransactionMarker(100L, (short) 5, timer.time().milliseconds(), 10, expectedType)
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

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
        assertEquals(List.of(0L, 1L), ctx.coordinator.snapshotRegistry().epochsList());

        // Advance clock to timeout Complete #1.
        timer.advanceClock(4);

        assertFutureThrows(timedOutCompletion, org.apache.kafka.common.errors.TimeoutException.class);

        // Verify that the state is still the same. We don't revert when the
        // operation timeouts because the record has been written to the log.
        assertEquals(1L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 1L), ctx.coordinator.snapshotRegistry().epochsList());
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        // Write #1. It should succeed and be applied to the coordinator.
        runtime.scheduleTransactionalWriteOperation(
            "write#1",
            TP,
            "transactional-id",
            100L,
            (short) 5,
            DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
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
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

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
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the state has been updated.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            transactionalRecords(100L, (short) 5, timer.time().milliseconds(), "record1", "record2")
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
        assertEquals(List.of(0L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            transactionalRecords(100L, (short) 5, timer.time().milliseconds(), "record1", "record2")
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record3", "record4"), "response2"));

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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));

        // Write #2.
        runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record3", "record4"), "response2"));

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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
            state -> new CoordinatorResult<>(List.of("record0"), "response0"));
        runtime.scheduleWriteOperation("write#1", coordinator1, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1"), "response1"));
        runtime.scheduleWriteOperation("write#2", coordinator2, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record2"), "response2"));

        // Commit writes.
        writer.commit(coordinator0);
        writer.commit(coordinator1);
        writer.commit(coordinator2);

        // Read.
        List<CompletableFuture<List<String>>> responses = runtime.scheduleReadAllOperation(
            "read",
            (state, offset) -> new ArrayList<>(state.records())
        );

        assertEquals(
            List.of("record0", "record1", "record2"),
            FutureUtils.combineFutures(responses, ArrayList::new, List::addAll).get(5, TimeUnit.SECONDS)
        );
    }

    @Test
    public void testClose() throws Exception {
        MockCoordinatorLoader loader = spy(new MockCoordinatorLoader());
        MockTimer timer = new MockTimer();
        ExecutorService executorService = mock(ExecutorService.class);
        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withLoader(loader)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(new MockPartitionWriter())
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(executorService)
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.coordinator.lastWrittenOffset());
        assertEquals(0, ctx.coordinator.lastCommittedOffset());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1", "record2"), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record3", "record4"), "response2"));

        // Writes are inflight.
        assertFalse(write1.isDone());
        assertFalse(write2.isDone());

        // The coordinator timer should be empty.
        assertEquals(0, ctx.timer.size());

        // Timer #1. This is never executed.
        ctx.timer.schedule("timer-1", 10, TimeUnit.SECONDS, true,
            () -> new CoordinatorResult<>(List.of("record5", "record6"), null));

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

        // Verify that the executor service was shutdown.
        verify(executorService).shutdown();
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
        when(builder.withExecutor(any())).thenReturn(builder);
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
            () -> new CoordinatorResult<>(List.of("record1", "record2"), null));

        // Timer #2.
        ctx.timer.schedule("timer-2", 20, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(List.of("record3", "record4"), null));

        // The coordinator timer should have two pending tasks.
        assertEquals(2, ctx.timer.size());

        // Advance time to fire timer #1,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(Set.of("record1", "record2"), ctx.coordinator.coordinator().records());
        assertEquals(1, ctx.timer.size());

        // Advance time to fire timer #2,
        timer.advanceClock(10 + 1);

        // Verify that the operation was executed.
        assertEquals(Set.of("record1", "record2", "record3", "record4"), ctx.coordinator.coordinator().records());
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
            () -> new CoordinatorResult<>(List.of("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(List.of("record2"), null));

        // The coordinator timer should still have one pending task.
        assertEquals(1, ctx.timer.size());

        // Schedule a third timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(List.of("record3"), null));

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
        assertEquals(Set.of("record3"), ctx.coordinator.coordinator().records());
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
            () -> new CoordinatorResult<>(List.of("record1"), null));

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance time to fire the pending timer.
        timer.advanceClock(10 + 1);

        // An event should be waiting in the processor.
        assertEquals(1, processor.size());

        // Schedule a second timer with the same key.
        ctx.timer.schedule("timer-1", 10, TimeUnit.MILLISECONDS, true,
            () -> new CoordinatorResult<>(List.of("record2"), null));

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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
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
    public void testTimerScheduleIfAbsent() throws InterruptedException {
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
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator.
        runtime.scheduleLoadOperation(TP, 10);

        // Check initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0, ctx.timer.size());

        // Timer #1.
        AtomicInteger cnt = new AtomicInteger(0);
        ctx.timer.scheduleIfAbsent("timer-1", 10, TimeUnit.MILLISECONDS, false, () -> {
            cnt.incrementAndGet();
            throw new KafkaException("error");
        });

        // The coordinator timer should have one pending task.
        assertEquals(1, ctx.timer.size());

        // Advance half of the time to fire the pending timer.
        timer.advanceClock(10 / 2);

        // Reschedule timer #1. Since the timer already exists, the timeout shouldn't be refreshed.
        ctx.timer.scheduleIfAbsent("timer-1", 10, TimeUnit.MILLISECONDS, false, () -> {
            cnt.incrementAndGet();
            throw new KafkaException("error");
        });

        // Advance the time to fire the pending timer.
        timer.advanceClock(10 / 2 + 1);

        // The timer should have been called and the timer should have no pending tasks.
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
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

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
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

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
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

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
                    List.of(5L, 15L, 27L),
                    List.of(5L, 15L)))
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(supplier)
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

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
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        when(builder.withSnapshotRegistry(any())).thenReturn(builder);
        when(builder.withLogContext(any())).thenReturn(builder);
        when(builder.withTime(any())).thenReturn(builder);
        when(builder.withTimer(any())).thenReturn(builder);
        when(builder.withCoordinatorMetrics(any())).thenReturn(builder);
        when(builder.withTopicPartition(any())).thenReturn(builder);
        when(builder.withExecutor(any())).thenReturn(builder);
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

    @Test
    public void testHighWatermarkUpdate() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1"), "response1")
        );
        processor.poll();

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record2"), "response2")
        );
        processor.poll();

        // Records have been written to the log.
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record1"),
            records(timer.time().milliseconds(), "record2")
        ), writer.entries(TP));

        // There is no pending high watermark.
        assertEquals(-1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Commit the first record.
        writer.commit(TP, 1);

        // We should have one pending event and the pending high watermark should be set.
        assertEquals(1, processor.size());
        assertEquals(1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Commit the second record.
        writer.commit(TP, 2);

        // We should still have one pending event and the pending high watermark should be updated.
        assertEquals(1, processor.size());
        assertEquals(2, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Poll once to process the high watermark update and complete the writes.
        processor.poll();

        assertEquals(-1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(2, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isDone());
        assertTrue(write2.isDone());
    }

    @Test
    public void testWriteEventWriteTimeoutTaskIsCancelledWhenHighWatermarkIsUpdated() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // Write#1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("Write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record1"), "response1")
        );
        processor.poll();

        // Write#2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("Write#2", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(List.of("record2"), "response2")
        );
        processor.poll();

        // Records have been written to the log.
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record1"),
            records(timer.time().milliseconds(), "record2")
        ), writer.entries(TP));

        // The write timeout tasks exist.
        assertEquals(2, timer.size());

        // Commit the first record.
        writer.commit(TP, 1);

        // Commit the second record.
        writer.commit(TP, 2);

        // We should still have one pending event and the pending high watermark should be updated.
        assertEquals(1, processor.size());
        assertEquals(2, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // The write timeout tasks should have not yet been cancelled.
        assertEquals(2, timer.size());
        timer.taskQueue().forEach(taskEntry -> assertFalse(taskEntry.cancelled()));

        // Poll once to process the high watermark update and complete the writes.
        processor.poll();

        assertEquals(-1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(2, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isDone());
        assertTrue(write2.isDone());

        // All timer tasks have been cancelled. TimerTask entries are not removed in MockTimer.
        assertEquals(2, timer.size());
        timer.taskQueue().forEach(taskEntry -> assertTrue(taskEntry.cancelled()));
    }

    @Test
    public void testCoordinatorCompleteTransactionEventWriteTimeoutTaskIsCancelledWhenHighWatermarkIsUpdated() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // transaction completion.
        CompletableFuture<Void> write1 = runtime.scheduleTransactionCompletion(
            "transactional-write",
            TP,
            100L,
            (short) 50,
            1,
            TransactionResult.COMMIT,
            DEFAULT_WRITE_TIMEOUT
        );
        processor.poll();

        // Records have been written to the log.
        assertEquals(List.of(
            endTransactionMarker(100, (short) 50, timer.time().milliseconds(), 1, ControlRecordType.COMMIT)
        ), writer.entries(TP));

        // The write timeout tasks exist.
        assertEquals(1, timer.size());

        // Commit the first record.
        writer.commit(TP, 1);

        // We should still have one pending event and the pending high watermark should be updated.
        assertEquals(1, processor.size());
        assertEquals(1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // The write timeout tasks should have not yet been cancelled.
        assertEquals(1, timer.size());
        timer.taskQueue().forEach(taskEntry -> assertFalse(taskEntry.cancelled()));

        // Poll once to process the high watermark update and complete the writes.
        processor.poll();

        assertEquals(-1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(1, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isDone());

        // All timer tasks have been cancelled. TimerTask entries are not removed in MockTimer.
        assertEquals(1, timer.size());
        timer.taskQueue().forEach(taskEntry -> assertTrue(taskEntry.cancelled()));
    }

    @Test
    public void testAppendRecordBatchSize() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        StringSerializer serializer = new StringSerializer();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(serializer)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());

        int maxBatchSize = writer.config(TP).maxMessageSize();
        assertTrue(maxBatchSize > MIN_BUFFER_SIZE);

        // Generate enough records to create a batch that has 16KB < batchSize < maxBatchSize
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            records.add("record-" + i);
        }

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, DEFAULT_WRITE_TIMEOUT,
            state -> new CoordinatorResult<>(records, "response1")
        );

        // Verify that the write has not completed exceptionally.
        // This will catch any exceptions thrown including RecordTooLargeException.
        assertFalse(write1.isCompletedExceptionally());

        int batchSize = writer.entries(TP).get(0).sizeInBytes();
        assertTrue(batchSize > MIN_BUFFER_SIZE && batchSize < maxBatchSize);
    }

    @Test
    public void testScheduleWriteOperationWithBatching() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1 with two records.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(0, 2), "response1")
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // A batch has been created.
        assertNotNull(ctx.currentBatch);

        // Verify the state. Records are replayed but no batch written.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Write #2 with one record.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(2, 3), "response2")
        );

        // Verify that the write is not committed yet.
        assertFalse(write2.isDone());

        // Verify the state. Records are replayed but no batch written.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Write #3 with one record. This one cannot go into the existing batch
        // so the existing batch should be flushed and a new one should be created.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(3, 4), "response3")
        );

        // Verify that the write is not committed yet.
        assertFalse(write3.isDone());

        // Verify the state. Records are replayed. The previous batch
        // got flushed with all the records but the new one from #3.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(timer.time().milliseconds(), records.subList(0, 3))
        ), writer.entries(TP));

        // Advance past the linger time.
        timer.advanceClock(11);

        // Verify the state. The pending batch is flushed.
        assertEquals(4L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 3L, 4L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(timer.time().milliseconds() - 11, records.subList(0, 3)),
            records(timer.time().milliseconds() - 11, records.subList(3, 4))
        ), writer.entries(TP));

        // Commit and verify that writes are completed.
        writer.commit(TP);
        assertTrue(write1.isDone());
        assertTrue(write2.isDone());
        assertTrue(write3.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));
        assertEquals("response2", write2.get(5, TimeUnit.SECONDS));
        assertEquals("response3", write3.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleWriteOperationWithBatchingWhenRecordsTooLarge() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write all the records.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records, "response1")
        );

        assertFutureThrows(write, RecordTooLargeException.class);
    }

    @Test
    public void testScheduleWriteOperationWithBatchingWhenWriteFails() {
        MockTimer timer = new MockTimer();
        // The partition writer does not accept any writes
        MockPartitionWriter writer = new MockPartitionWriter(0);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(0, 1), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(1, 2), "response2"));

        // Write #3.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(2, 3), "response3"));

        // Verify the state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Write #4. This write cannot make it in the current batch. So the current batch
        // is flushed. It will fail. So we expect all writes to fail.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(3, 4), "response4"));

        // Verify the futures.
        assertFutureThrows(write1, KafkaException.class);
        assertFutureThrows(write2, KafkaException.class);
        assertFutureThrows(write3, KafkaException.class);
        // Write #4 is also expected to fail.
        assertFutureThrows(write4, KafkaException.class);

        // Verify the state. The state should be reverted to the initial state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Collections.emptyList(), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));
    }

    @Test
    public void testScheduleWriteOperationWithBatchingWhenReplayFails() {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

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
                    if (offset >= 1) {
                        throw new IllegalArgumentException("error");
                    }
                    super.replay(
                        offset,
                        producerId,
                        producerEpoch,
                        record
                    );
                }
            },
            TP
        );

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each.
        List<String> records = Stream.of('1', '2').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(0, 1), "response1"));

        // Verify the state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Write #2. It should fail.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(1, 2), "response2"));

        // Verify the futures.
        assertFutureThrows(write1, IllegalArgumentException.class);
        assertFutureThrows(write2, IllegalArgumentException.class);

        // Verify the state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Collections.emptyList(), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));
    }

    @Test
    public void testScheduleTransactionalWriteOperationWithBatching() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Write #1 with one record.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(List.of("record#1"), "response1")
        );

        // Verify that the write is not committed yet.
        assertFalse(write1.isDone());

        // Verify the state. Records are replayed but no batch written.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Set.of("record#1"), ctx.coordinator.coordinator().records());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Transactional write #2 with one record. This will flush the current batch.
        CompletableFuture<String> write2 = runtime.scheduleTransactionalWriteOperation(
            "txn-write#1",
            TP,
            "transactional-id",
            100L,
            (short) 50,
            Duration.ofMillis(20),
            state -> new CoordinatorResult<>(List.of("record#2"), "response2"),
            TXN_OFFSET_COMMIT_LATEST_VERSION
        );

        // Verify that the write is not committed yet.
        assertFalse(write2.isDone());

        // Verify the state. The current batch and the transactional records are
        // written to the log.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 1L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record#2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Set.of("record#1"), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record#1"),
            transactionalRecords(100L, (short) 50, timer.time().milliseconds(), "record#2")
        ), writer.entries(TP));

        // Write #3 with one record.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(List.of("record#3"), "response3")
        );

        // Verify that the write is not committed yet.
        assertFalse(write3.isDone());

        // Verify the state.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 1L, 2L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Set.of("record#2"), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Set.of("record#1", "record#3"), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record#1"),
            transactionalRecords(100L, (short) 50, timer.time().milliseconds(), "record#2")
        ), writer.entries(TP));

        // Complete transaction #1. It will flush the current batch if any.
        CompletableFuture<Void> complete1 = runtime.scheduleTransactionCompletion(
            "complete#1",
            TP,
            100L,
            (short) 50,
            10,
            TransactionResult.COMMIT,
            DEFAULT_WRITE_TIMEOUT
        );

        // Verify that the completion is not committed yet.
        assertFalse(complete1.isDone());

        // Verify the state.
        assertEquals(4L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Collections.emptySet(), ctx.coordinator.coordinator().pendingRecords(100L));
        assertEquals(Set.of("record#1", "record#2", "record#3"), ctx.coordinator.coordinator().records());
        assertEquals(List.of(
            records(timer.time().milliseconds(), "record#1"),
            transactionalRecords(100L, (short) 50, timer.time().milliseconds(), "record#2"),
            records(timer.time().milliseconds(), "record#3"),
            endTransactionMarker(100L, (short) 50, timer.time().milliseconds(), 10, ControlRecordType.COMMIT)
        ), writer.entries(TP));

        // Commit and verify that writes are completed.
        writer.commit(TP);
        assertTrue(write1.isDone());
        assertTrue(write2.isDone());
        assertTrue(write3.isDone());
        assertTrue(complete1.isDone());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));
        assertEquals("response2", write2.get(5, TimeUnit.SECONDS));
        assertEquals("response3", write3.get(5, TimeUnit.SECONDS));
        assertNull(complete1.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testStateMachineIsReloadedWhenOutOfSync() {
        MockTimer timer = new MockTimer();
        MockCoordinatorLoader loader = spy(new MockCoordinatorLoader());
        MockPartitionWriter writer = new MockPartitionWriter() {
            @Override
            public long append(
                TopicPartition tp,
                VerificationGuard verificationGuard,
                MemoryRecords batch
            ) {
                // Add 1 to the returned offsets.
                return super.append(tp, verificationGuard, batch) + 1;
            }
        };

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(loader)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(ACTIVE, ctx.state);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Keep a reference to the current coordinator.
        SnapshottableCoordinator<MockCoordinatorShard, String> coordinator = ctx.coordinator;

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(0, 1), "response1"));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(1, 2), "response2"));

        // Write #3.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(2, 3), "response3"));

        // Write #4. This write cannot make it in the current batch. So the current batch
        // is flushed. It will fail. So we expect all writes to fail.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(3, 4), "response4"));

        // Verify the futures.
        assertFutureThrows(write1, NotCoordinatorException.class);
        assertFutureThrows(write2, NotCoordinatorException.class);
        assertFutureThrows(write3, NotCoordinatorException.class);
        // Write #4 is also expected to fail.
        assertFutureThrows(write4, NotCoordinatorException.class);

        // Verify that the state machine was loaded twice.
        verify(loader, times(2)).load(eq(TP), any());

        // Verify that the state is active and that the state machine
        // is actually a new one.
        assertEquals(ACTIVE, ctx.state);
        assertNotEquals(coordinator, ctx.coordinator);
    }

    @Test
    public void testWriteOpIsNotReleasedWhenStateMachineIsNotCaughtUpAfterLoad() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        CoordinatorLoader<String> loader = new CoordinatorLoader<String>() {
            @Override
            public CompletableFuture<LoadSummary> load(
                TopicPartition tp,
                CoordinatorPlayback<String> coordinator
            ) {
                coordinator.replay(
                    0,
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    "record#0"
                );

                coordinator.replay(
                    0,
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    "record#1"
                );

                coordinator.updateLastWrittenOffset(2L);
                coordinator.updateLastCommittedOffset(1L);

                return CompletableFuture.completedFuture(new LoadSummary(
                    0L,
                    0L,
                    0L,
                    2,
                    1
                ));
            }

            @Override
            public void close() {}
        };

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(loader)
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(1L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(2L), ctx.coordinator.snapshotRegistry().epochsList());

        // Schedule a write operation that does not generate any records.
        CompletableFuture<String> write = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(Collections.emptyList(), "response1"));

        // The write operation should not be done.
        assertFalse(write.isDone());

        // Advance the last committed offset.
        ctx.highWatermarklistener.onHighWatermarkUpdated(TP, 2L);

        // Verify the state.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(2L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(2L), ctx.coordinator.snapshotRegistry().epochsList());

        // The write operation should be completed.
        assertEquals("response1", write.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleNonAtomicWriteOperation() throws ExecutionException, InterruptedException, TimeoutException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Let's try to write all the records atomically (the default) to ensure
        // that it fails.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records, "write#1")
        );

        assertFutureThrows(write1, RecordTooLargeException.class);

        // Let's try to write the same records non-atomically.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records, "write#2", null, true, false)
        );

        // The write is pending.
        assertFalse(write2.isDone());

        // Verify the state.
        assertNotNull(ctx.currentBatch);
        // The last written offset is 3L because one batch was written to the log with
        // the first three records. The 4th one is pending.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(timer.time().milliseconds(), records.subList(0, 3))
        ), writer.entries(TP));

        // Commit up to 3L.
        writer.commit(TP, 3L);

        // The write is still pending.
        assertFalse(write2.isDone());

        // Advance past the linger time to flush the pending batch.
        timer.advanceClock(11);

        // Verify the state.
        assertNull(ctx.currentBatch);
        assertEquals(4L, ctx.coordinator.lastWrittenOffset());
        assertEquals(3L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(3L, 4L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(timer.time().milliseconds() - 11, records.subList(0, 3)),
            records(timer.time().milliseconds() - 11, records.subList(3, 4))
        ), writer.entries(TP));

        // Commit up to 4L.
        writer.commit(TP, 4L);

        // Verify that the write is completed.
        assertTrue(write2.isDone());
        assertEquals("write#2", write2.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testScheduleNonAtomicWriteOperationWithRecordTooLarge() throws InterruptedException {
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Create another record larger than the max batch size.
        char[] payload = new char[maxBatchSize];
        Arrays.fill(payload, '4');
        String record = new String(payload);

        // Let's write the first three records.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records, "write#1", null, true, false)
        );

        // Verify the state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Let's write the 4th record which is too large. This will flush the current
        // pending batch, allocate a new batch, and put the record into it.
        // Note that the batch will fail only when the batch is written because the
        // MemoryBatchBuilder always accept one record.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(List.of(record), "write#2", null, true, false)
        );

        // Advance past the linger time to flush the pending batch.
        timer.advanceClock(11);

        // The write should have failed...
        assertFutureThrows(write2, RecordTooLargeException.class);

        // ... but write#1 should be left intact.
        assertFalse(write1.isDone());

        // Verify the state.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L, 3L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(timer.time().milliseconds() - 11, records.subList(0, 3))
        ), writer.entries(TP));
    }

    @Test
    public void testScheduleNonAtomicWriteOperationWhenWriteFails() {
        MockTimer timer = new MockTimer();
        // The partition writer does not accept any writes.
        MockPartitionWriter writer = new MockPartitionWriter(0);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(0, 1), "response1", null, true, false));

        // Write #2.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(1, 2), "response2", null, true, false));

        // Write #3.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(2, 3), "response3", null, true, false));

        // Verify the state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));

        // Write #4. This write cannot make it in the current batch. So the current batch
        // is flushed. It will fail. So we expect all writes to fail.
        CompletableFuture<String> write4 = runtime.scheduleWriteOperation("write#4", TP, Duration.ofMillis(20),
            state -> new CoordinatorResult<>(records.subList(3, 4), "response4", null, true, false));

        // Verify the futures.
        assertFutureThrows(write1, KafkaException.class);
        assertFutureThrows(write2, KafkaException.class);
        assertFutureThrows(write3, KafkaException.class);
        // Write #4 is also expected to fail.
        assertFutureThrows(write4, KafkaException.class);

        // Verify the state. The state should be reverted to the initial state.
        assertEquals(0L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(0L), ctx.coordinator.snapshotRegistry().epochsList());
        assertEquals(Collections.emptyList(), ctx.coordinator.coordinator().fullRecords());
        assertEquals(Collections.emptyList(), writer.entries(TP));
    }

    @Test
    public void testRecordFlushTime() throws Exception {
        MockTimer timer = new MockTimer();

        // Writer sleeps for 10ms before appending records.
        MockPartitionWriter writer = new MockPartitionWriter(timer.time(), Integer.MAX_VALUE, false);
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(Duration.ofMillis(20))
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(new DirectEventProcessor())
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withAppendLingerMs(10)
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Schedule the loading.
        runtime.scheduleLoadOperation(TP, 10);

        // Verify the initial state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertNull(ctx.currentBatch);

        // Get the max batch size.
        int maxBatchSize = writer.config(TP).maxMessageSize();

        // Create records with a quarter of the max batch size each. Keep in mind that
        // each batch has a header so it is not possible to have those four records
        // in one single batch.
        List<String> records = Stream.of('1', '2', '3', '4').map(c -> {
            char[] payload = new char[maxBatchSize / 4];
            Arrays.fill(payload, c);
            return new String(payload);
        }).collect(Collectors.toList());

        // Write #1 with two records.
        long firstBatchTimestamp = timer.time().milliseconds();
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, Duration.ofMillis(50),
            state -> new CoordinatorResult<>(records.subList(0, 2), "response1")
        );

        // A batch has been created.
        assertNotNull(ctx.currentBatch);

        // Write #2 with one record.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, Duration.ofMillis(50),
            state -> new CoordinatorResult<>(records.subList(2, 3), "response2")
        );

        // Verify the state. Records are replayed but no batch written.
        assertEquals(Collections.emptyList(), writer.entries(TP));
        verify(runtimeMetrics, times(0)).recordFlushTime(10);

        // Write #3 with one record. This one cannot go into the existing batch
        // so the existing batch should be flushed and a new one should be created.
        long secondBatchTimestamp = timer.time().milliseconds();
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, Duration.ofMillis(50),
            state -> new CoordinatorResult<>(records.subList(3, 4), "response3")
        );

        // Verify the state. Records are replayed. The previous batch
        // got flushed with all the records but the new one from #3.
        assertEquals(3L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(firstBatchTimestamp, records.subList(0, 3))
        ), writer.entries(TP));
        verify(runtimeMetrics, times(1)).recordFlushTime(10);

        // Advance past the linger time.
        timer.advanceClock(11);

        // Verify the state. The pending batch is flushed.
        assertEquals(4L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, records.get(0)),
            new MockCoordinatorShard.RecordAndMetadata(1, records.get(1)),
            new MockCoordinatorShard.RecordAndMetadata(2, records.get(2)),
            new MockCoordinatorShard.RecordAndMetadata(3, records.get(3))
        ), ctx.coordinator.coordinator().fullRecords());
        assertEquals(List.of(
            records(secondBatchTimestamp, records.subList(0, 3)),
            records(secondBatchTimestamp, records.subList(3, 4))
        ), writer.entries(TP));
        verify(runtimeMetrics, times(2)).recordFlushTime(10);

        // Commit and verify that writes are completed.
        writer.commit(TP);
        assertTrue(write1.isDone());
        assertTrue(write2.isDone());
        assertTrue(write3.isDone());
        assertEquals(4L, ctx.coordinator.lastCommittedOffset());
        assertEquals("response1", write1.get(5, TimeUnit.SECONDS));
        assertEquals("response2", write2.get(5, TimeUnit.SECONDS));
        assertEquals("response3", write3.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testRecordEventPurgatoryTime() throws Exception {
        Duration writeTimeout = Duration.ofMillis(1000);
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(writeTimeout)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // write#1 will be committed and update the high watermark. Record time spent in purgatory.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, writeTimeout,
            state -> new CoordinatorResult<>(List.of("record1"), "response1")
        );
        // write#2 will time out sitting in the purgatory. Record time spent in purgatory.
        CompletableFuture<String> write2 = runtime.scheduleWriteOperation("write#2", TP, writeTimeout,
            state -> new CoordinatorResult<>(List.of("record2"), "response2")
        );
        // write#3 will error while appending. Does not spend time in purgatory.
        CompletableFuture<String> write3 = runtime.scheduleWriteOperation("write#3", TP, writeTimeout,
            state -> {
                throw new KafkaException("write#3 failed.");
            });

        processor.poll();
        processor.poll();
        processor.poll();

        // Confirm we do not record purgatory time for write#3.
        assertTrue(write3.isCompletedExceptionally());
        verify(runtimeMetrics, times(0)).recordEventPurgatoryTime(0L);

        // Records have been written to the log.
        long writeTimestamp = timer.time().milliseconds();
        assertEquals(List.of(
            records(writeTimestamp, "record1"),
            records(writeTimestamp, "record2")
        ), writer.entries(TP));

        // There is no pending high watermark.
        assertEquals(NO_OFFSET, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Advance the clock then commit records from write#1.
        timer.advanceClock(700);
        writer.commit(TP, 1);

        // We should still have one pending event and the pending high watermark should be updated.
        assertEquals(1, processor.size());
        assertEquals(1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Poll once to process the high watermark update and complete the writes.
        processor.poll();
        long purgatoryTimeMs = timer.time().milliseconds() - writeTimestamp;

        // Advance the clock past write timeout. write#2 has now timed out.
        timer.advanceClock(300 + 1);
        processor.poll();

        assertEquals(NO_OFFSET, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(1, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isDone());
        assertTrue(write2.isCompletedExceptionally());
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(purgatoryTimeMs);
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(writeTimeout.toMillis() + 1);
    }

    @Test
    public void testWriteEventCompletesOnlyOnce() throws Exception {
        // Completes once via timeout, then again with HWM update.
        Duration writeTimeout = Duration.ofMillis(1000L);
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(writeTimeout)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // write#1 will be committed and update the high watermark. Record time spent in purgatory.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, writeTimeout,
            state -> new CoordinatorResult<>(List.of("record1"), "response1")
        );

        processor.poll();

        // Records have been written to the log.
        long writeTimestamp = timer.time().milliseconds();
        assertEquals(Collections.singletonList(
            records(writeTimestamp, "record1")
        ), writer.entries(TP));

        // There is no pending high watermark.
        assertEquals(NO_OFFSET, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Advance the clock to time out the write event. Confirm write#1 is completed with a timeout.
        timer.advanceClock(writeTimeout.toMillis() + 1L);
        processor.poll();
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(writeTimeout.toMillis() + 1);
        assertTrue(write1.isCompletedExceptionally());

        // HWM update
        writer.commit(TP, 1);
        assertEquals(1, processor.size());
        assertEquals(1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Poll once to process the high watermark update and complete write#1. It has already
        // been completed and this is a noop.
        processor.poll();

        assertEquals(NO_OFFSET, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(1, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isCompletedExceptionally());
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(writeTimeout.toMillis() + 1L);
    }

    @Test
    public void testCompleteTransactionEventCompletesOnlyOnce() throws Exception {
        // Completes once via timeout, then again with HWM update.
        Duration writeTimeout = Duration.ofMillis(1000L);
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(writeTimeout)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // transaction completion.
        CompletableFuture<Void> write1 = runtime.scheduleTransactionCompletion(
            "transactional-write",
            TP,
            100L,
            (short) 50,
            1,
            TransactionResult.COMMIT,
            writeTimeout
        );
        processor.poll();

        // Records have been written to the log.
        assertEquals(List.of(
            endTransactionMarker(100, (short) 50, timer.time().milliseconds(), 1, ControlRecordType.COMMIT)
        ), writer.entries(TP));

        // The write timeout tasks exist.
        assertEquals(1, timer.size());
        assertFalse(write1.isDone());

        // Advance the clock to time out the write event. Confirm write#1 is completed with a timeout.
        timer.advanceClock(writeTimeout.toMillis() + 1L);
        processor.poll();
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(writeTimeout.toMillis() + 1);
        assertTrue(write1.isCompletedExceptionally());

        // HWM update
        writer.commit(TP, 1);
        assertEquals(1, processor.size());
        assertEquals(1, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());

        // Poll once to process the high watermark update and complete write#1. It has already
        // been completed and this is a noop.
        processor.poll();

        assertEquals(NO_OFFSET, runtime.contextOrThrow(TP).highWatermarklistener.lastHighWatermark());
        assertEquals(1, runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertTrue(write1.isCompletedExceptionally());
        verify(runtimeMetrics, times(1)).recordEventPurgatoryTime(writeTimeout.toMillis() + 1L);
    }
    @Test
    public void testCoordinatorExecutor() {
        Duration writeTimeout = Duration.ofMillis(1000);
        MockTimer timer = new MockTimer();
        MockPartitionWriter writer = new MockPartitionWriter();
        ManualEventProcessor processor = new ManualEventProcessor();
        CoordinatorRuntimeMetrics runtimeMetrics = mock(CoordinatorRuntimeMetrics.class);
        ExecutorService executorService = mock(ExecutorService.class);

        when(executorService.submit(any(Runnable.class))).thenAnswer(args -> {
            Runnable op = args.getArgument(0);
            op.run();
            return CompletableFuture.completedFuture(null);
        });

        CoordinatorRuntime<MockCoordinatorShard, String> runtime =
            new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(writeTimeout)
                .withLoader(new MockCoordinatorLoader())
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(runtimeMetrics)
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(executorService)
                .build();

        // Loads the coordinator. Poll once to execute the load operation and once
        // to complete the load.
        runtime.scheduleLoadOperation(TP, 10);
        processor.poll();
        processor.poll();

        // Schedule a write which schedules an async tasks.
        CompletableFuture<String> write1 = runtime.scheduleWriteOperation("write#1", TP, writeTimeout,
            state -> {
                state.executor.schedule(
                    "write#1#task",
                    () -> "task result",
                    (result, exception) -> {
                        assertEquals("task result", result);
                        assertNull(exception);
                        return new CoordinatorResult<>(Collections.singletonList("record2"), null);
                    }
                );
                return new CoordinatorResult<>(Collections.singletonList("record1"), "response1");
            }
        );

        // Execute the write.
        processor.poll();

        // We should have a new write event in the queue as a result of the
        // task being executed immediately.
        assertEquals(1, processor.queue.size());

        // Verify the state.
        CoordinatorRuntime<MockCoordinatorShard, String>.CoordinatorContext ctx = runtime.contextOrThrow(TP);
        assertEquals(1L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, "record1")
        ), ctx.coordinator.coordinator().fullRecords());

        // Execute the pending write.
        processor.poll();

        // The processor must be empty now.
        assertEquals(0, processor.queue.size());

        // Verify the state.
        assertEquals(2L, ctx.coordinator.lastWrittenOffset());
        assertEquals(0L, ctx.coordinator.lastCommittedOffset());
        assertEquals(List.of(
            new MockCoordinatorShard.RecordAndMetadata(0, "record1"),
            new MockCoordinatorShard.RecordAndMetadata(1, "record2")
        ), ctx.coordinator.coordinator().fullRecords());

        // Commit.
        writer.commit(TP);
        processor.poll();
        assertTrue(write1.isDone());
    }

    private static <S extends CoordinatorShard<U>, U> ArgumentMatcher<CoordinatorPlayback<U>> coordinatorMatcher(
        CoordinatorRuntime<S, U> runtime,
        TopicPartition tp
    ) {
        return c -> c.equals(runtime.contextOrThrow(tp).coordinator);
    }
}
