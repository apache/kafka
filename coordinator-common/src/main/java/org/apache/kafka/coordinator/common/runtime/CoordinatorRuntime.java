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
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.deferred.DeferredEvent;
import org.apache.kafka.deferred.DeferredEventQueue;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorWriteEvent.NOT_QUEUED;

/**
 * The CoordinatorRuntime provides a framework to implement coordinators such as the group coordinator
 * or the transaction coordinator.
 *
 * The runtime framework maps each underlying partitions (e.g. __consumer_offsets) that that broker is a
 * leader of to a coordinator replicated state machine. A replicated state machine holds the hard and soft
 * state of all the objects (e.g. groups or offsets) assigned to the partition. The hard state is stored in
 * timeline datastructures backed by a SnapshotRegistry. The runtime supports two type of operations
 * on state machines: (1) Writes and (2) Reads.
 *
 * (1) A write operation, aka a request, can read the full and potentially **uncommitted** state from state
 * machine to handle the operation. A write operation typically generates a response and a list of
 * records. The records are applied to the state machine and persisted to the partition. The response
 * is parked until the records are committed and delivered when they are.
 *
 * (2) A read operation, aka a request, can only read the committed state from the state machine to handle
 * the operation. A read operation typically generates a response that is immediately completed.
 *
 * The runtime framework exposes an asynchronous, future based, API to the world. All the operations
 * are executed by an CoordinatorEventProcessor. The processor guarantees that operations for a
 * single partition or state machine are not processed concurrently.
 *
 * @param <S> The type of the state machine.
 * @param <U> The type of the record.
 */
public class CoordinatorRuntime<S extends CoordinatorShard<U>, U> implements AutoCloseable {

    /**
     * Builder to create a CoordinatorRuntime.
     *
     * @param <S> The type of the state machine.
     * @param <U> The type of the record.
     */
    public static class Builder<S extends CoordinatorShard<U>, U> {
        private String logPrefix;
        private LogContext logContext;
        private CoordinatorEventProcessor eventProcessor;
        private PartitionWriter partitionWriter;
        private CoordinatorLoader<U> loader;
        private CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier;
        private Time time = Time.SYSTEM;
        private Timer timer;
        private Duration defaultWriteTimeout;
        private CoordinatorRuntimeMetrics runtimeMetrics;
        private CoordinatorMetrics coordinatorMetrics;
        private Serializer<U> serializer;
        private Compression compression;
        private int appendLingerMs;
        private ExecutorService executorService;

        public Builder<S, U> withLogPrefix(String logPrefix) {
            this.logPrefix = logPrefix;
            return this;
        }

        public Builder<S, U> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public Builder<S, U> withEventProcessor(CoordinatorEventProcessor eventProcessor) {
            this.eventProcessor = eventProcessor;
            return this;
        }

        public Builder<S, U> withPartitionWriter(PartitionWriter partitionWriter) {
            this.partitionWriter = partitionWriter;
            return this;
        }

        public Builder<S, U> withLoader(CoordinatorLoader<U> loader) {
            this.loader = loader;
            return this;
        }

        public Builder<S, U> withCoordinatorShardBuilderSupplier(CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier) {
            this.coordinatorShardBuilderSupplier = coordinatorShardBuilderSupplier;
            return this;
        }

        public Builder<S, U> withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder<S, U> withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder<S, U> withDefaultWriteTimeOut(Duration defaultWriteTimeout) {
            this.defaultWriteTimeout = defaultWriteTimeout;
            return this;
        }

        public Builder<S, U> withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics runtimeMetrics) {
            this.runtimeMetrics = runtimeMetrics;
            return this;
        }

        public Builder<S, U> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        public Builder<S, U> withSerializer(Serializer<U> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<S, U> withCompression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder<S, U> withAppendLingerMs(int appendLingerMs) {
            this.appendLingerMs = appendLingerMs;
            return this;
        }

        public Builder<S, U> withExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public CoordinatorRuntime<S, U> build() {
            if (logPrefix == null)
                logPrefix = "";
            if (logContext == null)
                logContext = new LogContext(logPrefix);
            if (eventProcessor == null)
                throw new IllegalArgumentException("Event processor must be set.");
            if (partitionWriter == null)
                throw new IllegalArgumentException("Partition write must be set.");
            if (loader == null)
                throw new IllegalArgumentException("Loader must be set.");
            if (coordinatorShardBuilderSupplier == null)
                throw new IllegalArgumentException("State machine supplier must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (runtimeMetrics == null)
                throw new IllegalArgumentException("CoordinatorRuntimeMetrics must be set.");
            if (coordinatorMetrics == null)
                throw new IllegalArgumentException("CoordinatorMetrics must be set.");
            if (serializer == null)
                throw new IllegalArgumentException("Serializer must be set.");
            if (compression == null)
                compression = Compression.NONE;
            if (appendLingerMs < 0)
                throw new IllegalArgumentException("AppendLinger must be >= 0");
            if (executorService == null)
                throw new IllegalArgumentException("ExecutorService must be set.");

            return new CoordinatorRuntime<>(
                logPrefix,
                logContext,
                eventProcessor,
                partitionWriter,
                loader,
                coordinatorShardBuilderSupplier,
                time,
                timer,
                defaultWriteTimeout,
                runtimeMetrics,
                coordinatorMetrics,
                serializer,
                compression,
                appendLingerMs,
                executorService
            );
        }
    }

    /**
     * The various state that a coordinator for a partition can be in.
     */
    public enum CoordinatorState {
        /**
         * Initial state when a coordinator is created.
         */
        INITIAL {
            @Override
            boolean canTransitionFrom(CoordinatorState state) {
                return false;
            }
        },

        /**
         * The coordinator is being loaded.
         */
        LOADING {
            @Override
            boolean canTransitionFrom(CoordinatorState state) {
                return state == INITIAL || state == FAILED;
            }
        },

        /**
         * The coordinator is active and can service requests.
         */
        ACTIVE {
            @Override
            boolean canTransitionFrom(CoordinatorState state) {
                return state == ACTIVE || state == LOADING;
            }
        },

        /**
         * The coordinator is closed.
         */
        CLOSED {
            @Override
            boolean canTransitionFrom(CoordinatorState state) {
                return true;
            }
        },

        /**
         * The coordinator loading has failed.
         */
        FAILED {
            @Override
            boolean canTransitionFrom(CoordinatorState state) {
                return state == LOADING || state == ACTIVE;
            }
        };

        abstract boolean canTransitionFrom(CoordinatorState state);
    }

    /**
     * The EventBasedCoordinatorTimer implements the CoordinatorTimer interface and provides an event based
     * timer which turns timeouts of a regular {@link Timer} into {@link CoordinatorWriteEvent} events which
     * are executed by the {@link CoordinatorEventProcessor} used by this coordinator runtime. This is done
     * to ensure that the timer respects the threading model of the coordinator runtime.
     *
     * The {@link CoordinatorWriteEvent} events pushed by the coordinator timer wraps the
     * {@link TimeoutOperation} operations scheduled by the coordinators.
     *
     * It also keeps track of all the scheduled {@link TimerTask}. This allows timeout operations to be
     * cancelled or rescheduled. When a timer is cancelled or overridden, the previous timer is guaranteed to
     * not be executed even if it already expired and got pushed to the event processor.
     *
     * When a timer fails with an unexpected exception, the timer is rescheduled with a backoff.
     */
    class EventBasedCoordinatorTimer implements CoordinatorTimer<Void, U> {
        /**
         * The logger.
         */
        final Logger log;

        /**
         * The topic partition.
         */
        final TopicPartition tp;

        /**
         * The scheduled timers keyed by their key.
         */
        final Map<String, TimerTask> tasks = new HashMap<>();

        EventBasedCoordinatorTimer(TopicPartition tp, LogContext logContext) {
            this.tp = tp;
            this.log = logContext.logger(EventBasedCoordinatorTimer.class);
        }

        @Override
        public void schedule(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            TimeoutOperation<Void, U> operation
        ) {
            schedule(key, delay, unit, retry, 500, operation);
        }

        @Override
        public void schedule(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            long retryBackoff,
            TimeoutOperation<Void, U> operation
        ) {
            // The TimerTask wraps the TimeoutOperation into a CoordinatorWriteEvent. When the TimerTask
            // expires, the event is pushed to the queue of the coordinator runtime to be executed. This
            // ensures that the threading model of the runtime is respected.
            TimerTask task = new TimerTask(unit.toMillis(delay)) {
                @Override
                public void run() {
                    String eventName = "Timeout(tp=" + tp + ", key=" + key + ")";
                    CoordinatorWriteEvent<Void> event = new CoordinatorWriteEvent<>(eventName, tp, defaultWriteTimeout, coordinator -> {
                        log.debug("Executing write event {} for timer {}.", eventName, key);

                        // If the task is different, it means that the timer has been
                        // cancelled while the event was waiting to be processed.
                        if (!tasks.remove(key, this)) {
                            throw new RejectedExecutionException("Timer " + key + " was overridden or cancelled");
                        }

                        // Execute the timeout operation.
                        return operation.generateRecords();
                    });

                    // If the write event fails, it is rescheduled with a small backoff except if retry
                    // is disabled or if the error is fatal.
                    event.future.exceptionally(ex -> {
                        if (ex instanceof RejectedExecutionException) {
                            log.debug("The write event {} for the timer {} was not executed because it was " +
                                "cancelled or overridden.", event.name, key);
                            return null;
                        }

                        if (ex instanceof NotCoordinatorException || ex instanceof CoordinatorLoadInProgressException) {
                            log.debug("The write event {} for the timer {} failed due to {}. Ignoring it because " +
                                "the coordinator is not active.", event.name, key, ex.getMessage());
                            return null;
                        }

                        if (retry) {
                            log.info("The write event {} for the timer {} failed due to {}. Rescheduling it. ",
                                event.name, key, ex.getMessage());
                            schedule(key, retryBackoff, TimeUnit.MILLISECONDS, true, retryBackoff, operation);
                        } else {
                            log.error("The write event {} for the timer {} failed due to {}. Ignoring it. ",
                                event.name, key, ex.getMessage());
                        }

                        return null;
                    });

                    log.debug("Scheduling write event {} for timer {}.", event.name, key);
                    try {
                        enqueueLast(event);
                    } catch (NotCoordinatorException ex) {
                        log.info("Failed to enqueue write event {} for timer {} because the runtime is closed. Ignoring it.",
                            event.name, key);
                    }
                }
            };

            log.debug("Registering timer {} with delay of {}ms.", key, unit.toMillis(delay));
            TimerTask prevTask = tasks.put(key, task);
            if (prevTask != null) prevTask.cancel();

            timer.add(task);
        }

        @Override
        public void scheduleIfAbsent(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            TimeoutOperation<Void, U> operation
        ) {
            if (!tasks.containsKey(key)) {
                schedule(key, delay, unit, retry, 500, operation);
            }
        }

        @Override
        public void cancel(String key) {
            TimerTask prevTask = tasks.remove(key);
            if (prevTask != null) prevTask.cancel();
        }

        public void cancelAll() {
            Iterator<Map.Entry<String, TimerTask>> iterator = tasks.entrySet().iterator();
            while (iterator.hasNext()) {
                iterator.next().getValue().cancel();
                iterator.remove();
            }
        }

        public int size() {
            return tasks.size();
        }
    }

    /**
     * A simple container class to hold all the attributes
     * related to a pending batch.
     */
    private static class CoordinatorBatch {
        /**
         * The base (or first) offset of the batch. If the batch fails
         * for any reason, the state machines is rolled back to it.
         */
        final long baseOffset;

        /**
         * The time at which the batch was created.
         */
        final long appendTimeMs;

        /**
         * The max batch size.
         */
        final int maxBatchSize;

        /**
         * The verification guard associated to the batch if it is
         * transactional.
         */
        final VerificationGuard verificationGuard;

        /**
         * The byte buffer backing the records builder.
         */
        final ByteBuffer buffer;

        /**
         * The records builder.
         */
        final MemoryRecordsBuilder builder;

        /**
         * The timer used to enfore the append linger time if
         * it is non-zero.
         */
        final Optional<TimerTask> lingerTimeoutTask;

        /**
         * The list of deferred events associated with the batch.
         */
        final List<DeferredEvent> deferredEvents;

        /**
         * The next offset. This is updated when records
         * are added to the batch.
         */
        long nextOffset;

        CoordinatorBatch(
            long baseOffset,
            long appendTimeMs,
            int maxBatchSize,
            VerificationGuard verificationGuard,
            ByteBuffer buffer,
            MemoryRecordsBuilder builder,
            Optional<TimerTask> lingerTimeoutTask
        ) {
            this.baseOffset = baseOffset;
            this.nextOffset = baseOffset;
            this.appendTimeMs = appendTimeMs;
            this.maxBatchSize = maxBatchSize;
            this.verificationGuard = verificationGuard;
            this.buffer = buffer;
            this.builder = builder;
            this.lingerTimeoutTask = lingerTimeoutTask;
            this.deferredEvents = new ArrayList<>();
        }
    }

    /**
     * CoordinatorContext holds all the metadata around a coordinator state machine.
     */
    class CoordinatorContext {
        /**
         * The lock which protects all data in the context. Note that the context
         * is never accessed concurrently, but it is accessed by multiple threads.
         */
        final ReentrantLock lock;

        /**
         * The topic partition backing the coordinator.
         */
        final TopicPartition tp;

        /**
         * The log context.
         */
        final LogContext logContext;

        /**
         * The deferred event queue used to park events waiting
         * on records to be committed.
         */
        final DeferredEventQueue deferredEventQueue;

        /**
         * The coordinator timer.
         */
        final EventBasedCoordinatorTimer timer;

        /**
         * The coordinator executor.
         */
        final CoordinatorExecutorImpl<S, U> executor;

        /**
         * The current state.
         */
        CoordinatorState state;

        /**
         * The current epoch of the coordinator. This represents
         * the epoch of the partition leader.
         */
        int epoch;

        /**
         * The state machine and the metadata that can be accessed by
         * other threads.
         */
        SnapshottableCoordinator<S, U> coordinator;

        /**
         * The high watermark listener registered to all the partitions
         * backing the coordinators.
         */
        HighWatermarkListener highWatermarklistener;

        /**
         * The buffer supplier used to write records to the log.
         */
        BufferSupplier bufferSupplier;

        /**
         * The current (or pending) batch.
         */
        CoordinatorBatch currentBatch;

        /**
         * Constructor.
         *
         * @param tp The topic partition of the coordinator.
         */
        private CoordinatorContext(
            TopicPartition tp
        ) {
            this.lock = new ReentrantLock();
            this.tp = tp;
            this.logContext = new LogContext(String.format("[%s topic=%s partition=%d] ",
                logPrefix,
                tp.topic(),
                tp.partition()
            ));
            this.state = CoordinatorState.INITIAL;
            this.epoch = -1;
            this.deferredEventQueue = new DeferredEventQueue(logContext);
            this.timer = new EventBasedCoordinatorTimer(tp, logContext);
            this.executor = new CoordinatorExecutorImpl<>(
                logContext,
                tp,
                CoordinatorRuntime.this,
                executorService,
                defaultWriteTimeout
            );
            this.bufferSupplier = new BufferSupplier.GrowableBufferSupplier();
        }

        /**
         * Transitions to the new state.
         *
         * @param newState The new state.
         */
        private void transitionTo(
            CoordinatorState newState
        ) {
            if (!newState.canTransitionFrom(state)) {
                throw new IllegalStateException("Cannot transition from " + state + " to " + newState);
            }
            CoordinatorState oldState = state;

            log.debug("Transition from {} to {}.", state, newState);
            switch (newState) {
                case LOADING:
                    state = CoordinatorState.LOADING;
                    SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
                    coordinator = new SnapshottableCoordinator<>(
                        logContext,
                        snapshotRegistry,
                        coordinatorShardBuilderSupplier
                            .get()
                            .withLogContext(logContext)
                            .withSnapshotRegistry(snapshotRegistry)
                            .withTime(time)
                            .withTimer(timer)
                            .withExecutor(executor)
                            .withCoordinatorMetrics(coordinatorMetrics)
                            .withTopicPartition(tp)
                            .build(),
                        tp
                    );
                    load();
                    break;

                case ACTIVE:
                    state = CoordinatorState.ACTIVE;
                    highWatermarklistener = new HighWatermarkListener();
                    partitionWriter.registerListener(tp, highWatermarklistener);
                    coordinator.onLoaded(metadataImage);
                    break;

                case FAILED:
                    state = CoordinatorState.FAILED;
                    unload();
                    break;

                case CLOSED:
                    state = CoordinatorState.CLOSED;
                    unload();
                    break;

                default:
                    throw new IllegalArgumentException("Transitioning to " + newState + " is not supported.");
            }

            runtimeMetrics.recordPartitionStateChange(oldState, state);
        }

        /**
         * Loads the coordinator.
         */
        private void load() {
            if (state != CoordinatorState.LOADING) {
                throw new IllegalStateException("Coordinator must be in loading state");
            }

            loader.load(tp, coordinator).whenComplete((summary, exception) -> {
                scheduleInternalOperation("CompleteLoad(tp=" + tp + ", epoch=" + epoch + ")", tp, () -> {
                    CoordinatorContext context = coordinators.get(tp);
                    if (context != null)  {
                        if (context.state != CoordinatorState.LOADING) {
                            log.info("Ignored load completion from {} because context is in {} state.",
                                context.tp, context.state);
                            return;
                        }
                        try {
                            if (exception != null) throw exception;
                            context.transitionTo(CoordinatorState.ACTIVE);
                            if (summary != null) {
                                runtimeMetrics.recordPartitionLoadSensor(summary.startTimeMs(), summary.endTimeMs());
                                log.info("Finished loading of metadata from {} with epoch {} in {}ms where {}ms " +
                                        "was spent in the scheduler. Loaded {} records which total to {} bytes.",
                                    tp, epoch, summary.endTimeMs() - summary.startTimeMs(),
                                    summary.schedulerQueueTimeMs(), summary.numRecords(), summary.numBytes());
                            }
                        } catch (Throwable ex) {
                            log.error("Failed to load metadata from {} with epoch {} due to {}.",
                                tp, epoch, ex.toString());
                            context.transitionTo(CoordinatorState.FAILED);
                        }
                    } else {
                        log.debug("Failed to complete the loading of metadata for {} in epoch {} since the coordinator does not exist.",
                            tp, epoch);
                    }
                });
            });
        }

        /**
         * Unloads the coordinator.
         */
        private void unload() {
            if (highWatermarklistener != null) {
                partitionWriter.deregisterListener(tp, highWatermarklistener);
                highWatermarklistener = null;
            }
            timer.cancelAll();
            executor.cancelAll();
            deferredEventQueue.failAll(Errors.NOT_COORDINATOR.exception());
            failCurrentBatch(Errors.NOT_COORDINATOR.exception());
            if (coordinator != null) {
                coordinator.onUnloaded();
            }
            coordinator = null;
        }

        /**
         * Frees the current batch.
         */
        private void freeCurrentBatch() {
            // Cancel the linger timeout.
            currentBatch.lingerTimeoutTask.ifPresent(TimerTask::cancel);

            // Release the buffer.
            bufferSupplier.release(currentBatch.buffer);

            currentBatch = null;
        }

        /**
         * Flushes the current (or pending) batch to the log. When the batch is written
         * locally, a new snapshot is created in the snapshot registry and the events
         * associated with the batch are added to the deferred event queue.
         */
        private void flushCurrentBatch() {
            if (currentBatch != null) {
                try {
                    long flushStartMs = time.milliseconds();
                    // Write the records to the log and update the last written offset.
                    long offset = partitionWriter.append(
                        tp,
                        currentBatch.verificationGuard,
                        currentBatch.builder.build()
                    );
                    runtimeMetrics.recordFlushTime(time.milliseconds() - flushStartMs);
                    coordinator.updateLastWrittenOffset(offset);

                    if (offset != currentBatch.nextOffset) {
                        log.error("The state machine of the coordinator {} is out of sync with the underlying log. " +
                            "The last written offset returned is {} while the coordinator expected {}. The coordinator " +
                            "will be reloaded in order to re-synchronize the state machine.",
                            tp, offset, currentBatch.nextOffset);
                        // Transition to FAILED state to unload the state machine and complete
                        // exceptionally all the pending operations.
                        transitionTo(CoordinatorState.FAILED);
                        // Transition to LOADING to trigger the restoration of the state.
                        transitionTo(CoordinatorState.LOADING);
                        // Thrown NotCoordinatorException to fail the operation that
                        // triggered the write. We use NotCoordinatorException to be
                        // consistent with the transition to FAILED.
                        throw Errors.NOT_COORDINATOR.exception();
                    }

                    // Add all the pending deferred events to the deferred event queue.
                    for (DeferredEvent event : currentBatch.deferredEvents) {
                        deferredEventQueue.add(offset, event);
                    }

                    // Free up the current batch.
                    freeCurrentBatch();
                } catch (Throwable t) {
                    log.error("Writing records to {} failed due to: {}.", tp, t.getMessage());
                    failCurrentBatch(t);
                    // We rethrow the exception for the caller to handle it too.
                    throw t;
                }
            }
        }

        /**
         * Flushes the current batch if it is transactional or if it has passed the append linger time.
         */
        private void maybeFlushCurrentBatch(long currentTimeMs) {
            if (currentBatch != null) {
                if (currentBatch.builder.isTransactional() || (currentBatch.appendTimeMs - currentTimeMs) >= appendLingerMs) {
                    flushCurrentBatch();
                }
            }
        }

        /**
         * Fails the current batch, reverts to the snapshot to the base/start offset of the
         * batch, fails all the associated events.
         */
        private void failCurrentBatch(Throwable t) {
            if (currentBatch != null) {
                coordinator.revertLastWrittenOffset(currentBatch.baseOffset);
                for (DeferredEvent event : currentBatch.deferredEvents) {
                    event.complete(t);
                }
                freeCurrentBatch();
            }
        }

        /**
         * Allocates a new batch if none already exists.
         */
        private void maybeAllocateNewBatch(
            long producerId,
            short producerEpoch,
            VerificationGuard verificationGuard,
            long currentTimeMs
        ) {
            if (currentBatch == null) {
                LogConfig logConfig = partitionWriter.config(tp);
                byte magic = logConfig.recordVersion().value;
                int maxBatchSize = logConfig.maxMessageSize();
                long prevLastWrittenOffset = coordinator.lastWrittenOffset();
                ByteBuffer buffer = bufferSupplier.get(maxBatchSize);

                MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                    buffer,
                    magic,
                    compression,
                    TimestampType.CREATE_TIME,
                    0L,
                    currentTimeMs,
                    producerId,
                    producerEpoch,
                    0,
                    producerId != RecordBatch.NO_PRODUCER_ID,
                    false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    maxBatchSize
                );

                Optional<TimerTask> lingerTimeoutTask = Optional.empty();
                if (appendLingerMs > 0) {
                    lingerTimeoutTask = Optional.of(new TimerTask(appendLingerMs) {
                        @Override
                        public void run() {
                            // An event to flush the batch is pushed to the front of the queue
                            // to ensure that the linger time is respected.
                            enqueueFirst(new CoordinatorInternalEvent("FlushBatch", tp, () -> {
                                if (this.isCancelled()) return;
                                withActiveContextOrThrow(tp, CoordinatorContext::flushCurrentBatch);
                            }));
                        }
                    });
                    CoordinatorRuntime.this.timer.add(lingerTimeoutTask.get());
                }

                currentBatch = new CoordinatorBatch(
                    prevLastWrittenOffset,
                    currentTimeMs,
                    maxBatchSize,
                    verificationGuard,
                    buffer,
                    builder,
                    lingerTimeoutTask
                );
            }
        }

        /**
         * Appends records to the log and replay them to the state machine.
         *
         * @param producerId        The producer id.
         * @param producerEpoch     The producer epoch.
         * @param verificationGuard The verification guard.
         * @param records           The records to append.
         * @param replay            A boolean indicating whether the records
         *                          must be replayed or not.
         * @param isAtomic          A boolean indicating whether the records
         *                          must be written atomically or not.
         * @param event             The event that must be completed when the
         *                          records are written.
         */
        private void append(
            long producerId,
            short producerEpoch,
            VerificationGuard verificationGuard,
            List<U> records,
            boolean replay,
            boolean isAtomic,
            DeferredEvent event
        ) {
            if (state != CoordinatorState.ACTIVE) {
                throw new IllegalStateException("Coordinator must be active to append records");
            }

            if (records.isEmpty()) {
                // If the records are empty, it was a read operation after all. In this case,
                // the response can be returned directly iff there are no pending write operations;
                // otherwise, the read needs to wait on the last write operation to be completed.
                if (currentBatch != null) {
                    currentBatch.deferredEvents.add(event);
                } else {
                    if (coordinator.lastCommittedOffset() < coordinator.lastWrittenOffset()) {
                        deferredEventQueue.add(coordinator.lastWrittenOffset(), event);
                    } else {
                        event.complete(null);
                    }
                }
            } else {
                // If the records are not empty, first, they are applied to the state machine,
                // second, they are appended to the opened batch.
                long currentTimeMs = time.milliseconds();

                // If the current write operation is transactional, the current batch
                // is written before proceeding with it.
                if (producerId != RecordBatch.NO_PRODUCER_ID) {
                    isAtomic = true;
                    // If flushing fails, we don't catch the exception in order to let
                    // the caller fail the current operation.
                    flushCurrentBatch();
                }

                // Allocate a new batch if none exists.
                maybeAllocateNewBatch(
                    producerId,
                    producerEpoch,
                    verificationGuard,
                    currentTimeMs
                );

                // Prepare the records.
                List<SimpleRecord> recordsToAppend = new ArrayList<>(records.size());
                for (U record : records) {
                    recordsToAppend.add(new SimpleRecord(
                        currentTimeMs,
                        serializer.serializeKey(record),
                        serializer.serializeValue(record)
                    ));
                }

                if (isAtomic) {
                    // Compute the estimated size of the records.
                    int estimatedSize = AbstractRecords.estimateSizeInBytes(
                        currentBatch.builder.magic(),
                        compression.type(),
                        recordsToAppend
                    );

                    // Check if the current batch has enough space. We check this before
                    // replaying the records in order to avoid having to revert back
                    // changes if the records do not fit within a batch.
                    if (estimatedSize > currentBatch.builder.maxAllowedBytes()) {
                        throw new RecordTooLargeException("Message batch size is " + estimatedSize +
                            " bytes in append to partition " + tp + " which exceeds the maximum " +
                            "configured size of " + currentBatch.maxBatchSize + ".");
                    }

                    if (!currentBatch.builder.hasRoomFor(estimatedSize)) {
                        // Otherwise, we write the current batch, allocate a new one and re-verify
                        // whether the records fit in it.
                        // If flushing fails, we don't catch the exception in order to let
                        // the caller fail the current operation.
                        flushCurrentBatch();
                        maybeAllocateNewBatch(
                            producerId,
                            producerEpoch,
                            verificationGuard,
                            currentTimeMs
                        );
                    }
                }


                for (int i = 0; i < records.size(); i++) {
                    U recordToReplay = records.get(i);
                    SimpleRecord recordToAppend = recordsToAppend.get(i);

                    if (!isAtomic) {
                        // Check if the current batch has enough space. We check this before
                        // replaying the record in order to avoid having to revert back
                        // changes if the record do not fit within a batch.
                        boolean hasRoomFor = currentBatch.builder.hasRoomFor(
                            recordToAppend.timestamp(),
                            recordToAppend.key(),
                            recordToAppend.value(),
                            recordToAppend.headers()
                        );

                        if (!hasRoomFor) {
                            // If flushing fails, we don't catch the exception in order to let
                            // the caller fail the current operation.
                            flushCurrentBatch();
                            maybeAllocateNewBatch(
                                producerId,
                                producerEpoch,
                                verificationGuard,
                                currentTimeMs
                            );
                        }
                    }

                    try {
                        if (replay) {
                            coordinator.replay(
                                currentBatch.nextOffset,
                                producerId,
                                producerEpoch,
                                recordToReplay
                            );
                        }

                        currentBatch.builder.append(recordToAppend);
                        currentBatch.nextOffset++;
                    } catch (Throwable t) {
                        log.error("Replaying record {} to {} failed due to: {}.", recordToReplay, tp, t.getMessage());

                        // Add the event to the list of pending events associated with the last
                        // batch in order to fail it too.
                        currentBatch.deferredEvents.add(event);

                        // If an exception is thrown, we fail the entire batch. Exceptions should be
                        // really exceptional in this code path and they would usually be the results
                        // of bugs preventing records to be replayed.
                        failCurrentBatch(t);

                        return;
                    }
                }

                // Add the event to the list of pending events associated with the batch.
                currentBatch.deferredEvents.add(event);

                // Write the current batch if it is transactional or if the linger timeout
                // has expired.
                // If flushing fails, we don't catch the exception in order to let
                // the caller fail the current operation.
                maybeFlushCurrentBatch(currentTimeMs);
            }
        }

        /**
         * Completes a transaction.
         *
         * @param producerId        The producer id.
         * @param producerEpoch     The producer epoch.
         * @param coordinatorEpoch  The coordinator epoch of the transaction coordinator.
         * @param result            The transaction result.
         * @param event             The event that must be completed when the
         *                          control record is written.
         */
        private void completeTransaction(
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            TransactionResult result,
            DeferredEvent event
        ) {
            if (state != CoordinatorState.ACTIVE) {
                throw new IllegalStateException("Coordinator must be active to complete a transaction");
            }

            // The current batch must be written before the transaction marker is written
            // in order to respect the order.
            flushCurrentBatch();

            long prevLastWrittenOffset = coordinator.lastWrittenOffset();
            try {
                coordinator.replayEndTransactionMarker(
                    producerId,
                    producerEpoch,
                    result
                );

                long flushStartMs = time.milliseconds();
                long offset = partitionWriter.append(
                    tp,
                    VerificationGuard.SENTINEL,
                    MemoryRecords.withEndTransactionMarker(
                        time.milliseconds(),
                        producerId,
                        producerEpoch,
                        new EndTransactionMarker(
                            result == TransactionResult.COMMIT ? ControlRecordType.COMMIT : ControlRecordType.ABORT,
                            coordinatorEpoch
                        )
                    )
                );
                runtimeMetrics.recordFlushTime(time.milliseconds() - flushStartMs);
                coordinator.updateLastWrittenOffset(offset);

                deferredEventQueue.add(offset, event);
            } catch (Throwable t) {
                coordinator.revertLastWrittenOffset(prevLastWrittenOffset);
                event.complete(t);
            }
        }
    }

    class OperationTimeout extends TimerTask {
        private final TopicPartition tp;
        private final DeferredEvent event;

        public OperationTimeout(
            TopicPartition tp,
            DeferredEvent event,
            long delayMs
        ) {
            super(delayMs);
            this.event = event;
            this.tp = tp;
        }

        @Override
        public void run() {
            String name = event.toString();
            scheduleInternalOperation("OperationTimeout(name=" + name + ", tp=" + tp + ")", tp,
                () -> event.complete(new TimeoutException(name + " timed out after " + delayMs + "ms")));
        }
    }

    /**
     * A coordinator write operation.
     *
     * @param <S> The type of the coordinator state machine.
     * @param <T> The type of the response.
     * @param <U> The type of the records.
     */
    public interface CoordinatorWriteOperation<S, T, U> {
        /**
         * Generates the records needed to implement this coordinator write operation. In general,
         * this operation should not modify the hard state of the coordinator. That modifications
         * will happen later on, when the records generated by this function are applied to the
         * coordinator.
         *
         * @param coordinator The coordinator state machine.
         * @return A result containing a list of records and the RPC result.
         * @throws KafkaException
         */
        CoordinatorResult<T, U> generateRecordsAndResult(S coordinator) throws KafkaException;
    }

    /**
     * A coordinator event that modifies the coordinator state.
     *
     * @param <T> The type of the response.
     */
    class CoordinatorWriteEvent<T> implements CoordinatorEvent, DeferredEvent {

        /**
         * Indicates that the event was not appended to the deferred event queue.
         */
        public static final long NOT_QUEUED = -1L;

        /**
         * The topic partition that this write event is applied to.
         */
        final TopicPartition tp;

        /**
         * The operation name.
         */
        final String name;

        /**
         * The transactional id.
         */
        final String transactionalId;

        /**
         * The producer id.
         */
        final long producerId;

        /**
         * The producer epoch.
         */
        final short producerEpoch;

        /**
         * The verification guard.
         */
        final VerificationGuard verificationGuard;

        /**
         * The write operation to execute.
         */
        final CoordinatorWriteOperation<S, T, U> op;

        /**
         * The future that will be completed with the response
         * generated by the write operation or an error.
         */
        final CompletableFuture<T> future;

        /**
         * Timeout value for the write operation.
         */
        final Duration writeTimeout;

        /**
         * The operation timeout.
         */
        private OperationTimeout operationTimeout = null;

        /**
         * The result of the write operation. It could be null
         * if an exception is thrown before it is assigned.
         */
        CoordinatorResult<T, U> result;

        /**
         * The time this event was created.
         */
        private final long createdTimeMs;

        /**
         * The time the event was added to the deferred queue.
         */
        private long deferredEventQueuedTimestamp;

        /**
         * Constructor.
         *
         * @param name                  The operation name.
         * @param tp                    The topic partition that the operation is applied to.
         * @param writeTimeout          The write operation timeout
         * @param op                    The write operation.
         */
        CoordinatorWriteEvent(
            String name,
            TopicPartition tp,
            Duration writeTimeout,
            CoordinatorWriteOperation<S, T, U> op
        ) {
            this(
                name,
                tp,
                null,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                VerificationGuard.SENTINEL,
                writeTimeout,
                op
            );
        }

        /**
         * Constructor.
         *
         * @param name                      The operation name.
         * @param tp                        The topic partition that the operation is applied to.
         * @param transactionalId           The transactional id.
         * @param producerId                The producer id.
         * @param producerEpoch             The producer epoch.
         * @param verificationGuard         The verification guard.
         * @param writeTimeout              The write operation timeout
         * @param op                        The write operation.
         */
        CoordinatorWriteEvent(
            String name,
            TopicPartition tp,
            String transactionalId,
            long producerId,
            short producerEpoch,
            VerificationGuard verificationGuard,
            Duration writeTimeout,
            CoordinatorWriteOperation<S, T, U> op
        ) {
            this.tp = tp;
            this.name = name;
            this.op = op;
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.verificationGuard = verificationGuard;
            this.future = new CompletableFuture<>();
            this.createdTimeMs = time.milliseconds();
            this.writeTimeout = writeTimeout;
            this.deferredEventQueuedTimestamp = NOT_QUEUED;
        }

        /**
         * @return The key used by the CoordinatorEventProcessor to ensure
         * that events with the same key are not processed concurrently.
         */
        @Override
        public TopicPartition key() {
            return tp;
        }

        /**
         * Called by the CoordinatorEventProcessor when the event is executed.
         */
        @Override
        public void run() {
            try {
                // Get the context of the coordinator or fail if the coordinator is not in active state.
                withActiveContextOrThrow(tp, context -> {
                    // Execute the operation.
                    result = op.generateRecordsAndResult(context.coordinator.coordinator());

                    // Append the records and replay them to the state machine.
                    context.append(
                        producerId,
                        producerEpoch,
                        verificationGuard,
                        result.records(),
                        result.replayRecords(),
                        result.isAtomic(),
                        this
                    );

                    // If the operation is not done, create an operation timeout.
                    if (!future.isDone()) {
                        operationTimeout = new OperationTimeout(tp, this, writeTimeout.toMillis());
                        timer.add(operationTimeout);

                        // Only update when this event was appended to the deferred queue.
                        deferredEventQueuedTimestamp = time.milliseconds();
                    }
                });
            } catch (Throwable t) {
                complete(t);
            }
        }

        /**
         * Completes the future with either the result of the write operation
         * or the provided exception.
         *
         * @param exception The exception to complete the future with.
         */
        @Override
        public void complete(Throwable exception) {
            final long purgatoryTimeMs = time.milliseconds() - deferredEventQueuedTimestamp;
            CompletableFuture<Void> appendFuture = result != null ? result.appendFuture() : null;

            if (exception == null) {
                if (appendFuture != null) result.appendFuture().complete(null);
                future.complete(result.response());
            } else {
                if (appendFuture != null) result.appendFuture().completeExceptionally(exception);
                future.completeExceptionally(exception);
            }

            if (operationTimeout != null) {
                operationTimeout.cancel();
                operationTimeout = null;
            }

            if (deferredEventQueuedTimestamp != NOT_QUEUED) {
                // Only record the purgatory time if the event was deferred.
                runtimeMetrics.recordEventPurgatoryTime(purgatoryTimeMs);
            }
        }

        @Override
        public long createdTimeMs() {
            return this.createdTimeMs;
        }

        @Override
        public String toString() {
            return "CoordinatorWriteEvent(name=" + name + ")";
        }
    }

    /**
     * A coordinator read operation.
     *
     * @param <S> The type of the coordinator state machine.
     * @param <T> The type of the response.
     */
    public interface CoordinatorReadOperation<S, T> {
        /**
         * Generates the response to implement this coordinator read operation. A read
         * operation received the last committed offset. It must use it to ensure that
         * it does not read uncommitted data from the timeline data structures.
         *
         * @param state     The coordinator state machine.
         * @param offset    The last committed offset.
         * @return A response.
         * @throws KafkaException
         */
        T generateResponse(S state, long offset) throws KafkaException;
    }

    /**
     * A coordinator that reads the committed coordinator state.
     *
     * @param <T> The type of the response.
     */
    class CoordinatorReadEvent<T> implements CoordinatorEvent {
        /**
         * The topic partition that this read event is applied to.
         */
        final TopicPartition tp;

        /**
         * The operation name.
         */
        final String name;

        /**
         * The read operation to execute.
         */
        final CoordinatorReadOperation<S, T> op;

        /**
         * The future that will be completed with the response
         * generated by the read operation or an error.
         */
        final CompletableFuture<T> future;

        /**
         * The result of the read operation. It could be null
         * if an exception is thrown before it is assigned.
         */
        T response;

        /**
         * The time this event was created.
         */
        private final long createdTimeMs;

        /**
         * Constructor.
         *
         * @param name  The operation name.
         * @param tp    The topic partition that the operation is applied to.
         * @param op    The read operation.
         */
        CoordinatorReadEvent(
            String name,
            TopicPartition tp,
            CoordinatorReadOperation<S, T> op
        ) {
            this.tp = tp;
            this.name = name;
            this.op = op;
            this.future = new CompletableFuture<>();
            this.createdTimeMs = time.milliseconds();
        }

        /**
         * @return The key used by the CoordinatorEventProcessor to ensure
         * that events with the same key are not processed concurrently.
         */
        @Override
        public TopicPartition key() {
            return tp;
        }

        /**
         * Called by the CoordinatorEventProcessor when the event is executed.
         */
        @Override
        public void run() {
            try {
                // Get the context of the coordinator or fail if the coordinator is not in active state.
                withActiveContextOrThrow(tp, context -> {
                    // Execute the read operation.
                    response = op.generateResponse(
                        context.coordinator.coordinator(),
                        context.coordinator.lastCommittedOffset()
                    );

                    // The response can be completed immediately.
                    complete(null);
                });
            } catch (Throwable t) {
                complete(t);
            }
        }

        /**
         * Completes the future with either the result of the read operation
         * or the provided exception.
         *
         * @param exception The exception to complete the future with.
         */
        @Override
        public void complete(Throwable exception) {
            if (exception == null) {
                future.complete(response);
            } else {
                future.completeExceptionally(exception);
            }
        }

        @Override
        public long createdTimeMs() {
            return this.createdTimeMs;
        }

        @Override
        public String toString() {
            return "CoordinatorReadEvent(name=" + name + ")";
        }
    }

    /**
     * A coordinator event that applies and writes a transaction end marker.
     */
    class CoordinatorCompleteTransactionEvent implements CoordinatorEvent, DeferredEvent {
        /**
         * The topic partition that this write event is applied to.
         */
        final TopicPartition tp;

        /**
         * The operation name.
         */
        final String name;

        /**
         * The producer id.
         */
        final long producerId;

        /**
         * The producer epoch.
         */
        final short producerEpoch;

        /**
         * The coordinator epoch of the transaction coordinator.
         */
        final int coordinatorEpoch;

        /**
         * The transaction result.
         */
        final TransactionResult result;

        /**
         * Timeout value for the write operation.
         */
        final Duration writeTimeout;

        /**
         * The operation timeout.
         */
        private OperationTimeout operationTimeout = null;

        /**
         * The future that will be completed with the response
         * generated by the write operation or an error.
         */
        final CompletableFuture<Void> future;

        /**
         * The time this event was created.
         */
        private final long createdTimeMs;

        /**
         * The time the records were appended to the log.
         */
        private long deferredEventQueuedTimestamp;

        CoordinatorCompleteTransactionEvent(
            String name,
            TopicPartition tp,
            long producerId,
            short producerEpoch,
            int coordinatorEpoch,
            TransactionResult result,
            Duration writeTimeout
        ) {
            this.name = name;
            this.tp = tp;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.coordinatorEpoch = coordinatorEpoch;
            this.result = result;
            this.writeTimeout = writeTimeout;
            this.future = new CompletableFuture<>();
            this.createdTimeMs = time.milliseconds();
            this.deferredEventQueuedTimestamp = NOT_QUEUED;
        }

        /**
         * @return The key used by the CoordinatorEventProcessor to ensure
         * that events with the same key are not processed concurrently.
         */
        @Override
        public TopicPartition key() {
            return tp;
        }

        /**
         * Called by the CoordinatorEventProcessor when the event is executed.
         */
        @Override
        public void run() {
            try {
                withActiveContextOrThrow(tp, context -> {
                    context.completeTransaction(
                        producerId,
                        producerEpoch,
                        coordinatorEpoch,
                        result,
                        this
                    );

                    if (!future.isDone()) {
                        operationTimeout = new OperationTimeout(tp, this, writeTimeout.toMillis());
                        timer.add(operationTimeout);

                        // Only update when this event was appended to the deferred queue.
                        deferredEventQueuedTimestamp = time.milliseconds();
                    }
                });
            } catch (Throwable t) {
                complete(t);
            }
        }

        /**
         * Completes the future with either the result of the write operation
         * or the provided exception.
         *
         * @param exception The exception to complete the future with.
         */
        @Override
        public void complete(Throwable exception) {
            final long purgatoryTimeMs = time.milliseconds() - deferredEventQueuedTimestamp;
            if (exception == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(exception);
            }

            if (operationTimeout != null) {
                operationTimeout.cancel();
                operationTimeout = null;
            }

            if (deferredEventQueuedTimestamp != NOT_QUEUED) {
                // Only record the purgatory time if the event was deferred.
                runtimeMetrics.recordEventPurgatoryTime(purgatoryTimeMs);
            }
        }

        @Override
        public long createdTimeMs() {
            return createdTimeMs;
        }

        @Override
        public String toString() {
            return "CoordinatorCompleteTransactionEvent(name=" + name + ")";
        }
    }

    /**
     * A coordinator internal event.
     */
    class CoordinatorInternalEvent implements CoordinatorEvent {
        /**
         * The topic partition that this internal event is applied to.
         */
        final TopicPartition tp;

        /**
         * The operation name.
         */
        final String name;

        /**
         * The internal operation to execute.
         */
        final Runnable op;

        /**
         * The time this event was created.
         */
        private final long createdTimeMs;

        /**
         * Constructor.
         *
         * @param name  The operation name.
         * @param tp    The topic partition that the operation is applied to.
         * @param op    The operation.
         */
        CoordinatorInternalEvent(
            String name,
            TopicPartition tp,
            Runnable op
        ) {
            this.tp = tp;
            this.name = name;
            this.op = op;
            this.createdTimeMs = time.milliseconds();
        }

        /**
         * @return The key used by the CoordinatorEventProcessor to ensure
         * that events with the same key are not processed concurrently.
         */
        @Override
        public TopicPartition key() {
            return tp;
        }

        /**
         * Called by the CoordinatorEventProcessor when the event is executed.
         */
        @Override
        public void run() {
            try {
                op.run();
            } catch (Throwable t) {
                complete(t);
            }
        }

        /**
         * Logs any exceptions thrown while the event is executed.
         *
         * @param exception The exception.
         */
        @Override
        public void complete(Throwable exception) {
            if (exception != null) {
                log.error("Execution of {} failed due to {}.", name, exception.getMessage(), exception);
            }
        }

        @Override
        public long createdTimeMs() {
            return this.createdTimeMs;
        }

        @Override
        public String toString() {
            return "InternalEvent(name=" + name + ")";
        }
    }

    /**
     * Partition listener to be notified when the high watermark of the partitions
     * backing the coordinator are updated.
     */
    class HighWatermarkListener implements PartitionWriter.Listener {

        static final long NO_OFFSET = -1L;

        /**
         * The atomic long is used to store the last and unprocessed high watermark
         * received from the partition. The atomic value is replaced by -1L when
         * the high watermark is taken to update the context.
         */
        private final AtomicLong lastHighWatermark = new AtomicLong(NO_OFFSET);

        /**
         * @return The last high watermark received or NO_OFFSET if none is pending.
         */
        public long lastHighWatermark() {
            return lastHighWatermark.get();
        }

        /**
         * Updates the high watermark of the corresponding coordinator.
         *
         * @param tp        The topic partition.
         * @param offset    The new high watermark.
         */
        @Override
        public void onHighWatermarkUpdated(
            TopicPartition tp,
            long offset
        ) {
            log.debug("High watermark of {} incremented to {}.", tp, offset);
            if (lastHighWatermark.getAndSet(offset) == NO_OFFSET) {
                // An event to apply the new high watermark is pushed to the front of the
                // queue only if the previous value was -1L. If it was not, it means that
                // there is already an event waiting to process the last value.
                enqueueFirst(new CoordinatorInternalEvent("HighWatermarkUpdate", tp, () -> {
                    long newHighWatermark = lastHighWatermark.getAndSet(NO_OFFSET);

                    CoordinatorContext context = coordinators.get(tp);
                    if (context != null) {
                        context.lock.lock();
                        try {
                            if (context.state == CoordinatorState.ACTIVE) {
                                // The updated high watermark can be applied to the coordinator only if the coordinator
                                // exists and is in the active state.
                                log.debug("Updating high watermark of {} to {}.", tp, newHighWatermark);
                                context.coordinator.updateLastCommittedOffset(newHighWatermark);
                                context.deferredEventQueue.completeUpTo(newHighWatermark);
                                coordinatorMetrics.onUpdateLastCommittedOffset(tp, newHighWatermark);
                            } else {
                                log.debug("Ignored high watermark updated for {} to {} because the coordinator is not active.",
                                    tp, newHighWatermark);
                            }
                        } finally {
                            context.lock.unlock();
                        }
                    } else {
                        log.debug("Ignored high watermark updated for {} to {} because the coordinator does not exist.",
                            tp, newHighWatermark);
                    }
                }));
            }
        }
    }

    /**
     * 16KB. Used for initial buffer size for write operations.
     */
    static final int MIN_BUFFER_SIZE = 16384;

    /**
     * The log prefix.
     */
    private final String logPrefix;

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final Timer timer;

    /**
     * The write operation timeout
     */
    private final Duration defaultWriteTimeout;

    /**
     * The coordinators keyed by topic partition.
     */
    private final ConcurrentHashMap<TopicPartition, CoordinatorContext> coordinators;

    /**
     * The event processor used by the runtime.
     */
    private final CoordinatorEventProcessor processor;

    /**
     * The partition writer used by the runtime to persist records.
     */
    private final PartitionWriter partitionWriter;

    /**
     * The coordinator loaded used by the runtime.
     */
    private final CoordinatorLoader<U> loader;

    /**
     * The coordinator state machine builder used by the runtime
     * to instantiate a coordinator.
     */
    private final CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier;

    /**
     * The coordinator runtime metrics.
     */
    private final CoordinatorRuntimeMetrics runtimeMetrics;

    /**
     * The coordinator metrics.
     */
    private final CoordinatorMetrics coordinatorMetrics;

    /**
     * The serializer used to serialize records.
     */
    private final Serializer<U> serializer;

    /**
     * The compression codec used when writing records.
     */
    private final Compression compression;

    /**
     * The duration in milliseconds that the coordinator will wait for writes to
     * accumulate before flushing them to disk.
     */
    private final int appendLingerMs;

    /**
     * The executor service used by the coordinator runtime to schedule
     * asynchronous tasks.
     */
    private final ExecutorService executorService;

    /**
     * Atomic boolean indicating whether the runtime is running.
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * The latest known metadata image.
     */
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;

    /**
     * Constructor.
     *
     * @param logPrefix                         The log prefix.
     * @param logContext                        The log context.
     * @param processor                         The event processor.
     * @param partitionWriter                   The partition writer.
     * @param loader                            The coordinator loader.
     * @param coordinatorShardBuilderSupplier   The coordinator builder.
     * @param time                              The system time.
     * @param timer                             The system timer.
     * @param defaultWriteTimeout               The write operation timeout.
     * @param runtimeMetrics                    The runtime metrics.
     * @param coordinatorMetrics                The coordinator metrics.
     * @param serializer                        The serializer.
     * @param compression                       The compression codec.
     * @param appendLingerMs                    The append linger time in ms.
     * @param executorService                   The executor service.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private CoordinatorRuntime(
        String logPrefix,
        LogContext logContext,
        CoordinatorEventProcessor processor,
        PartitionWriter partitionWriter,
        CoordinatorLoader<U> loader,
        CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier,
        Time time,
        Timer timer,
        Duration defaultWriteTimeout,
        CoordinatorRuntimeMetrics runtimeMetrics,
        CoordinatorMetrics coordinatorMetrics,
        Serializer<U> serializer,
        Compression compression,
        int appendLingerMs,
        ExecutorService executorService
    ) {
        this.logPrefix = logPrefix;
        this.logContext = logContext;
        this.log = logContext.logger(CoordinatorRuntime.class);
        this.time = time;
        this.timer = timer;
        this.defaultWriteTimeout = defaultWriteTimeout;
        this.coordinators = new ConcurrentHashMap<>();
        this.processor = processor;
        this.partitionWriter = partitionWriter;
        this.loader = loader;
        this.coordinatorShardBuilderSupplier = coordinatorShardBuilderSupplier;
        this.runtimeMetrics = runtimeMetrics;
        this.coordinatorMetrics = coordinatorMetrics;
        this.serializer = serializer;
        this.compression = compression;
        this.appendLingerMs = appendLingerMs;
        this.executorService = executorService;
    }

    /**
     * Throws a NotCoordinatorException exception if the runtime is not
     * running.
     */
    private void throwIfNotRunning() {
        if (!isRunning.get()) {
            throw Errors.NOT_COORDINATOR.exception();
        }
    }

    /**
     * Enqueues a new event at the end of the processing queue.
     *
     * @param event The event.
     * @throws NotCoordinatorException If the event processor is closed.
     */
    private void enqueueLast(CoordinatorEvent event) {
        try {
            processor.enqueueLast(event);
        } catch (RejectedExecutionException ex) {
            throw new NotCoordinatorException("Can't accept an event because the processor is closed", ex);
        }
    }

    /**
     * Enqueues a new event at the front of the processing queue.
     *
     * @param event The event.
     * @throws NotCoordinatorException If the event processor is closed.
     */
    private void enqueueFirst(CoordinatorEvent event) {
        try {
            processor.enqueueFirst(event);
        } catch (RejectedExecutionException ex) {
            throw new NotCoordinatorException("Can't accept an event because the processor is closed", ex);
        }
    }

    /**
     * @return The coordinator context or a new context if it does not exist.
     * Package private for testing.
     */
    CoordinatorContext maybeCreateContext(TopicPartition tp) {
        return coordinators.computeIfAbsent(tp, CoordinatorContext::new);
    }

    /**
     * @return The coordinator context or thrown an exception if it does
     * not exist.
     * @throws NotCoordinatorException
     * Package private for testing.
     */
    CoordinatorContext contextOrThrow(TopicPartition tp) throws NotCoordinatorException {
        CoordinatorContext context = coordinators.get(tp);

        if (context == null) {
            throw Errors.NOT_COORDINATOR.exception();
        } else {
            return context;
        }
    }

    /**
     * Calls the provided function with the context iff the context is active; throws
     * an exception otherwise. This method ensures that the context lock is acquired
     * before calling the function and releases afterwards.
     *
     * @param tp    The topic partition.
     * @param func  The function that will receive the context.
     * @throws NotCoordinatorException
     * @throws CoordinatorLoadInProgressException
     */
    private void withActiveContextOrThrow(
        TopicPartition tp,
        Consumer<CoordinatorContext> func
    ) throws NotCoordinatorException, CoordinatorLoadInProgressException {
        CoordinatorContext context = contextOrThrow(tp);

        try {
            context.lock.lock();
            if (context.state == CoordinatorState.ACTIVE) {
                func.accept(context);
            } else if (context.state == CoordinatorState.LOADING) {
                throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception();
            } else {
                throw Errors.NOT_COORDINATOR.exception();
            }
        } finally {
            context.lock.unlock();
        }
    }

    /**
     * Schedules a write operation.
     *
     * @param name      The name of the write operation.
     * @param tp        The address of the coordinator (aka its topic-partitions).
     * @param timeout   The write operation timeout.
     * @param op        The write operation.
     *
     * @return A future that will be completed with the result of the write operation
     * when the operation is completed or an exception if the write operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> CompletableFuture<T> scheduleWriteOperation(
        String name,
        TopicPartition tp,
        Duration timeout,
        CoordinatorWriteOperation<S, T, U> op
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of write operation {}.", name);
        CoordinatorWriteEvent<T> event = new CoordinatorWriteEvent<>(name, tp, timeout, op);
        enqueueLast(event);
        return event.future;
    }

    /**
     * Schedule a write operation for each coordinator.
     *
     * @param name      The name of the write operation.
     * @param timeout   The write operation timeout.
     * @param op        The write operation.
     *
     * @return A list of futures where each future will be completed with the result of the write operation
     * when the operation is completed or an exception if the write operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> List<CompletableFuture<T>> scheduleWriteAllOperation(
        String name,
        Duration timeout,
        CoordinatorWriteOperation<S, T, U> op
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of write all operation {}.", name);
        return coordinators
            .keySet()
            .stream()
            .map(tp -> scheduleWriteOperation(name, tp, timeout, op))
            .collect(Collectors.toList());
    }

    /**
     * Schedules a transactional write operation.
     *
     * @param name              The name of the write operation.
     * @param tp                The address of the coordinator (aka its topic-partitions).
     * @param transactionalId   The transactional id.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param timeout           The write operation timeout.
     * @param op                The write operation.
     * @param apiVersion        The Version of the Txn_Offset_Commit request
     *
     * @return A future that will be completed with the result of the write operation
     * when the operation is completed or an exception if the write operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> CompletableFuture<T> scheduleTransactionalWriteOperation(
        String name,
        TopicPartition tp,
        String transactionalId,
        long producerId,
        short producerEpoch,
        Duration timeout,
        CoordinatorWriteOperation<S, T, U> op,
        Short apiVersion
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of transactional write operation {}.", name);
        return partitionWriter.maybeStartTransactionVerification(
            tp,
            transactionalId,
            producerId,
            producerEpoch,
            apiVersion
        ).thenCompose(verificationGuard -> {
            CoordinatorWriteEvent<T> event = new CoordinatorWriteEvent<>(
                name,
                tp,
                transactionalId,
                producerId,
                producerEpoch,
                verificationGuard,
                timeout,
                op
            );
            enqueueLast(event);
            return event.future;
        });
    }

    /**
     * Schedules the transaction completion.
     *
     * @param name              The name of the operation.
     * @param tp                The address of the coordinator (aka its topic-partitions).
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param coordinatorEpoch  The epoch of the transaction coordinator.
     * @param result            The transaction result.
     *
     * @return A future that will be completed with null when the operation is
     * completed or an exception if the operation failed.
     */
    public CompletableFuture<Void> scheduleTransactionCompletion(
        String name,
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        TransactionResult result,
        Duration timeout
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of transaction completion for {} with producer id={}, producer epoch={}, " +
            "coordinator epoch={} and transaction result={}.", tp, producerId, producerEpoch, coordinatorEpoch, result);
        CoordinatorCompleteTransactionEvent event = new CoordinatorCompleteTransactionEvent(
            name,
            tp,
            producerId,
            producerEpoch,
            coordinatorEpoch,
            result,
            timeout
        );
        enqueueLast(event);
        return event.future;
    }

    /**
     * Schedules a read operation.
     *
     * @param name  The name of the read operation.
     * @param tp    The address of the coordinator (aka its topic-partitions).
     * @param op    The read operation.
     *
     * @return A future that will be completed with the result of the read operation
     * when the operation is completed or an exception if the read operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> CompletableFuture<T> scheduleReadOperation(
        String name,
        TopicPartition tp,
        CoordinatorReadOperation<S, T> op
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of read operation {}.", name);
        CoordinatorReadEvent<T> event = new CoordinatorReadEvent<>(name, tp, op);
        enqueueLast(event);
        return event.future;
    }

    /**
     * Schedules a read operation for each coordinator.
     *
     * @param name  The name of the read operation.
     * @param op    The read operation.
     *
     * @return A list of futures where each future will be completed with the result of the read operation
     * when the operation is completed or an exception if the read operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> List<CompletableFuture<T>> scheduleReadAllOperation(
        String name,
        CoordinatorReadOperation<S, T> op
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of read all operation {}.", name);
        return coordinators
            .keySet()
            .stream()
            .map(tp -> scheduleReadOperation(name, tp, op))
            .collect(Collectors.toList());
    }

    /**
     * Schedules an internal event.
     *
     * @param name  The name of the write operation.
     * @param tp    The address of the coordinator (aka its topic-partitions).
     * @param op    The operation.
     */
    private void scheduleInternalOperation(
        String name,
        TopicPartition tp,
        Runnable op
    ) {
        log.debug("Scheduled execution of internal operation {}.", name);
        enqueueLast(new CoordinatorInternalEvent(name, tp, op));
    }

    /**
     * Schedules the loading of a coordinator. This is called when the broker is elected as
     * the leader for a partition.
     *
     * @param tp                The topic partition of the coordinator. Records from this
     *                          partitions will be read and applied to the coordinator.
     * @param partitionEpoch    The epoch of the partition.
     */
    public void scheduleLoadOperation(
        TopicPartition tp,
        int partitionEpoch
    ) {
        throwIfNotRunning();
        log.info("Scheduling loading of metadata from {} with epoch {}", tp, partitionEpoch);

        // Touch the state to make the runtime immediately aware of the new coordinator.
        maybeCreateContext(tp);

        scheduleInternalOperation("Load(tp=" + tp + ", epoch=" + partitionEpoch + ")", tp, () -> {
            // The context is re-created if it does not exist.
            CoordinatorContext context = maybeCreateContext(tp);

            context.lock.lock();
            try {
                if (context.epoch < partitionEpoch) {
                    context.epoch = partitionEpoch;

                    switch (context.state) {
                        case FAILED:
                        case INITIAL:
                            context.transitionTo(CoordinatorState.LOADING);
                            break;

                        case LOADING:
                            log.info("The coordinator {} is already loading metadata.", tp);
                            break;

                        case ACTIVE:
                            log.info("The coordinator {} is already active.", tp);
                            break;

                        default:
                            log.error("Cannot load coordinator {} in state {}.", tp, context.state);
                    }
                } else {
                    log.info("Ignored loading metadata from {} since current epoch {} is larger than or equals to {}.",
                        context.tp, context.epoch, partitionEpoch);
                }
            } finally {
                context.lock.unlock();
            }
        });
    }

    /**
     * Schedules the unloading of a coordinator. This is called when the broker is not the
     * leader anymore.
     *
     * @param tp                The topic partition of the coordinator.
     * @param partitionEpoch    The partition epoch as an optional value.
     *                          An empty value means that the topic was deleted.
     */
    public void scheduleUnloadOperation(
        TopicPartition tp,
        OptionalInt partitionEpoch
    ) {
        throwIfNotRunning();
        log.info("Scheduling unloading of metadata for {} with epoch {}", tp, partitionEpoch);

        scheduleInternalOperation("UnloadCoordinator(tp=" + tp + ", epoch=" + partitionEpoch + ")", tp, () -> {
            CoordinatorContext context = coordinators.get(tp);
            if (context != null) {
                context.lock.lock();
                try {
                    if (partitionEpoch.isEmpty() || context.epoch < partitionEpoch.getAsInt()) {
                        log.info("Started unloading metadata for {} with epoch {}.", tp, partitionEpoch);
                        context.transitionTo(CoordinatorState.CLOSED);
                        coordinators.remove(tp, context);
                        log.info("Finished unloading metadata for {} with epoch {}.", tp, partitionEpoch);
                    } else {
                        log.info("Ignored unloading metadata for {} in epoch {} since current epoch is {}.",
                            tp, partitionEpoch, context.epoch);
                    }
                } finally {
                    context.lock.unlock();
                }
            } else {
                log.info("Ignored unloading metadata for {} in epoch {} since metadata was never loaded.",
                    tp, partitionEpoch);
            }
        });
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The metadata delta.
     */
    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        throwIfNotRunning();
        log.debug("Scheduling applying of a new metadata image with offset {}.", newImage.offset());

        // Update global image.
        metadataImage = newImage;

        // Push an event for each coordinator.
        coordinators.keySet().forEach(tp -> {
            scheduleInternalOperation("UpdateImage(tp=" + tp + ", offset=" + newImage.offset() + ")", tp, () -> {
                CoordinatorContext context = coordinators.get(tp);
                if (context != null) {
                    context.lock.lock();
                    try {
                        if (context.state == CoordinatorState.ACTIVE) {
                            // The new image can be applied to the coordinator only if the coordinator
                            // exists and is in the active state.
                            log.debug("Applying new metadata image with offset {} to {}.", newImage.offset(), tp);
                            context.coordinator.onNewMetadataImage(newImage, delta);
                        } else {
                            log.debug("Ignored new metadata image with offset {} for {} because the coordinator is not active.",
                                newImage.offset(), tp);
                        }
                    } finally {
                        context.lock.unlock();
                    }
                } else {
                    log.debug("Ignored new metadata image with offset {} for {} because the coordinator does not exist.",
                        newImage.offset(), tp);
                }
            });
        });
    }

    /**
     * Closes the runtime. This closes all the coordinators currently registered
     * in the runtime.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        if (!isRunning.compareAndSet(true, false)) {
            log.warn("Coordinator runtime is already shutting down.");
            return;
        }

        log.info("Closing coordinator runtime.");
        Utils.closeQuietly(loader, "loader");
        Utils.closeQuietly(timer, "timer");
        // This close the processor, drain all the pending events and
        // reject any new events.
        Utils.closeQuietly(processor, "event processor");
        // Unload all the coordinators.
        coordinators.forEach((tp, context) -> {
            context.lock.lock();
            try {
                context.transitionTo(CoordinatorState.CLOSED);
            } finally {
                context.lock.unlock();
            }
        });
        coordinators.clear();
        executorService.shutdown();
        Utils.closeQuietly(runtimeMetrics, "runtime metrics");
        log.info("Coordinator runtime closed.");
    }
}
