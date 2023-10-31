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
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.metrics.CoordinatorRuntimeMetrics;
import org.apache.kafka.deferred.DeferredEvent;
import org.apache.kafka.deferred.DeferredEventQueue;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
        private PartitionWriter<U> partitionWriter;
        private CoordinatorLoader<U> loader;
        private CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier;
        private Time time = Time.SYSTEM;
        private Timer timer;
        private CoordinatorRuntimeMetrics runtimeMetrics;

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

        public Builder<S, U> withPartitionWriter(PartitionWriter<U> partitionWriter) {
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

        public Builder<S, U> withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics runtimeMetrics) {
            this.runtimeMetrics = runtimeMetrics;
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

            return new CoordinatorRuntime<>(
                logPrefix,
                logContext,
                eventProcessor,
                partitionWriter,
                loader,
                coordinatorShardBuilderSupplier,
                time,
                timer,
                runtimeMetrics
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
                return state == LOADING;
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
            // The TimerTask wraps the TimeoutOperation into a CoordinatorWriteEvent. When the TimerTask
            // expires, the event is pushed to the queue of the coordinator runtime to be executed. This
            // ensures that the threading model of the runtime is respected.
            TimerTask task = new TimerTask(unit.toMillis(delay)) {
                @Override
                public void run() {
                    String eventName = "Timeout(tp=" + tp + ", key=" + key + ")";
                    CoordinatorWriteEvent<Void> event = new CoordinatorWriteEvent<>(eventName, tp, coordinator -> {
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
                            schedule(key, 500, TimeUnit.MILLISECONDS, retry, operation);
                        } else {
                            log.error("The write event {} for the timer {} failed due to {}. Ignoring it. ",
                                event.name, key, ex.getMessage());
                        }

                        return null;
                    });

                    log.debug("Scheduling write event {} for timer {}.", event.name, key);
                    try {
                        enqueue(event);
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
         * The current state.
         */
        CoordinatorState state;

        /**
         * The current epoch of the coordinator. This represents
         * the epoch of the partition leader.
         */
        int epoch;

        /**
         * The snapshot registry backing the coordinator.
         */
        SnapshotRegistry snapshotRegistry;

        /**
         * The actual state machine.
         */
        S coordinator;

        /**
         * The last offset written to the partition.
         */
        long lastWrittenOffset;

        /**
         * The last offset committed. This represents the high
         * watermark of the partition.
         */
        long lastCommittedOffset;

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
        }

        /**
         * Updates the last written offset. This also create a new snapshot
         * in the snapshot registry.
         *
         * @param offset The new last written offset.
         */
        private void updateLastWrittenOffset(
            long offset
        ) {
            if (offset <= lastWrittenOffset) {
                throw new IllegalStateException("New last written offset " + offset + " of " + tp +
                    " must be larger than " + lastWrittenOffset + ".");
            }

            log.debug("Update last written offset of {} to {}.", tp, offset);
            lastWrittenOffset = offset;
            snapshotRegistry.getOrCreateSnapshot(offset);
        }

        /**
         * Reverts the last written offset. This also reverts the snapshot
         * registry to this offset. All the changes applied after the offset
         * are lost.
         *
         * @param offset The offset to revert to.
         */
        private void revertLastWrittenOffset(
            long offset
        ) {
            if (offset > lastWrittenOffset) {
                throw new IllegalStateException("New offset " + offset + " of " + tp +
                    " must be smaller than " + lastWrittenOffset + ".");
            }

            log.debug("Revert last written offset of {} to {}.", tp, offset);
            lastWrittenOffset = offset;
            snapshotRegistry.revertToSnapshot(offset);
        }

        /**
         * Updates the last committed offset. This completes all the deferred
         * events waiting on this offset. This also cleanups all the snapshots
         * prior to this offset.
         *
         * @param offset The new last committed offset.
         */
        private void updateLastCommittedOffset(
            long offset
        ) {
            if (offset <= lastCommittedOffset) {
                throw new IllegalStateException("New committed offset " + offset + " of " + tp +
                    " must be larger than " + lastCommittedOffset + ".");
            }

            log.debug("Update committed offset of {} to {}.", tp, offset);
            lastCommittedOffset = offset;
            deferredEventQueue.completeUpTo(offset);
            snapshotRegistry.deleteSnapshotsUpTo(offset);
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
                    snapshotRegistry = new SnapshotRegistry(logContext);
                    lastWrittenOffset = 0L;
                    lastCommittedOffset = 0L;
                    coordinator = coordinatorShardBuilderSupplier
                        .get()
                        .withLogContext(logContext)
                        .withSnapshotRegistry(snapshotRegistry)
                        .withTime(time)
                        .withTimer(timer)
                        .build();
                    break;

                case ACTIVE:
                    state = CoordinatorState.ACTIVE;
                    snapshotRegistry.getOrCreateSnapshot(0);
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
         * Unloads the coordinator.
         */
        private void unload() {
            partitionWriter.deregisterListener(tp, highWatermarklistener);
            timer.cancelAll();
            deferredEventQueue.failAll(Errors.NOT_COORDINATOR.exception());
            if (coordinator != null) {
                coordinator.onUnloaded();
            }
            coordinator = null;
            snapshotRegistry = null;
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
         * The topic partition that this write event is applied to.
         */
        final TopicPartition tp;

        /**
         * The operation name.
         */
        final String name;

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
         * The result of the write operation. It could be null
         * if an exception is thrown before it is assigned.
         */
        CoordinatorResult<T, U> result;

        /**
         * The time this event was created.
         */
        private final long createdTimeMs;

        /**
         * Constructor.
         *
         * @param name  The operation name.
         * @param tp    The topic partition that the operation is applied to.
         * @param op    The write operation.
         */
        CoordinatorWriteEvent(
            String name,
            TopicPartition tp,
            CoordinatorWriteOperation<S, T, U> op
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
                    long prevLastWrittenOffset = context.lastWrittenOffset;

                    // Execute the operation.
                    result = op.generateRecordsAndResult(context.coordinator);

                    if (result.records().isEmpty()) {
                        // If the records are empty, it was a read operation after all. In this case,
                        // the response can be returned directly iff there are no pending write operations;
                        // otherwise, the read needs to wait on the last write operation to be completed.
                        OptionalLong pendingOffset = context.deferredEventQueue.highestPendingOffset();
                        if (pendingOffset.isPresent()) {
                            context.deferredEventQueue.add(pendingOffset.getAsLong(), this);
                        } else {
                            complete(null);
                        }
                    } else {
                        // If the records are not empty, first, they are applied to the state machine,
                        // second, then are written to the partition/log, and finally, the response
                        // is put into the deferred event queue.
                        try {
                            // Apply the records to the state machine.
                            if (result.replayRecords()) {
                                result.records().forEach(context.coordinator::replay);
                            }

                            // Write the records to the log and update the last written
                            // offset.
                            long offset = partitionWriter.append(tp, result.records());
                            context.updateLastWrittenOffset(offset);

                            // Add the response to the deferred queue.
                            if (!future.isDone()) {
                                context.deferredEventQueue.add(offset, this);
                            } else {
                                complete(null);
                            }
                        } catch (Throwable t) {
                            context.revertLastWrittenOffset(prevLastWrittenOffset);
                            complete(t);
                        }
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
            CompletableFuture<Void> appendFuture = result != null ? result.appendFuture() : null;

            if (exception == null) {
                if (appendFuture != null) result.appendFuture().complete(null);
                future.complete(result.response());
            } else {
                if (appendFuture != null) result.appendFuture().completeExceptionally(exception);
                future.completeExceptionally(exception);
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
                        context.coordinator,
                        context.lastCommittedOffset
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
            scheduleInternalOperation("HighWatermarkUpdated(tp=" + tp + ", offset=" + offset + ")", tp, () -> {
                withActiveContextOrThrow(tp, context -> {
                    context.updateLastCommittedOffset(offset);
                });
            });
        }
    }

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
    private final PartitionWriter<U> partitionWriter;

    /**
     * The high watermark listener registered to all the partitions
     * backing the coordinators.
     */
    private final HighWatermarkListener highWatermarklistener;

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
     */
    private CoordinatorRuntime(
        String logPrefix,
        LogContext logContext,
        CoordinatorEventProcessor processor,
        PartitionWriter<U> partitionWriter,
        CoordinatorLoader<U> loader,
        CoordinatorShardBuilderSupplier<S, U> coordinatorShardBuilderSupplier,
        Time time,
        Timer timer,
        CoordinatorRuntimeMetrics runtimeMetrics
    ) {
        this.logPrefix = logPrefix;
        this.logContext = logContext;
        this.log = logContext.logger(CoordinatorRuntime.class);
        this.time = time;
        this.timer = timer;
        this.coordinators = new ConcurrentHashMap<>();
        this.processor = processor;
        this.partitionWriter = partitionWriter;
        this.highWatermarklistener = new HighWatermarkListener();
        this.loader = loader;
        this.coordinatorShardBuilderSupplier = coordinatorShardBuilderSupplier;
        this.runtimeMetrics = runtimeMetrics;
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
     * Enqueues a new event.
     *
     * @param event The event.
     * @throws NotCoordinatorException If the event processor is closed.
     */
    private void enqueue(CoordinatorEvent event) {
        try {
            processor.enqueue(event);
        } catch (RejectedExecutionException ex) {
            throw new NotCoordinatorException("Can't accept an event because the processor is closed", ex);
        }
    }

    /**
     * Creates the context if it does not exist.
     *
     * @param tp    The topic partition.
     */
    private void maybeCreateContext(TopicPartition tp) {
        coordinators.computeIfAbsent(tp, CoordinatorContext::new);
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
     * Calls the provided function with the context; throws an exception otherwise.
     * This method ensures that the context lock is acquired before calling the
     * function and releases afterwards.
     *
     * @param tp    The topic partition.
     * @param func  The function that will receive the context.
     * @throws NotCoordinatorException
     */
    private void withContextOrThrow(
        TopicPartition tp,
        Consumer<CoordinatorContext> func
    ) throws NotCoordinatorException {
        CoordinatorContext context = contextOrThrow(tp);

        try {
            context.lock.lock();
            func.accept(context);
        } finally {
            context.lock.unlock();
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
     * @param name  The name of the write operation.
     * @param tp    The address of the coordinator (aka its topic-partitions).
     * @param op    The write operation.
     *
     * @return A future that will be completed with the result of the write operation
     * when the operation is completed or an exception if the write operation failed.
     *
     * @param <T> The type of the result.
     */
    public <T> CompletableFuture<T> scheduleWriteOperation(
        String name,
        TopicPartition tp,
        CoordinatorWriteOperation<S, T, U> op
    ) {
        throwIfNotRunning();
        log.debug("Scheduled execution of write operation {}.", name);
        CoordinatorWriteEvent<T> event = new CoordinatorWriteEvent<>(name, tp, op);
        enqueue(event);
        return event.future;
    }

    /**
     * Schedules a read operation.
     *
     * @param name  The name of the write operation.
     * @param tp    The address of the coordinator (aka its topic-partitions).
     * @param op    The read operation.
     *
     * @return A future that will be completed with the result of the read operation
     * when the operation is completed or an exception if the write operation failed.
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
        enqueue(event);
        return event.future;
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
        enqueue(new CoordinatorInternalEvent(name, tp, op));
    }

    /**
     * @return The topic partitions of the coordinators currently registered in the
     * runtime.
     */
    public Set<TopicPartition> partitions() {
        throwIfNotRunning();
        return new HashSet<>(coordinators.keySet());
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
            withContextOrThrow(tp, context -> {
                if (context.epoch < partitionEpoch) {
                    context.epoch = partitionEpoch;

                    switch (context.state) {
                        case FAILED:
                        case INITIAL:
                            context.transitionTo(CoordinatorState.LOADING);
                            loader.load(tp, context.coordinator).whenComplete((summary, exception) -> {
                                scheduleInternalOperation("CompleteLoad(tp=" + tp + ", epoch=" + partitionEpoch + ")", tp, () -> {
                                    withContextOrThrow(tp, ctx -> {
                                        if (ctx.state != CoordinatorState.LOADING) {
                                            log.info("Ignoring load completion from {} because context is in {} state.",
                                                ctx.tp, ctx.state
                                            );
                                            return;
                                        }
                                        try {
                                            if (exception != null) throw exception;
                                            ctx.transitionTo(CoordinatorState.ACTIVE);
                                            if (summary != null) {
                                                runtimeMetrics.recordPartitionLoadSensor(summary.startTimeMs(), summary.endTimeMs());
                                            }
                                            log.info("Finished loading of metadata from {} with epoch {} and LoadSummary={}.",
                                                tp, partitionEpoch, summary
                                            );
                                        } catch (Throwable ex) {
                                            log.error("Failed to load metadata from {} with epoch {} due to {}.",
                                                tp, partitionEpoch, ex.toString()
                                            );
                                            ctx.transitionTo(CoordinatorState.FAILED);
                                        }
                                    });
                                });
                            });
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
                    log.info("Ignoring loading metadata from {} since current epoch {} is larger than or equals to {}.",
                        context.tp, context.epoch, partitionEpoch
                    );
                }
            });
        });
    }

    /**
     * Schedules the unloading of a coordinator. This is called when the broker is not the
     * leader anymore.
     *
     * @param tp                The topic partition of the coordinator.
     * @param partitionEpoch    The partition epoch.
     */
    public void scheduleUnloadOperation(
        TopicPartition tp,
        int partitionEpoch
    ) {
        throwIfNotRunning();
        log.info("Scheduling unloading of metadata for {} with epoch {}", tp, partitionEpoch);

        scheduleInternalOperation("UnloadCoordinator(tp=" + tp + ", epoch=" + partitionEpoch + ")", tp, () -> {
            CoordinatorContext context = coordinators.get(tp);
            if (context != null) {
                try {
                    context.lock.lock();
                    if (context.epoch < partitionEpoch) {
                        log.info("Started unloading metadata for {} with epoch {}.", tp, partitionEpoch);
                        context.transitionTo(CoordinatorState.CLOSED);
                        coordinators.remove(tp, context);
                        log.info("Finished unloading metadata for {} with epoch {}.", tp, partitionEpoch);
                    } else {
                        log.info("Ignored unloading metadata for {} in epoch {} since current epoch is {}.",
                            tp, partitionEpoch, context.epoch
                        );
                    }
                } finally {
                    context.lock.unlock();
                }
            } else {
                log.info("Ignored unloading metadata for {} in epoch {} since metadata was never loaded.",
                    tp, partitionEpoch
                );
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
                withContextOrThrow(tp, context -> {
                    if (context.state == CoordinatorState.ACTIVE) {
                        log.debug("Applying new metadata image with offset {} to {}.", newImage.offset(), tp);
                        context.coordinator.onNewMetadataImage(newImage, delta);
                    } else {
                        log.debug("Ignoring new metadata image with offset {} for {} because the coordinator is not active.",
                            newImage.offset(), tp);
                    }
                });
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
            context.transitionTo(CoordinatorState.CLOSED);
        });
        coordinators.clear();
        Utils.closeQuietly(runtimeMetrics, "runtime metrics");
        log.info("Coordinator runtime closed.");
    }
}
