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

import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An event-based coordinator timer that bridges Kafka's utility Timer class with the
 * coordinator runtime's event-driven architecture. It converts timeout expiries into
 * write operations that are scheduled through the runtime, ensuring all timeout operations
 * respect the coordinator's threading model.
 *
 * When a timer fails with an unexpected exception, the timer is rescheduled with a backoff.
 */
public class CoordinatorTimerImpl<U> implements CoordinatorTimer<U> {

    private record TimerState<U>(
        long deadlineMs,
        TimeoutOperation<U> operation,
        boolean retry,
        long retryBackoffMs
    ) { }

    private final Logger log;
    private final Timer timer;
    private final Time time;
    private final CoordinatorShardScheduler<U> scheduler;
    private final Map<String, TimerTask> tasks = new ConcurrentHashMap<>();
    private TimelineHashMap<String, TimerState<U>> states;

    public CoordinatorTimerImpl(
        LogContext logContext,
        Timer timer,
        CoordinatorShardScheduler<U> scheduler
    ) {
        this(logContext, timer, scheduler, Time.SYSTEM);
    }

    public CoordinatorTimerImpl(
        LogContext logContext,
        Timer timer,
        CoordinatorShardScheduler<U> scheduler,
        Time time
    ) {
        this(logContext, timer, scheduler, new SnapshotRegistry(logContext), time);
    }

    public CoordinatorTimerImpl(
        LogContext logContext,
        Timer timer,
        CoordinatorShardScheduler<U> scheduler,
        SnapshotRegistry snapshotRegistry,
        Time time
    ) {
        this.log = logContext.logger(CoordinatorTimerImpl.class);
        this.timer = timer;
        this.time = time;
        this.scheduler = scheduler;
        this.states = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Binds the timer state to a newly loaded shard's snapshot registry.
     * Existing tasks must be cancelled before reinitializing the state.
     * The caller must hold the coordinator context lock.
     */
    void initializeSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
        if (!tasks.isEmpty()) {
            throw new IllegalStateException("Cannot initialize the snapshot registry while timers are scheduled");
        }
        states = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    @Override
    public void schedule(
        String key,
        long delay,
        TimeUnit unit,
        boolean retry,
        TimeoutOperation<U> operation
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
        TimeoutOperation<U> operation
    ) {
        long delayMs = unit.toMillis(delay);
        TimerState<U> state = new TimerState<>(time.hiResClockMs() + delayMs, operation, retry, retryBackoff);
        states.put(key, state);
        scheduleTask(key, state, delayMs);
    }

    private void scheduleTask(String key, TimerState<U> state, long delayMs) {
        // The TimerTask wraps the TimeoutOperation into a write operation. When the TimerTask
        // expires, the operation is scheduled through the scheduler to be executed. This
        // ensures that the threading model of the runtime is respected.
        var task = new TimerTask(delayMs) {
            @Override
            public void run() {
                var operationName = "Timeout(key=" + key + ")";

                scheduler.scheduleWriteOperation(
                    operationName,
                    () -> {
                        log.debug("Executing write event {} for timer {}.", operationName, key);

                        // If the task is different, it means that the timer has been
                        // cancelled while the event was waiting to be processed.
                        if (!tasks.remove(key, this)) {
                            throw new RejectedExecutionException("Timer " + key + " was overridden or cancelled");
                        }
                        states.remove(key);

                        // Execute the timeout operation.
                        return state.operation().generateRecords();
                    }
                ).exceptionally(ex -> {
                    // Exceptions may be wrapped in CompletionException when propagated
                    // through CompletableFuture chains, so we unwrap them before
                    // checking types with instanceof.
                    ex = Errors.maybeUnwrapException(ex);

                    // Remove the task after a failure.
                    if (tasks.remove(key, this)) {
                        states.remove(key);
                    }

                    if (ex instanceof RejectedExecutionException) {
                        log.debug("The write event {} for the timer {} was not executed because it was " +
                            "cancelled or overridden.", operationName, key);
                        return null;
                    }

                    if (ex instanceof NotCoordinatorException || ex instanceof CoordinatorLoadInProgressException) {
                        log.debug("The write event {} for the timer {} failed due to {}. Ignoring it because " +
                            "the coordinator is not active.", operationName, key, ex.getMessage());
                        return null;
                    }

                    if (state.retry()) {
                        log.info("The write event {} for the timer {} failed due to {}. Rescheduling it. ",
                            operationName, key, ex.getMessage());
                        schedule(key, state.retryBackoffMs(), TimeUnit.MILLISECONDS, true,
                            state.retryBackoffMs(), state.operation());
                    } else {
                        log.error("The write event {} for the timer {} failed due to {}. Ignoring it. ",
                            operationName, key, ex.getMessage(), ex);
                    }

                    return null;
                });

                log.debug("Scheduling write event {} for timer {}.", operationName, key);
            }
        };

        log.debug("Registering timer {} with delay of {}ms.", key, delayMs);
        var prevTask = tasks.put(key, task);
        if (prevTask != null) prevTask.cancel();

        timer.add(task);
    }

    @Override
    public void scheduleIfAbsent(
        String key,
        long delay,
        TimeUnit unit,
        boolean retry,
        TimeoutOperation<U> operation
    ) {
        if (!tasks.containsKey(key)) {
            schedule(key, delay, unit, retry, 500, operation);
        }
    }

    @Override
    public void cancel(String key) {
        states.remove(key);
        var prevTask = tasks.remove(key);
        if (prevTask != null) prevTask.cancel();
    }

    @Override
    public boolean isScheduled(String key) {
        return tasks.containsKey(key);
    }

    public void cancelAll() {
        states.clear();
        cancelTasks();
    }

    /**
     * Rebuilds the scheduled tasks after the supplied snapshot registry has been reverted.
     * This must be called under the same external synchronization as the registry rollback.
     * It preserves the restored states and their original deadlines.
     */
    public void onRollback() {
        cancelTasks();
        // A timer implementation may execute an expired task immediately, so iterate over
        // a copy in case that task updates the states while we are rescheduling them.
        new HashMap<>(states).forEach(
            (key, state) ->
                scheduleTask(
                    key,
                    state,
                    Math.max(0, state.deadlineMs() - time.hiResClockMs())
                )
        );
    }

    private void cancelTasks() {
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
