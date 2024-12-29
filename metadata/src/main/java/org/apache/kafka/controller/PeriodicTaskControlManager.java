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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.errors.PeriodicControlTaskException;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;


class PeriodicTaskControlManager {
    static class Builder {
        private LogContext logContext = null;
        private Time time = Time.SYSTEM;
        private QueueAccessor queueAccessor = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        Builder setQueueAccessor(QueueAccessor queueAccessor) {
            this.queueAccessor = queueAccessor;
            return this;
        }

        PeriodicTaskControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (queueAccessor == null) throw new RuntimeException("You must set queueAccessor");
            return new PeriodicTaskControlManager(logContext,
                    time,
                    queueAccessor);
        }
    }

    interface QueueAccessor {
        void scheduleDeferred(
            String tag,
            long deadlineNs,
            Supplier<ControllerResult<Void>> op
        );

        void cancelDeferred(String tag);
    }

    class PeriodicTaskOperation implements Supplier<ControllerResult<Void>> {
        private final PeriodicTask task;

        PeriodicTaskOperation(PeriodicTask task) {
            this.task = task;
        }

        @Override
        public ControllerResult<Void> get() {
            long startNs = 0;
            if (log.isDebugEnabled() || task.flags().contains(PeriodicTaskFlag.VERBOSE)) {
                startNs = time.nanoseconds();
            }
            ControllerResult<Boolean> result;
            try {
                result = task.op().get();
            } catch (Exception e) {
                // Reschedule the task after a lengthy delay.
                reschedule(task, false, true);
                // We wrap the exception in a PeriodicControlTaskException before throwing it to ensure
                // that it is handled correctly in QuorumController::handleEventException. We want it to
                // cause the metadata error metric to be incremented, but not cause a controller failover.
                throw new PeriodicControlTaskException(task.name() + ": periodic task failed: " +
                    e.getMessage(), e);
            }
            if (log.isDebugEnabled() || task.flags().contains(PeriodicTaskFlag.VERBOSE)) {
                long endNs = time.nanoseconds();
                long durationUs = NANOSECONDS.toMicros(endNs - startNs);
                if (task.flags().contains(PeriodicTaskFlag.VERBOSE)) {
                    log.info("Periodic task {} generated {} records in {} microseconds.",
                            task.name(), result.records().size(), durationUs);
                } else if (log.isDebugEnabled()) {
                    log.debug("Periodic task {} generated {} records in {} microseconds.",
                            task.name(), result.records().size(), durationUs);
                }
            }
            reschedule(task, result.response(), false);
            if (result.isAtomic()) {
                return ControllerResult.atomicOf(result.records(), null);
            } else {
                return ControllerResult.of(result.records(), null);
            }
        }
    }

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The clock.
     */
    private final Time time;

    /**
     * Used to schedule events on the queue.
     */
    private final QueueAccessor queueAccessor;

    /**
     * True if the manager is active.
     */
    private boolean active;

    /**
     * The currently registered periodic tasks.
     */
    private final Map<String, PeriodicTask> tasks;

    private PeriodicTaskControlManager(
        LogContext logContext,
        Time time,
        QueueAccessor queueAccessor
    ) {
        this.log = logContext.logger(PeriodicTaskControlManager.class);
        this.time = time;
        this.queueAccessor = queueAccessor;
        this.active = false;
        this.tasks = new HashMap<>();
    }

    boolean active() {
        return active;
    }

    void registerTask(PeriodicTask task) {
        if (tasks.containsKey(task.name())) {
            log.debug("Periodic task {} is already registered.", task.name());
            return;
        }
        tasks.put(task.name(), task);
        log.info("Registering periodic task {} to run every {} ms", task.name(),
                NANOSECONDS.toMillis(task.periodNs()));
        reschedule(task, false, false);
    }

    void unregisterTask(String taskName) {
        PeriodicTask task = tasks.remove(taskName);
        if (task == null) {
            log.debug("Periodic task {} is already unregistered.", taskName);
            return;
        }
        log.info("Unregistering periodic task {}", taskName);
        reschedule(task, false, false);
    }

    private long nextDelayTimeNs(PeriodicTask task, boolean immediate, boolean error) {
        if (immediate) {
            // The current implementation of KafkaEventQueue always picks from the deferred
            // collection of operations before picking from the non-deferred collection of
            // operations. This can result in some unfairness if deferred operation are
            // scheduled for immediate execution. This delays them by a small amount of time.
            return MILLISECONDS.toNanos(10);
        } else if (error) {
            // If the periodic task hit an error, reschedule it in 5 minutes. This is to avoid
            // scenarios where we spin in a tight loop hitting errors, but still give the task
            // a chance to succeed.
            return MINUTES.toNanos(5);
        } else {
            // Otherwise, use the designated period.
            return task.periodNs();
        }
    }

    private void reschedule(PeriodicTask task, boolean immediate, boolean error) {
        if (!active) {
            log.trace("cancelling {} because we are inactive.", task.name());
            queueAccessor.cancelDeferred(task.name());
        } else if (tasks.containsKey(task.name())) {
            long nextDelayTimeNs = nextDelayTimeNs(task, immediate, error);
            long nextRunTimeNs = time.nanoseconds() + nextDelayTimeNs;
            log.trace("rescheduling {} in {} ns (immediate = {}, error = {})",
                    task.name(), nextDelayTimeNs, immediate, error);
            queueAccessor.scheduleDeferred(task.name(),
                    nextRunTimeNs,
                    new PeriodicTaskOperation(task));
        } else {
            log.trace("cancelling {} because it does not appear in the task map.", task.name());
            queueAccessor.cancelDeferred(task.name());
        }
    }

    /**
     * Called when the QuorumController becomes active.
     */
    void activate() {
        if (active) {
            throw new RuntimeException("Can't activate already active PeriodicTaskControlManager.");
        }
        active = true;
        for (PeriodicTask task : tasks.values()) {
            reschedule(task, false, false);
        }
        String[] taskNames = tasks.keySet().toArray(new String[0]);
        Arrays.sort(taskNames);
        log.info("Activated periodic tasks: {}", String.join(", ", taskNames));
    }

    /**
     * Called when the QuorumController becomes inactive.
     */
    void deactivate() {
        if (!active) {
            return;
        }
        active = false;
        for (PeriodicTask task : tasks.values()) {
            reschedule(task, false, false);
        }
        String[] taskNames = tasks.keySet().toArray(new String[0]);
        Arrays.sort(taskNames);
        log.info("Deactivated periodic tasks: {}", String.join(", ", taskNames));
    }
}
