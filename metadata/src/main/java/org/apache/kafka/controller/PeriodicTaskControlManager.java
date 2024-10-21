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

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
            ControllerResult<Boolean> result = task.op().get();
            if (log.isDebugEnabled() || task.flags().contains(PeriodicTaskFlag.VERBOSE)) {
                long endNs = time.nanoseconds();
                long durationUs = TimeUnit.NANOSECONDS.toMicros(endNs - startNs);
                if (task.flags().contains(PeriodicTaskFlag.VERBOSE)) {
                    log.info("Periodic task {} generated {} records in {} microseconds.",
                            task.name(), result.records().size(), durationUs);
                } else if (log.isDebugEnabled()) {
                    log.debug("Periodic task {} generated {} records in {} microseconds.",
                            task.name(), result.records().size(), durationUs);
                }
            }
            reschedule(task, result.response());
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
        this.log = logContext.logger(OffsetControlManager.class);
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
        reschedule(task, false);
    }

    void unregisterTask(String taskName) {
        PeriodicTask task = tasks.remove(taskName);
        if (task == null) {
            log.debug("Periodic task {} is already unregistered.", taskName);
            return;
        }
        log.info("Unregistering periodic task {}", taskName);
        reschedule(task, false);
    }

    private long nextDelayTimeNs(PeriodicTask task, boolean immediate) {
        if (!immediate) {
            return task.periodNs();
        }
        // The current implementation of KafkaEventQueue always picks from the deferred collection of operations
        // before picking from the non-deferred collection of operations. This can result in some unfairness if
        // deferred operation are scheduled for immediate execution. This delays them by a small amount of time.
        return NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
    }

    private void reschedule(PeriodicTask task, boolean immediate) {
        if (!active) {
            log.trace("cancelling {} because we are inactive.", task.name());
            queueAccessor.cancelDeferred(task.name());
        } else if (tasks.containsKey(task.name())) {
            long nextDelayTimeNs = nextDelayTimeNs(task, immediate);
            long nextRunTimeNs = time.nanoseconds() + nextDelayTimeNs;
            log.trace("rescheduling {} in {} ns (immediate = {})",
                    task.name(), nextDelayTimeNs, immediate);
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
            reschedule(task, false);
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
            reschedule(task, false);
        }
        String[] taskNames = tasks.keySet().toArray(new String[0]);
        Arrays.sort(taskNames);
        log.info("Deactivated periodic tasks: {}", String.join(", ", taskNames));
    }
}
