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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

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
    private final Logger log;
    private final Timer timer;
    private final CoordinatorShardScheduler<U> scheduler;
    private final Map<String, TimerTask> tasks = new ConcurrentHashMap<>();

    public CoordinatorTimerImpl(
        LogContext logContext,
        Timer timer,
        CoordinatorShardScheduler<U> scheduler
    ) {
        this.log = logContext.logger(CoordinatorTimerImpl.class);
        this.timer = timer;
        this.scheduler = scheduler;
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
        // The TimerTask wraps the TimeoutOperation into a write operation. When the TimerTask
        // expires, the operation is scheduled through the scheduler to be executed. This
        // ensures that the threading model of the runtime is respected.
        var task = new TimerTask(unit.toMillis(delay)) {
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

                        // Execute the timeout operation.
                        return operation.generateRecords();
                    }
                ).exceptionally(ex -> {
                    // Remove the task after a failure.
                    tasks.remove(key, this);

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

                    if (retry) {
                        log.info("The write event {} for the timer {} failed due to {}. Rescheduling it. ",
                            operationName, key, ex.getMessage());
                        schedule(key, retryBackoff, TimeUnit.MILLISECONDS, true, retryBackoff, operation);
                    } else {
                        log.error("The write event {} for the timer {} failed due to {}. Ignoring it. ",
                            operationName, key, ex.getMessage(), ex);
                    }

                    return null;
                });

                log.debug("Scheduling write event {} for timer {}.", operationName, key);
            }
        };

        log.debug("Registering timer {} with delay of {}ms.", key, unit.toMillis(delay));
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
        var prevTask = tasks.remove(key);
        if (prevTask != null) prevTask.cancel();
    }

    @Override
    public boolean isScheduled(String key) {
        return tasks.containsKey(key);
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
