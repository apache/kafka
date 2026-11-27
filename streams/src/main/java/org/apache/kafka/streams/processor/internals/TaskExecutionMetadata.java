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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;

import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;

/**
 * Multi-threaded class that tracks the status of active tasks being processed. A single instance of this class is
 * shared between all StreamThreads.
 */
public class TaskExecutionMetadata {
    private static final long NOT_READY_LOG_INTERVAL_MS = 120_000L; // Log interval of not being ready (2 minutes)

    private final Set<String> pausedTopologies;
    private final ProcessingMode processingMode;
    // map of task to last not ready logging time (not present means ready, >= 0 means not ready and tracks last log time)
    private final ConcurrentHashMap<TaskId, Long> taskToLastNotReadyLogTime = new ConcurrentHashMap<>();
    private final Logger log;

    public TaskExecutionMetadata(final Set<String> pausedTopologies,
                                 final ProcessingMode processingMode) {
        this.pausedTopologies = pausedTopologies;
        this.processingMode = processingMode;
        this.log = new LogContext("").logger(TaskExecutionMetadata.class);
    }

    public ProcessingMode processingMode() {
        return processingMode;
    }

    public boolean canProcessTask(final Task task, final long now) {
        final boolean taskWasReady = !taskToLastNotReadyLogTime.containsKey(task.id());
        // TODO implement error handling/backoff for non-named topologies (needs KIP)
        final boolean canProcess = !pausedTopologies.contains(UNNAMED_TOPOLOGY);
        // KAFKA-21372: called for every task on every loop iteration, so build the message only when it's logged
        final Supplier<String> logMessage;
        if (canProcess) {
            logMessage = () -> String.format("Task %s can be processed: topology is not paused", task.id());
        } else {
            logMessage = () -> String.format("Task %s can't be processed: topology is paused", task.id());
        }

        if (!canProcess) {
            if (taskWasReady) {
                // READY -> NOT_READY - start timer
                taskToLastNotReadyLogTime.put(task.id(), now);
            } else {
                // NOT_READY - check if it should log
                maybeLogNotReady(task.id(), now, logMessage);
            }
        } else {
            // Task is ready - clear the timer
            taskToLastNotReadyLogTime.remove(task.id());
            if (log.isTraceEnabled()) {
                log.trace(logMessage.get());
            }
        }

        return canProcess;
    }

    private void maybeLogNotReady(final TaskId taskId, final long now, final Supplier<String> logMessage) {
        final Long lastLogTime = taskToLastNotReadyLogTime.get(taskId);
        if (lastLogTime == null) {
            return;
        }

        final long timeSinceLastLog = now - lastLogTime;
        if (timeSinceLastLog >= NOT_READY_LOG_INTERVAL_MS) {
            log.info("Task {} is not ready to process: {}", taskId, logMessage.get());
            taskToLastNotReadyLogTime.put(taskId, now);
        }
    }

    public boolean canPunctuateTask(final Task task) {
        return !pausedTopologies.contains(UNNAMED_TOPOLOGY);
    }

    void removeTaskFromNotReadyTracking(final Task task) {
        final TaskId taskId = task.id();
        taskToLastNotReadyLogTime.remove(taskId);
    }
}
