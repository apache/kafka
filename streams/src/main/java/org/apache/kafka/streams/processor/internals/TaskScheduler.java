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

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;

/**
 * Multi-threaded class that tracks the status of active tasks being processed and decides if/when they can
 * be executed.
 *
 * Note: A single instance of this class is shared between all StreamThreads, so it must be thread-safe
 */
public class TaskScheduler {
    private static final long INITIAL_BACKOFF_MS = 3 * 1000L;  // wait 3s after the first task failure
    private static final int RETRY_BACKOFF_EXP_BASE = 2;
    private static final long MAXIMUM_BACKOFF_MS = 60 * 1000L; // back off up to a maximum of 1 minute between retries
    private static final double RETRY_BACKOFF_JITTER = 0.2;

    enum TaskStatus {
        RUNNING,  // the task and its topology are healthy and able to be processed
        BACKOFF,  // the task and/or its topology are unhealthy, task has remaining backoff time to wait before a retry
        RETRIABLE // the task and/or its topology are still considered unhealthy but are ready to be retried
    }

    private final ExponentialBackoff taskBackoff = new ExponentialBackoff(
        INITIAL_BACKOFF_MS,
        RETRY_BACKOFF_EXP_BASE,
        MAXIMUM_BACKOFF_MS,
        RETRY_BACKOFF_JITTER
    );

    // map of topologies experiencing errors/currently under backoff
    private final ConcurrentHashMap<String, NamedTopologyMetadata> topologyNameToErrorMetadata = new ConcurrentHashMap<>();
    private final boolean hasNamedTopologies;

    public TaskScheduler(final Set<String> allTopologyNames) {
        this.hasNamedTopologies = !(allTopologyNames.size() == 1 && allTopologyNames.contains(UNNAMED_TOPOLOGY));
    }

    public TaskStatus getTaskStatus(final Task task, final long now) {
        final String topologyName = task.id().topologyName();
        if (!hasNamedTopologies) {
            // TODO implement error handling/backoff for non-named topologies (needs KIP)
            return TaskStatus.RUNNING;
        } else {
            final NamedTopologyMetadata metadata = topologyNameToErrorMetadata.get(topologyName);
            if (metadata == null) {
                return TaskStatus.RUNNING;
            } else {
                return metadata.getTaskStatus(task, now);
            }
        }
    }

    public void registerTaskError(final Task task, final long now) {
        if (hasNamedTopologies) {
            final String topologyName = task.id().topologyName();
            // Only need to register this error if the task is currently healthy
            if (!topologyNameToErrorMetadata.containsKey(topologyName)) {
                final NamedTopologyMetadata namedTopologyMetadata = new NamedTopologyMetadata(topologyName);
                namedTopologyMetadata.registerTaskError(task, now);
                topologyNameToErrorMetadata.put(topologyName, namedTopologyMetadata);
            }
        }
    }

    public void registerTaskSuccess(final Task task) {
        if (hasNamedTopologies && task.id().topologyName() != null) {
            final NamedTopologyMetadata topologyMetadata = topologyNameToErrorMetadata.get(task.id().topologyName());
            if (topologyMetadata != null) {
                topologyMetadata.registerRetrySuccess(task);
            }
        }
    }

    private class NamedTopologyMetadata {
        private final Logger log;
        private final Map<TaskId, ErrorMetadata> tasksToErrorTime = new ConcurrentHashMap<>();

        public NamedTopologyMetadata(final String topologyName) {
            final LogContext logContext = new LogContext(String.format("topology-name [%s] ", topologyName));
            this.log = logContext.logger(NamedTopologyMetadata.class);
        }

        public TaskStatus getTaskStatus(final Task task, final long now) {
            final ErrorMetadata errorMetadata = tasksToErrorTime.get(task.id());
            if (errorMetadata == null) {
                return TaskStatus.RUNNING;
            } else {
                ++errorMetadata.numAttempts;

                final long remainingBackoffMs = now - errorMetadata.firstErrorTimeMs + taskBackoff.backoff(errorMetadata.numAttempts);

                if (remainingBackoffMs > 0) {
                    log.info("Task {} is ready to re-attempt processing, retry attempt count is {}",
                             task.id(), errorMetadata.numAttempts);
                    return TaskStatus.RETRIABLE;
                } else {
                    log.debug("Skip processing for task {} with remaining backoff time = {}ms",
                              task.id(), remainingBackoffMs);
                    return TaskStatus.BACKOFF;
                }
            }
        }

        public synchronized void registerTaskError(final Task task, final long now) {
            log.info("Begin backoff for unhealthy task {} at t={}", task.id(), now);
            tasksToErrorTime.put(task.id(), new ErrorMetadata(now));
        }

        public synchronized void registerRetrySuccess(final Task task) {
            log.info("End backoff for task {}", task.id());
            tasksToErrorTime.remove(task.id());
            if (tasksToErrorTime.isEmpty()) {
                topologyNameToErrorMetadata.remove(task.id().topologyName());
            }
        }

        private class ErrorMetadata {
            public final long firstErrorTimeMs;
            public int numAttempts = 0;

            public ErrorMetadata(final long firstErrorTimeMs) {
                this.firstErrorTimeMs = firstErrorTimeMs;
            }
        }

    }

}
