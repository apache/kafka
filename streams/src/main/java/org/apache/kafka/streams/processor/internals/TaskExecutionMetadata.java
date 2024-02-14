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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;

/**
 * Multi-threaded class that tracks the status of active tasks being processed. A single instance of this class is
 * shared between all StreamThreads.
 */
public class TaskExecutionMetadata {
    // TODO: implement exponential backoff, for now we just wait 5s
    private static final long CONSTANT_BACKOFF_MS = 5_000L;

    private final boolean hasNamedTopologies;
    private final Set<String> pausedTopologies;
    private final ProcessingMode processingMode;
    private final Collection<Task> successfullyProcessed = new HashSet<>();
    // map of topologies experiencing errors/currently under backoff
    private final ConcurrentHashMap<String, NamedTopologyMetadata> topologyNameToErrorMetadata = new ConcurrentHashMap<>();

    public TaskExecutionMetadata(final Set<String> allTopologyNames,
                                 final Set<String> pausedTopologies,
                                 final ProcessingMode processingMode) {
        this.hasNamedTopologies = !(allTopologyNames.size() == 1 && allTopologyNames.contains(UNNAMED_TOPOLOGY));
        this.pausedTopologies = pausedTopologies;
        this.processingMode = processingMode;
    }

    public boolean hasNamedTopologies() {
        return hasNamedTopologies;
    }

    public ProcessingMode processingMode() {
        return processingMode;
    }

    public boolean canProcessTask(final Task task, final long now) {
        final String topologyName = task.id().topologyName();
        if (!hasNamedTopologies) {
            // TODO implement error handling/backoff for non-named topologies (needs KIP)
            return !pausedTopologies.contains(UNNAMED_TOPOLOGY);
        } else {
            if (pausedTopologies.contains(topologyName)) {
                return false;
            } else {
                final NamedTopologyMetadata metadata = topologyNameToErrorMetadata.get(topologyName);
                return metadata == null || (metadata.canProcess() && metadata.canProcessTask(task, now));
            }
        }
    }

    public boolean canPunctuateTask(final Task task) {
        final String topologyName = task.id().topologyName();

        if (topologyName == null) {
            return !pausedTopologies.contains(UNNAMED_TOPOLOGY);
        } else {
            return !pausedTopologies.contains(topologyName);
        }
    }

    public void registerTaskError(final Task task, final Throwable t, final long now) {
        if (hasNamedTopologies) {
            final String topologyName = task.id().topologyName();
            topologyNameToErrorMetadata.computeIfAbsent(topologyName, n -> new NamedTopologyMetadata(topologyName))
                .registerTaskError(task, t, now);
        }
    }

    Collection<Task> successfullyProcessed() {
        return successfullyProcessed;
    }

    public void addToSuccessfullyProcessed(final Task task) {
        successfullyProcessed.add(task);
    }

    void removeTaskFromSuccessfullyProcessedBeforeClosing(final Task task) {
        successfullyProcessed.remove(task);
    }

    void clearSuccessfullyProcessed() {
        successfullyProcessed.clear();
    }

    private class NamedTopologyMetadata {
        private final Logger log;
        private final Map<TaskId, Long> tasksToErrorTime = new ConcurrentHashMap<>();

        public NamedTopologyMetadata(final String topologyName) {
            final LogContext logContext = new LogContext(String.format("topology-name [%s] ", topologyName));
            this.log = logContext.logger(NamedTopologyMetadata.class);
        }

        public boolean canProcess() {
            // TODO: during long task backoffs, pause the full topology to avoid it getting out of sync
            return true;
        }

        public boolean canProcessTask(final Task task, final long now) {
            final Long errorTime = tasksToErrorTime.get(task.id());
            if (errorTime == null) {
                return true;
            } else if (now - errorTime > CONSTANT_BACKOFF_MS) {
                log.info("End backoff for task {} at t={}", task.id(), now);
                tasksToErrorTime.remove(task.id());
                if (tasksToErrorTime.isEmpty()) {
                    topologyNameToErrorMetadata.remove(task.id().topologyName());
                }
                return true;
            } else {
                log.debug("Skipping processing for unhealthy task {} at t={}", task.id(), now);
                return false;
            }
        }

        public synchronized void registerTaskError(final Task task, final Throwable t, final long now) {
            log.info("Begin backoff for unhealthy task {} at t={} due to {}", task.id(), now, t.getClass().getName());
            tasksToErrorTime.put(task.id(), now);
        }
    }
}
