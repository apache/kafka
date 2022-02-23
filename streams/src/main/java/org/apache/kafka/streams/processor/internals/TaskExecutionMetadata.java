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
import org.apache.kafka.streams.errors.UnknownTopologyException;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;

import static org.apache.kafka.streams.processor.internals.TopologyMetadata.UNNAMED_TOPOLOGY;

/**
 * Multi-threaded class that tracks the status of active tasks being processed. A single instance of this class is
 * shared between all StreamThreads.
 */
public class TaskExecutionMetadata {
    private Logger log;

    private final boolean hasNamedTopologies;
    private final ConcurrentHashMap<String, NamedTopologyMetadata> topologyNameToMetadata = new ConcurrentHashMap<>();

    public TaskExecutionMetadata(final Set<String> allTopologyNames) {
        this.hasNamedTopologies = !(allTopologyNames.size() == 1 && allTopologyNames.contains(UNNAMED_TOPOLOGY));
        allTopologyNames.forEach(name -> topologyNameToMetadata.put(name, new NamedTopologyMetadata(name)));
    }

    public void setLog(final LogContext logContext) {
        log = logContext.logger(getClass());
    }

    public boolean canProcessTopology(final String topologyName) {
        if (!hasNamedTopologies) {
            return true;
        } else {
            return getMetadata(topologyName).canProcess();
        }
    }

    public boolean canProcessTask(final Task task) {
        final String topologyName = task.id().topologyName();
        if (!hasNamedTopologies) {
            // TODO implement error handling/backoff for non-named topologies (needs KIP)
            return true;
        } else {
            return getMetadata(topologyName).canProcessTask(task);
        }
    }

    public void registerTaskError(final Task task, final Throwable t) {
        if (hasNamedTopologies) {
            getMetadata(task.id().topologyName()).registerTaskError(task, t);
        }
    }

    public void registerTopology(final String topologyName) {
        if (topologyNameToMetadata.containsKey(topologyName)) {
            log.error("Topology {} is already registered in topology map.\n" +
                "topologyNameToMetadata: {}", topologyName, topologyNameToMetadata);
            throw new IllegalStateException("Tried to register new topology with execution metadata but "
            + topologyName + " was already registered");
        }
        topologyNameToMetadata.put(topologyName, new NamedTopologyMetadata(topologyName));
        log.debug("Registered topology {} with execution metadata", topologyName);
    }

    public void unregisterTopology(final String topologyName) {
        if (!topologyNameToMetadata.containsKey(topologyName)) {
            log.error("Topology {} is not already registered in topology map.\n" +
                "topologyNameToMetadata: {}", topologyName, topologyNameToMetadata);
            throw new IllegalStateException("Tried to unregister a topology with execution metadata but "
                + topologyName + " was not currently registered");
        }
        topologyNameToMetadata.remove(topologyName);
        log.debug("Unregistered topology {} with execution metadata", topologyName);
    }

    /**
     * Look up the metadata for this named topology
     *
     * @throws IllegalStateException                                      if topology name is invalid
     * @throws org.apache.kafka.streams.errors.UnknownTopologyException   if the topology name is not found
     */
    private NamedTopologyMetadata getMetadata(final String topologyName) {
        if (topologyName == null || topologyName.equals(UNNAMED_TOPOLOGY)) {
            log.error("Tried to look up metadata for named topology but topologyName was '{}'", topologyName);
            throw new IllegalStateException("Invalid topology name for ");
        }

        final NamedTopologyMetadata topologyMetadata = topologyNameToMetadata.get(topologyName);
        if (topologyMetadata == null) {
            log.error("Tried to look up metadata for named topology but could not find topologyName = '{}'", topologyName);
            throw new UnknownTopologyException("Failed to check execution status", topologyName);
        } else {
            return topologyMetadata;
        }
    }

    static class NamedTopologyMetadata {
        private final Logger log;
        private final Set<TaskId> tasksToBackoff = new ConcurrentSkipListSet<>();

        public NamedTopologyMetadata(final String topologyName) {
            final LogContext logContext = new LogContext(String.format("topology-name [%s] ", topologyName));
            this.log = logContext.logger(NamedTopologyMetadata.class);
        }

        public boolean canProcess() {
            // TODO: during long task backoffs, pause the full topology to avoid it getting out of sync
            return true;
        }

        public boolean canProcessTask(final Task task) {
            // TODO: implement true backoff, for now we just skip one iteration of processing a task upon error
            final boolean canProcess = !tasksToBackoff.remove(task.id());
            if (!canProcess) {
                log.info("Skipping processing iteration for task {}", task.id());
            }
            return canProcess;
        }

        public synchronized void registerTaskError(final Task task, final Throwable t) {
            log.warn("Registered error {} for task {}", t.getMessage(), task.id());
            tasksToBackoff.add(task.id());
        }
    }
}
