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
package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.state.HostInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSortedMap;
import static java.util.Collections.unmodifiableSortedSet;
import static java.util.Comparator.comparingLong;

public class DefaultKafkaStreamsState implements KafkaStreamsState {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultKafkaStreamsState.class);

    private final ProcessId processId;
    private final int numProcessingThreads;
    private final Map<String, String> clientTags;
    private final SortedSet<TaskId> previousActiveTasks;
    private final SortedSet<TaskId> previousStandbyTasks;
    private final SortedMap<String, Set<TaskId>> taskIdsByConsumer;
    private final Optional<HostInfo> hostInfo;
    private final Optional<Map<TaskId, Long>> taskLagTotals; // contains lag for all stateful tasks in the app topology
    private final Optional<String> rackId;

    public DefaultKafkaStreamsState(final ProcessId processId,
                                    final int numProcessingThreads,
                                    final Map<String, String> clientTags,
                                    final SortedSet<TaskId> previousActiveTasks,
                                    final SortedSet<TaskId> previousStandbyTasks,
                                    final SortedMap<String, Set<TaskId>> taskIdsByConsumer,
                                    final Optional<HostInfo> hostInfo,
                                    final Optional<Map<TaskId, Long>> taskLagTotals,
                                    final Optional<String> rackId) {
        this.processId = processId;
        this.numProcessingThreads = numProcessingThreads;
        this.clientTags = unmodifiableMap(clientTags);
        this.previousActiveTasks = unmodifiableSortedSet(previousActiveTasks);
        this.previousStandbyTasks = unmodifiableSortedSet(previousStandbyTasks);
        this.taskIdsByConsumer = unmodifiableSortedMap(taskIdsByConsumer);
        this.hostInfo = hostInfo;
        this.taskLagTotals = taskLagTotals;
        this.rackId = rackId;
    }

    @Override
    public ProcessId processId() {
        return processId;
    }

    @Override
    public int numProcessingThreads() {
        return numProcessingThreads;
    }

    @Override
    public SortedSet<String> consumerClientIds() {
        return new TreeSet<>(taskIdsByConsumer.keySet());
    }

    @Override
    public SortedSet<TaskId> previousActiveTasks() {
        return previousActiveTasks;
    }

    @Override
    public SortedSet<TaskId> previousStandbyTasks() {
        return previousStandbyTasks;
    }

    @Override
    public long lagFor(final TaskId task) {
        if (taskLagTotals.isEmpty()) {
            LOG.error("lagFor was called on a KafkaStreamsState {} that does not support lag computations.", processId);
            throw new UnsupportedOperationException("Lag computation was not requested for KafkaStreamsState with process " + processId);
        }

        final Long totalLag = taskLagTotals.get().get(task);
        if (totalLag == null) {
            LOG.error("Task lag lookup failed: {} not in {}", task,
                Arrays.toString(taskLagTotals.get().keySet().toArray()));
            throw new IllegalStateException("Tried to lookup lag for unknown task " + task);
        }
        return totalLag;
    }

    @Override
    public SortedSet<TaskId> prevTasksByLag(final String consumerClientId) {
        if (taskLagTotals.isEmpty()) {
            LOG.error("prevTasksByLag was called on a KafkaStreamsState {} that does not support lag computations.", processId);
            throw new UnsupportedOperationException("Lag computation was not requested for KafkaStreamsState with process " + processId);
        }

        final SortedSet<TaskId> prevTasksByLag =
            new TreeSet<>(comparingLong(this::lagFor).thenComparing(TaskId::compareTo));
        final Set<TaskId> prevOwnedStatefulTasks = taskIdsByConsumer.containsKey(consumerClientId)
            ? taskIdsByConsumer.get(consumerClientId) : new HashSet<>();
        for (final TaskId task : prevOwnedStatefulTasks) {
            if (taskLagTotals.get().containsKey(task)) {
                prevTasksByLag.add(task);
            } else {
                LOG.debug(
                    "Skipping previous task {} since it's not part of the current assignment",
                    task
                );
            }
        }
        return prevTasksByLag;
    }

    @Override
    public Map<TaskId, Long> statefulTasksToLagSums() {
        if (taskLagTotals.isEmpty()) {
            LOG.error("statefulTasksToLagSums was called on a KafkaStreamsState {} that does not support lag computations.", processId);
            throw new UnsupportedOperationException("Lag computation was not requested for KafkaStreamsState with process " + processId);
        }

        return taskLagTotals.get().keySet()
            .stream()
            .collect(Collectors.toMap(taskId -> taskId, this::lagFor));
    }

    @Override
    public Optional<HostInfo> hostInfo() {
        return hostInfo;
    }

    @Override
    public Map<String, String> clientTags() {
        return clientTags;
    }

    @Override
    public Optional<String> rackId() {
        return rackId;
    }
}
