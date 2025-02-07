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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackNeededEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Defines a self-contained object to exchange assignment-related metadata with the Kafka Streams instance.
 * <p>
 * It's used to exchange information between the streams module and the clients module, and should be mostly self-contained
 */
public class StreamsAssignmentInterface {

    private UUID processId;

    private Optional<HostInfo> endpoint;

    private Map<String, Subtopology> subtopologyMap;

    private Map<TaskId, Long> taskLags;

    private AtomicBoolean shutdownRequested;

    private Map<String, String> clientTags;

    public UUID processId() {
        return processId;
    }

    public int topologyEpoch() {
        // TODO: Introduce topology updating
        return 0;
    }

    public Optional<HostInfo> endpoint() {
        return endpoint;
    }

    public Map<String, Subtopology> subtopologyMap() {
        return subtopologyMap;
    }

    // TODO: This needs to be used somewhere
    public Map<TaskId, Long> taskLags() {
        return taskLags;
    }

    public Map<String, String> clientTags() {
        return clientTags;
    }

    public void requestShutdown() {
        shutdownRequested.set(true);
    }

    // TODO: This needs to be checked somewhere.
    public boolean shutdownRequested() {
        return shutdownRequested.get();
    }

    // TODO: This needs to be called somewhere
    public void setTaskLags(Map<TaskId, Long> taskLags) {
        this.taskLags = taskLags;
    }

    public final AtomicReference<Assignment> reconciledAssignment = new AtomicReference<>(
        new Assignment(
            new HashSet<>(),
            new HashSet<>(),
            new HashSet<>()
        )
    );

    public final AtomicReference<Assignment> targetAssignment = new AtomicReference<>();

    /**
     * List of partitions available on each host. Updated by the streams protocol client.
     */
    public final AtomicReference<Map<HostInfo, EndpointPartitions>> partitionsByHost = new AtomicReference<>(Collections.emptyMap());

    public static class HostInfo {

        public final String host;

        public final int port;

        public HostInfo(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "HostInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
        }
    }

    public static class EndpointPartitions {
        private final List<TopicPartition> activePartitions;
        private final List<TopicPartition> standbyPartitions;

        public EndpointPartitions(final List<TopicPartition> activePartitions,
                                  final List<TopicPartition> standbyPartitions) {
            this.activePartitions = activePartitions;
            this.standbyPartitions = standbyPartitions;
        }

        public List<TopicPartition> activePartitions() {
            return new ArrayList<>(activePartitions);
        }

        public List<TopicPartition> standbyPartitions() {
            return new ArrayList<>(standbyPartitions);
        }
        @Override
        public String toString() {
            return "EndpointPartitions {"
                    + "activePartitions=" + activePartitions
                    + ", standbyPartitions=" + standbyPartitions
                    + '}';
        }
    }
    
    public static class Assignment {

        public static final Assignment EMPTY = new Assignment();

        public final Set<TaskId> activeTasks = new HashSet<>();

        public final Set<TaskId> standbyTasks = new HashSet<>();

        public final Set<TaskId> warmupTasks = new HashSet<>();

        public Assignment() {
        }

        public Assignment(final Set<TaskId> activeTasks, final Set<TaskId> standbyTasks, final Set<TaskId> warmupTasks) {
            this.activeTasks.addAll(activeTasks);
            this.standbyTasks.addAll(standbyTasks);
            this.warmupTasks.addAll(warmupTasks);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Assignment that = (Assignment) o;
            return Objects.equals(activeTasks, that.activeTasks)
                && Objects.equals(standbyTasks, that.standbyTasks)
                && Objects.equals(warmupTasks, that.warmupTasks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(activeTasks, standbyTasks, warmupTasks);
        }

        public Assignment copy() {
            return new Assignment(activeTasks, standbyTasks, warmupTasks);
        }

        @Override
        public String toString() {
            return "Assignment{" +
                "activeTasks=" + activeTasks +
                ", standbyTasks=" + standbyTasks +
                ", warmupTasks=" + warmupTasks +
                '}';
        }
    }

    public static class TopicInfo {

        public final Optional<Integer> numPartitions;
        public final Optional<Short> replicationFactor;
        public final Map<String, String> topicConfigs;

        public TopicInfo(final Optional<Integer> numPartitions,
                         final Optional<Short> replicationFactor,
                         final Map<String, String> topicConfigs) {
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.topicConfigs = topicConfigs;
        }

        @Override
        public String toString() {
            return "TopicInfo{" +
                "numPartitions=" + numPartitions +
                ", replicationFactor=" + replicationFactor +
                ", topicConfigs=" + topicConfigs +
                '}';
        }
    }

    public static class TaskId implements Comparable<TaskId> {

        private final String subtopologyId;
        private final int partitionId;

        public int partitionId() {
            return partitionId;
        }

        public String subtopologyId() {
            return subtopologyId;
        }

        public TaskId(final String subtopologyId, final int partitionId) {
            this.subtopologyId = subtopologyId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskId taskId = (TaskId) o;
            return partitionId == taskId.partitionId && Objects.equals(subtopologyId, taskId.subtopologyId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtopologyId, partitionId);
        }

        @Override
        public int compareTo(TaskId taskId) {
            if (subtopologyId.equals(taskId.subtopologyId)) {
                return partitionId - taskId.partitionId;
            }
            return subtopologyId.compareTo(taskId.subtopologyId);
        }

        @Override
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", partitionId=" + partitionId +
                '}';
        }
    }

    public static class Subtopology {

        public final Set<String> sourceTopics;
        public final Set<String> repartitionSinkTopics;
        public final Map<String, TopicInfo> stateChangelogTopics;
        public final Map<String, TopicInfo> repartitionSourceTopics;
        public final Collection<Set<String>> copartitionGroups;

        public Subtopology(final Set<String> sourceTopics,
                           final Set<String> repartitionSinkTopics,
                           final Map<String, TopicInfo> repartitionSourceTopics,
                           final Map<String, TopicInfo> stateChangelogTopics,
                           final Collection<Set<String>> copartitionGroups
        ) {
            this.sourceTopics = sourceTopics;
            this.repartitionSinkTopics = repartitionSinkTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
            this.copartitionGroups = copartitionGroups;
        }

        @Override
        public String toString() {
            return "Subtopology{" +
                "sourceTopics=" + sourceTopics +
                ", repartitionSinkTopics=" + repartitionSinkTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                ", copartitionGroups=" + copartitionGroups +
                '}';
        }
    }

    private final BlockingQueue<BackgroundEvent> onCallbackRequests = new LinkedBlockingQueue<>();

    private ApplicationEventHandler applicationEventHandler = null;

    private Optional<Function<Set<StreamsAssignmentInterface.TaskId>, Optional<Exception>>> onTasksRevokedCallback = Optional.empty();
    private Optional<Function<Assignment, Optional<Exception>>> onTasksAssignedCallback = Optional.empty();
    private Optional<Supplier<Optional<Exception>>> onAllTasksLostCallback = Optional.empty();

    private final StreamsRebalanceEventProcessor streamsRebalanceEventProcessor;

    private class StreamsRebalanceEventProcessor implements EventProcessor<BackgroundEvent> {

        @Override
        public void process(final BackgroundEvent event) {
            switch (event.type()) {
                case ERROR:
                    throw ((ErrorEvent) event).error();

                case STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED:
                    processStreamsOnTasksRevokedCallbackNeededEvent((StreamsOnTasksRevokedCallbackNeededEvent) event);
                    break;

                case STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED:
                    processStreamsOnTasksAssignedCallbackNeededEvent((StreamsOnTasksAssignedCallbackNeededEvent) event);
                    break;

                case STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED:
                    processStreamsOnAllTasksLostCallbackNeededEvent((StreamsOnAllTasksLostCallbackNeededEvent) event);
                    break;

                default:
                    throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

            }
        }

        private void processStreamsOnTasksRevokedCallbackNeededEvent(final StreamsOnTasksRevokedCallbackNeededEvent event) {
            StreamsOnTasksRevokedCallbackCompletedEvent invokedEvent = invokeOnTasksRevokedCallback(event.activeTasksToRevoke(), event.future());
            applicationEventHandler.add(invokedEvent);
            if (invokedEvent.error().isPresent()) {
                throw invokedEvent.error().get();
            }
        }

        private void processStreamsOnTasksAssignedCallbackNeededEvent(final StreamsOnTasksAssignedCallbackNeededEvent event) {
            StreamsOnTasksAssignedCallbackCompletedEvent invokedEvent = invokeOnTasksAssignedCallback(event.assignment(), event.future());
            applicationEventHandler.add(invokedEvent);
            if (invokedEvent.error().isPresent()) {
                throw invokedEvent.error().get();
            }
        }

        private void processStreamsOnAllTasksLostCallbackNeededEvent(final StreamsOnAllTasksLostCallbackNeededEvent event) {
            StreamsOnAllTasksLostCallbackCompletedEvent invokedEvent = invokeOnAllTasksLostCallback(event.future());
            applicationEventHandler.add(invokedEvent);
            if (invokedEvent.error().isPresent()) {
                throw invokedEvent.error().get();
            }
        }

        private StreamsOnTasksRevokedCallbackCompletedEvent invokeOnTasksRevokedCallback(final Set<StreamsAssignmentInterface.TaskId> activeTasksToRevoke,
                                                                                          final CompletableFuture<Void> future) {
            final Optional<Exception> exceptionFromCallback = onTasksRevokedCallback
                .orElseThrow(() -> new IllegalStateException("No tasks assignment callback set!")).apply(activeTasksToRevoke);

            return exceptionFromCallback
                .map(exception ->
                    new StreamsOnTasksRevokedCallbackCompletedEvent(
                        future,
                        Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exception, "Task revocation callback throws an error"))
                    ))
                .orElseGet(() -> new StreamsOnTasksRevokedCallbackCompletedEvent(future, Optional.empty()));
        }

        private StreamsOnTasksAssignedCallbackCompletedEvent invokeOnTasksAssignedCallback(final StreamsAssignmentInterface.Assignment assignment,
                                                                                           final CompletableFuture<Void> future) {
            Optional<KafkaException> error = Optional.empty();
            // ToDo: Can we avoid the following check?
            if (!assignment.equals(reconciledAssignment.get())) {

                final Optional<Exception> exceptionFromCallback = onTasksAssignedCallback
                    .orElseThrow(() -> new IllegalStateException("No tasks assignment callback set!")).apply(assignment);

                if (exceptionFromCallback.isPresent()) {
                    error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task assignment callback throws an error"));
                } else {
                    reconciledAssignment.set(assignment);
                }
            }
            return new StreamsOnTasksAssignedCallbackCompletedEvent(future, error);
        }

        private StreamsOnAllTasksLostCallbackCompletedEvent invokeOnAllTasksLostCallback(final CompletableFuture<Void> future) {
            final Optional<Exception> exceptionFromCallback = onAllTasksLostCallback
                .orElseThrow(() -> new IllegalStateException("No tasks assignment callback set!")).get();

            final Optional<KafkaException> error;

            if (exceptionFromCallback.isPresent()) {
                error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task assignment callback throws an error"));
            } else {
                error = Optional.empty();
                reconciledAssignment.set(Assignment.EMPTY);
            }

            return new StreamsOnAllTasksLostCallbackCompletedEvent(future, error);
        }
    }

    public StreamsAssignmentInterface(UUID processId,
                                      Optional<HostInfo> endpoint,
                                      Map<String, Subtopology> subtopologyMap,
                                      Map<String, String> clientTags) {
        this.processId = processId;
        this.endpoint = endpoint;
        this.subtopologyMap = subtopologyMap;
        this.taskLags = new HashMap<>();
        this.shutdownRequested = new AtomicBoolean(false);
        this.clientTags = clientTags;
        this.streamsRebalanceEventProcessor = new StreamsRebalanceEventProcessor();
    }

    public void setOnTasksRevokedCallback(final Function<Set<StreamsAssignmentInterface.TaskId>, Optional<Exception>> onTasksRevokedCallback) {
        this.onTasksRevokedCallback = Optional.ofNullable(onTasksRevokedCallback);
    }

    public void setOnTasksAssignedCallback(final Function<Assignment, Optional<Exception>> onTasksAssignedCallback) {
        this.onTasksAssignedCallback = Optional.ofNullable(onTasksAssignedCallback);
    }

    public void setOnAllTasksLostCallback(final Supplier<Optional<Exception>> onAllTasksLostCallback) {
        this.onAllTasksLostCallback = Optional.ofNullable(onAllTasksLostCallback);
    }

    public void setApplicationEventHandler(final ApplicationEventHandler applicationEventHandler) {
        this.applicationEventHandler = applicationEventHandler;
    }

    public CompletableFuture<Void> requestOnTasksAssignedCallbackInvocation(final Assignment assignment) {
        final StreamsOnTasksAssignedCallbackNeededEvent onTasksAssignedCallbackNeededEvent = new StreamsOnTasksAssignedCallbackNeededEvent(assignment);
        onCallbackRequests.add(onTasksAssignedCallbackNeededEvent);
        return onTasksAssignedCallbackNeededEvent.future();
    }

    public CompletableFuture<Void> requestOnTasksRevokedCallbackInvocation(final Set<StreamsAssignmentInterface.TaskId> activeTasksToRevoke) {
        final StreamsOnTasksRevokedCallbackNeededEvent onTasksRevokedCallbackNeededEvent = new StreamsOnTasksRevokedCallbackNeededEvent(activeTasksToRevoke);
        onCallbackRequests.add(onTasksRevokedCallbackNeededEvent);
        return onTasksRevokedCallbackNeededEvent.future();
    }

    public CompletableFuture<Void> requestOnAllTasksLostCallbackInvocation() {
        final StreamsOnAllTasksLostCallbackNeededEvent onAllTasksLostCallbackNeededEvent = new StreamsOnAllTasksLostCallbackNeededEvent();
        onCallbackRequests.add(onAllTasksLostCallbackNeededEvent);
        return onAllTasksLostCallbackNeededEvent.future();
    }

    public void processStreamsRebalanceEvents() {
        LinkedList<BackgroundEvent> events = new LinkedList<>();
        onCallbackRequests.drainTo(events);
        for (BackgroundEvent event : events) {
            streamsRebalanceEventProcessor.process(event);
        }
    }

    @Override
    public String toString() {
        return "StreamsAssignmentMetadata{" +
            "processID=" + processId +
            ", endpoint='" + endpoint + '\'' +
            ", subtopologyMap=" + subtopologyMap +
            ", taskLags=" + taskLags +
            ", clientTags=" + clientTags +
            '}';
    }

}
