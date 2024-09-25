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

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Defines a self-contained object to exchange assignment-related metadata with the Kafka Streams instance.
 * <p>
 * It's used to exchange information between the streams module and the clients module, and should be mostly self-contained
 */
public class StreamsAssignmentInterface {

    private UUID processId;

    private Optional<HostInfo> endpoint;

    private String assignor;

    private Map<String, Subtopology> subtopologyMap;

    private Map<String, Object> assignmentConfiguration;

    private Map<TaskId, Long> taskLags;

    private AtomicBoolean shutdownRequested;

    private Map<String, String> clientTags;

    public UUID processId() {
        return processId;
    }

    public String topologyId() {
        // ToDo: As long as we do not compute the topology ID, let's use a constant one
        return "topology-id";
    }

    public Optional<HostInfo> endpoint() {
        return endpoint;
    }

    public String assignor() {
        return assignor;
    }

    public Map<String, Subtopology> subtopologyMap() {
        return subtopologyMap;
    }

    public Map<String, Object> assignmentConfiguration() {
        return assignmentConfiguration;
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
    public final AtomicReference<Map<HostInfo, List<TopicPartition>>> partitionsByHost = new AtomicReference<>(Collections.emptyMap());

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

    public static class Assignment {

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
        public final Map<String, String> topicConfigs;

        public TopicInfo(final Optional<Integer> numPartitions,
                         final Map<String, String> topicConfigs) {
            this.numPartitions = numPartitions;
            this.topicConfigs = topicConfigs;
        }

        @Override
        public String toString() {
            return "TopicInfo{" +
                "numPartitions=" + numPartitions +
                ", topicConfigs=" + topicConfigs +
                '}';
        }

    }

    public static class TaskId {

        public final String subtopologyId;
        public final int partitionId;

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
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", partitionId=" + partitionId +
                '}';
        }
    }

    public static class Subtopology {

        public final Set<String> sourceTopics;
        public final Set<String> sinkTopics;
        public final Map<String, TopicInfo> stateChangelogTopics;
        public final Map<String, TopicInfo> repartitionSourceTopics;

        public Subtopology(final Set<String> sourceTopics,
                           final Set<String> sinkTopics,
                           final Map<String, TopicInfo> repartitionSourceTopics,
                           final Map<String, TopicInfo> stateChangelogTopics) {
            this.sourceTopics = sourceTopics;
            this.sinkTopics = sinkTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        @Override
        public String toString() {
            return "Subtopology{" +
                "sourceTopics=" + sourceTopics +
                ", sinkTopics=" + sinkTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                '}';
        }
    }

    public StreamsAssignmentInterface(UUID processId,
                                      Optional<HostInfo> endpoint,
                                      String assignor,
                                      Map<String, Subtopology> subtopologyMap,
                                      Map<String, Object> assignmentConfiguration,
                                      Map<String, String> clientTags
    ) {
        this.processId = processId;
        this.endpoint = endpoint;
        this.assignor = assignor;
        this.subtopologyMap = subtopologyMap;
        this.assignmentConfiguration = assignmentConfiguration;
        this.taskLags = new HashMap<>();
        this.shutdownRequested = new AtomicBoolean(false);
        this.clientTags = clientTags;
    }

    @Override
    public String toString() {
        return "StreamsAssignmentMetadata{" +
            "processID=" + processId +
            ", endpoint='" + endpoint + '\'' +
            ", assignor='" + assignor + '\'' +
            ", subtopologyMap=" + subtopologyMap +
            ", assignmentConfiguration=" + assignmentConfiguration +
            ", taskLags=" + taskLags +
            ", clientTags=" + clientTags +
            '}';
    }

}
