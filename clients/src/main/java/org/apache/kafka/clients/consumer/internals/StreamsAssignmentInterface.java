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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Defines a self-contained object to exchange assignment-related metadata with the Kafka Streams instance.
 *
 * It's used to exchange information between the streams module and the clients module, and should be mostly self-contained
 */
public class StreamsAssignmentInterface {

    private UUID processID;

    private String userEndPointHost;

    private int userEndPointPort;

    private Map<String, SubTopology> subtopologyMap;

    private Map<String, Object> assignmentConfiguration;

    private Map<TaskId, Long> taskLags;

    private AtomicBoolean shutdownRequested;

    private Map<String, String> clientTags;

    public UUID processID() {
        return processID;
    }

    public String userEndPointHost() {
        return userEndPointHost;
    }

    public int userEndPointPort() {
        return userEndPointPort;
    }

    public Map<String, SubTopology> subtopologyMap() {
        return subtopologyMap;
    }

    public Map<String, Object> assignmentConfiguration() {
        return assignmentConfiguration;
    }

    public Map<TaskId, Long> taskLags() {
        return taskLags;
    }

    public byte[] computeTopologyHash() {
        // TODO
        return new byte[0];
    }

    public Map<String, String> clientTags() {
        return clientTags;
    }

    public void requestShutdown() {
        shutdownRequested.set(true);
    }

    public boolean shutdownRequested() {
        return shutdownRequested.get();
    }

    public void setTaskLags(Map<TaskId, Long> taskLags) {
        this.taskLags = taskLags;
    }

    // TODO: Reconciled assignment updated by the stream thread
    public final Assignment reconciledAssignment = new Assignment();

    // TODO: Target assignment read by the stream thread
    public final Assignment targetAssignment = new Assignment();

    public static class Assignment {

        public final Set<TaskId> activeTasks = new HashSet<>();

        public final Set<TaskId> standbyTasks = new HashSet<>();

        public final Set<TaskId> warmupTasks = new HashSet<>();

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
        public final int taskId;

        public int taskId() {
            return taskId;
        }

        public String subtopologyId() {
            return subtopologyId;
        }

        public TaskId(final String subtopologyId, final int taskId) {
            this.subtopologyId = subtopologyId;
            this.taskId = taskId;
        }

        @Override
        public String toString() {
            return "TaskId{" +
                "subtopologyId=" + subtopologyId +
                ", taskId=" + taskId +
                '}';
        }
    }

    public static class SubTopology {

        public final Set<String> sinkTopics;
        public final Set<String> sourceTopics;
        public final Map<String, TopicInfo> stateChangelogTopics;
        public final Map<String, TopicInfo> repartitionSourceTopics;

       public SubTopology(final Set<String> sinkTopics,
                          final Set<String> sourceTopics,
                          final Map<String, TopicInfo> repartitionSourceTopics,
                          final Map<String, TopicInfo> stateChangelogTopics) {
            this.sinkTopics = sinkTopics;
            this.sourceTopics = sourceTopics;
            this.stateChangelogTopics = stateChangelogTopics;
            this.repartitionSourceTopics = repartitionSourceTopics;
        }

        @Override
        public String toString() {
            return "SubTopology{" +
                "sinkTopics=" + sinkTopics +
                ", sourceTopics=" + sourceTopics +
                ", stateChangelogTopics=" + stateChangelogTopics +
                ", repartitionSourceTopics=" + repartitionSourceTopics +
                '}';
        }
    }

    public StreamsAssignmentInterface(UUID processID,
                                      String userEndPointHost,
                                      int userEndPointPort,
                                      Map<String, SubTopology> subtopologyMap,
                                      Map<String, Object> assignmentConfiguration,
                                      Map<String, String> clientTags
                                      ) {
        this.processID = processID;
        this.userEndPointHost = userEndPointHost;
        this.userEndPointPort = userEndPointPort;
        this.subtopologyMap = subtopologyMap;
        this.assignmentConfiguration = assignmentConfiguration;
        this.taskLags = new HashMap<>();
        this.shutdownRequested = new AtomicBoolean(false);
        this.clientTags = clientTags;
    }

    @Override
    public String toString() {
        return "StreamsAssignmentMetadata{" +
            "processID=" + processID +
            ", userEndPointHost='" + userEndPointHost + '\'' +
            ", userEndPointPort=" + userEndPointPort +
            ", subtopologyMap=" + subtopologyMap +
            ", assignmentConfiguration=" + assignmentConfiguration +
            ", taskLags=" + taskLags +
            ", clientTags=" + clientTags +
            '}';
    }

}
