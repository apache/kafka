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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Defines an object that holds metadata about a Kafka Streams instance.
 *
 * It's used to exchange information between the streams module and the clients module, and should be mostly self-contained
 */
public class StreamsAssignmentMetadata {

    private UUID processID;

    private String userEndPointHost;

    private int userEndPointPort;

    private Map<Integer, SubTopology> subtopologyMap;

    private Map<String, Object> assignmentConfiguration;

    private Map<String, Long> taskLags;

    // TODO: This probably needs to be extended by `InternalTopicsConfig`. See `TopicsInfo` class in the streams module
    public static class SubTopology {

        public final Set<String> sinkTopics;
        public final Set<String> sourceTopics;
        public final Set<String> stateChangelogTopics;
        public final Set<String> repartitionSourceTopics;

       public SubTopology(final Set<String> sinkTopics,
                          final Set<String> sourceTopics,
                          final Set<String> repartitionSourceTopics,
                          final Set<String> stateChangelogTopics) {
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

    public StreamsAssignmentMetadata(UUID processID,
                                     String userEndPointHost,
                                     int userEndPointPort,
                                     Map<Integer, SubTopology> subtopologyMap,
                                     Map<String, Object> assignmentConfiguration) {
        this.processID = processID;
        this.userEndPointHost = userEndPointHost;
        this.userEndPointPort = userEndPointPort;
        this.subtopologyMap = subtopologyMap;
        this.assignmentConfiguration = assignmentConfiguration;
        this.taskLags = new HashMap<>();
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
            '}';
    }
}
