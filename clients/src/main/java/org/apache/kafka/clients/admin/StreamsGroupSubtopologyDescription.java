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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A detailed description of a subtopology in a streams group.
 */
@InterfaceStability.Evolving
public class StreamsGroupSubtopologyDescription {

    private final String subtopologyId;
    private final List<String> sourceTopics;
    private final List<String> repartitionSinkTopics;
    private final Map<String, TopicInfo> stateChangelogTopics;
    private final Map<String, TopicInfo> repartitionSourceTopics;

    public StreamsGroupSubtopologyDescription(
        final String subtopologyId,
        final List<String> sourceTopics,
        final List<String> repartitionSinkTopics,
        final Map<String, TopicInfo> stateChangelogTopics,
        final Map<String, TopicInfo> repartitionSourceTopics
    ) {
        this.subtopologyId = Objects.requireNonNull(subtopologyId, "subtopologyId must be non-null");
        this.sourceTopics = Objects.requireNonNull(sourceTopics, "sourceTopics must be non-null");
        this.repartitionSinkTopics = Objects.requireNonNull(repartitionSinkTopics, "repartitionSinkTopics must be non-null");
        this.stateChangelogTopics = Objects.requireNonNull(stateChangelogTopics, "stateChangelogTopics must be non-null");
        this.repartitionSourceTopics = Objects.requireNonNull(repartitionSourceTopics, "repartitionSourceTopics must be non-null");
    }

    /**
     * String to uniquely identify the subtopology.
     */
    public String subtopologyId() {
        return subtopologyId;
    }

    /**
     * The topics the topology reads from.
     */
    public List<String> sourceTopics() {
        return List.copyOf(sourceTopics);
    }

    /**
     * The repartition topics the topology writes to.
     */
    public List<String> repartitionSinkTopics() {
        return List.copyOf(repartitionSinkTopics);
    }

    /**
     * The set of state changelog topics associated with this subtopology.
     */
    public Map<String, TopicInfo> stateChangelogTopics() {
        return Map.copyOf(stateChangelogTopics);
    }

    /**
     * The set of source topics that are internally created repartition topics.
     */
    public Map<String, TopicInfo> repartitionSourceTopics() {
        return Map.copyOf(repartitionSourceTopics);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupSubtopologyDescription that = (StreamsGroupSubtopologyDescription) o;
        return Objects.equals(subtopologyId, that.subtopologyId)
            && Objects.equals(sourceTopics, that.sourceTopics)
            && Objects.equals(repartitionSinkTopics, that.repartitionSinkTopics)
            && Objects.equals(stateChangelogTopics, that.stateChangelogTopics)
            && Objects.equals(repartitionSourceTopics, that.repartitionSourceTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            subtopologyId,
            sourceTopics,
            repartitionSinkTopics,
            stateChangelogTopics,
            repartitionSourceTopics
        );
    }

    @Override
    public String toString() {
        return "(" +
            "subtopologyId='" + subtopologyId + '\'' +
            ", sourceTopics=" + sourceTopics +
            ", repartitionSinkTopics=" + repartitionSinkTopics +
            ", stateChangelogTopics=" + stateChangelogTopics +
            ", repartitionSourceTopics=" + repartitionSourceTopics +
            ')';
    }

    /**
     * Information about a topic. These configs reflect what is required by the topology, but may differ from the current state on the
     * broker.
     */
    public static class TopicInfo {

        private final int partitions;
        private final int replicationFactor;
        private final Map<String, String> topicConfigs;

        public TopicInfo(final int partitions, final int replicationFactor, final Map<String, String> topicConfigs) {
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.topicConfigs = Objects.requireNonNull(topicConfigs, "topicConfigs must be non-null");
        }

        /**
         * The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced.
         */
        public int partitions() {
            return partitions;
        }

        /**
         * The replication factor of the topic. Can be 0 if the default replication factor is used.
         */
        public int replicationFactor() {
            return replicationFactor;
        }

        /**
         * Topic-level configurations as key-value pairs. Default configuration can be omitted.
         */
        public Map<String, String> topicConfigs() {
            return Map.copyOf(topicConfigs);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TopicInfo topicInfo = (TopicInfo) o;
            return partitions == topicInfo.partitions
                && replicationFactor == topicInfo.replicationFactor
                && Objects.equals(topicConfigs, topicInfo.topicConfigs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                partitions,
                replicationFactor,
                topicConfigs
            );
        }

        @Override
        public String toString() {
            return "TopicInfo(" +
                "partitions=" + partitions +
                ", replicationFactor=" + replicationFactor +
                ", topicConfigs=" + topicConfigs.entrySet().stream().map(x -> x.getKey() + "=" + x.getValue())
                .collect(Collectors.joining(",")) +
                ')';
        }
    }

}
