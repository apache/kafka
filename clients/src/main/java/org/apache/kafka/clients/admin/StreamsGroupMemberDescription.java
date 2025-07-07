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
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A detailed description of a single streams groups member in the cluster.
 */
@InterfaceStability.Evolving
public class StreamsGroupMemberDescription {

    private final String memberId;
    private final int memberEpoch;
    private final Optional<String> instanceId;
    private final Optional<String> rackId;
    private final String clientId;
    private final String clientHost;
    private final int topologyEpoch;
    private final String processId;
    private final Optional<Endpoint> userEndpoint;
    private final Map<String, String> clientTags;
    private final List<TaskOffset> taskOffsets;
    private final List<TaskOffset> taskEndOffsets;
    private final StreamsGroupMemberAssignment assignment;
    private final StreamsGroupMemberAssignment targetAssignment;
    private final boolean isClassic;

    @SuppressWarnings("ParameterNumber")
    public StreamsGroupMemberDescription(
        final String memberId,
        final int memberEpoch,
        final Optional<String> instanceId,
        final Optional<String> rackId,
        final String clientId,
        final String clientHost,
        final int topologyEpoch,
        final String processId,
        final Optional<Endpoint> userEndpoint,
        final Map<String, String> clientTags,
        final List<TaskOffset> taskOffsets,
        final List<TaskOffset> taskEndOffsets,
        final StreamsGroupMemberAssignment assignment,
        final StreamsGroupMemberAssignment targetAssignment,
        final boolean isClassic
    ) {
        this.memberId = Objects.requireNonNull(memberId);
        this.memberEpoch = memberEpoch;
        this.instanceId = Objects.requireNonNull(instanceId);
        this.rackId = Objects.requireNonNull(rackId);
        this.clientId = Objects.requireNonNull(clientId);
        this.clientHost = Objects.requireNonNull(clientHost);
        this.topologyEpoch = topologyEpoch;
        this.processId = Objects.requireNonNull(processId);
        this.userEndpoint = Objects.requireNonNull(userEndpoint);
        this.clientTags = Objects.requireNonNull(clientTags);
        this.taskOffsets = Objects.requireNonNull(taskOffsets);
        this.taskEndOffsets = Objects.requireNonNull(taskEndOffsets);
        this.assignment = Objects.requireNonNull(assignment);
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        this.isClassic = isClassic;
    }

    /**
     * The id of the group member.
     */
    public String memberId() {
        return memberId;
    }

    /**
     * The epoch of the group member.
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    /**
     * The id of the instance, used for static membership, if available.
     */
    public Optional<String> instanceId() {
        return instanceId;
    }

    /**
     * The rack ID of the group member.
     */
    public Optional<String> rackId() {
        return rackId;
    }

    /**
     * The client ID of the group member.
     */
    public String clientId() {
        return clientId;
    }

    /**
     * The host of the group member.
     */
    public String clientHost() {
        return clientHost;
    }

    /**
     * The epoch of the topology present on the client.
     */
    public int topologyEpoch() {
        return topologyEpoch;
    }

    /**
     * Identity of the streams instance that may have multiple clients.
     */
    public String processId() {
        return processId;
    }

    /**
     * User-defined endpoint for Interactive Queries.
     */
    public Optional<Endpoint> userEndpoint() {
        return userEndpoint;
    }

    /**
     * Used for rack-aware assignment algorithm.
     */
    public Map<String, String> clientTags() {
        return Map.copyOf(clientTags);
    }

    /**
     * Cumulative offsets for tasks.
     */
    public List<TaskOffset> taskOffsets() {
        return List.copyOf(taskOffsets);
    }

    /**
     * Cumulative task changelog end offsets for tasks.
     */
    public List<TaskOffset> taskEndOffsets() {
        return List.copyOf(taskEndOffsets);
    }

    /**
     * The current assignment.
     */
    public StreamsGroupMemberAssignment assignment() {
        return assignment;
    }

    /**
     * The target assignment.
     */
    public StreamsGroupMemberAssignment targetAssignment() {
        return targetAssignment;
    }

    /**
     * The flag indicating whether a member is classic.
     */
    public boolean isClassic() {
        return isClassic;
    }

    @SuppressWarnings("CyclomaticComplexity")
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StreamsGroupMemberDescription that = (StreamsGroupMemberDescription) o;
        return memberEpoch == that.memberEpoch
            && topologyEpoch == that.topologyEpoch
            && isClassic == that.isClassic
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(instanceId, that.instanceId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(processId, that.processId)
            && Objects.equals(userEndpoint, that.userEndpoint)
            && Objects.equals(clientTags, that.clientTags)
            && Objects.equals(taskOffsets, that.taskOffsets)
            && Objects.equals(taskEndOffsets, that.taskEndOffsets)
            && Objects.equals(assignment, that.assignment)
            && Objects.equals(targetAssignment, that.targetAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            memberId,
            memberEpoch,
            instanceId,
            rackId,
            clientId,
            clientHost,
            topologyEpoch,
            processId,
            userEndpoint,
            clientTags,
            taskOffsets,
            taskEndOffsets,
            assignment,
            targetAssignment,
            isClassic
        );
    }

    @Override
    public String toString() {
        return "(" +
            "memberId=" + memberId +
            ", memberEpoch=" + memberEpoch +
            ", instanceId=" + instanceId.orElse("null") +
            ", rackId=" + rackId.orElse("null") +
            ", clientId=" + clientId +
            ", clientHost=" + clientHost +
            ", topologyEpoch=" + topologyEpoch +
            ", processId=" + processId +
            ", userEndpoint=" + userEndpoint.map(Endpoint::toString).orElse("null") +
            ", clientTags=" + clientTags +
            ", taskOffsets=" + taskOffsets.stream().map(TaskOffset::toString).collect(Collectors.joining(",")) +
            ", taskEndOffsets=" + taskEndOffsets.stream().map(TaskOffset::toString).collect(Collectors.joining(",")) +
            ", assignment=" + assignment +
            ", targetAssignment=" + targetAssignment +
            ", isClassic=" + isClassic +
            ')';
    }

    /**
     * The user-defined endpoint for the member.
     */
    public static class Endpoint {

        private final String host;
        private final int port;

        public Endpoint(final String host, final int port) {
            this.host = Objects.requireNonNull(host);
            this.port = port;
        }

        public String host() {
            return host;
        }

        public int port() {
            return port;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Endpoint endpoint = (Endpoint) o;
            return port == endpoint.port && Objects.equals(host, endpoint.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return "(" +
                "host=" + host +
                ", port=" + port +
                ')';
        }
    }

    /**
     * The cumulative offset for one task.
     */
    public static class TaskOffset {

        private final String subtopologyId;
        private final int partition;
        private final long offset;

        public TaskOffset(final String subtopologyId, final int partition, final long offset) {
            this.subtopologyId = Objects.requireNonNull(subtopologyId);
            this.partition = partition;
            this.offset = offset;
        }

        /**
         * The subtopology identifier.
         */
        public String subtopologyId() {
            return subtopologyId;
        }

        /**
         * The partition of the task.
         */
        public int partition() {
            return partition;
        }

        /**
         * The cumulative offset (sum of offsets in all input partitions).
         */
        public long offset() {
            return offset;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TaskOffset that = (TaskOffset) o;
            return partition == that.partition
                && offset == that.offset
                && Objects.equals(subtopologyId, that.subtopologyId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                subtopologyId,
                partition,
                offset
            );
        }

        @Override
        public String toString() {
            return subtopologyId +
                "_" + partition +
                "=" + offset;
        }
    }
}
