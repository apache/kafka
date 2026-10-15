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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A detailed description of a cluster mirror.
 * <p>
 * This class provides comprehensive information about a cluster mirror retrieved via
 * {@link AdminClient#describeClusterMirrors(String[], DescribeClusterMirrorsOptions)}.
 * <p>
 * In addition to basic mirror metadata (name, source cluster details), it includes
 * per-partition leader state information describing the replication progress and status
 * of each partition in the mirror, as well as the authorized operations the client can perform.
 * <p>
 * The leader states are organized by topic name, with each topic containing the states
 * of all partitions being replicated. The state of each partition includes the current
 * replication lag, the mirroring state machine state, and error information if applicable.
 */
@InterfaceStability.Evolving
public class ClusterMirrorDescription {
    private final String mirrorName;
    private final String sourceBootstrap;
    private final String sourceClusterId;
    private final Map<String, Set<LeaderStateDescription>> leaderStates;
    private final Set<AclOperation> authorizedOperations;

    public ClusterMirrorDescription(String mirrorName,
                                    String sourceBootstrap,
                                    String sourceClusterId,
                                    Map<String, Set<LeaderStateDescription>> leaderStates,
                                    Set<AclOperation> authorizedOperations) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceClusterId = sourceClusterId;
        this.leaderStates = Collections.unmodifiableMap(leaderStates);
        this.authorizedOperations = authorizedOperations;
    }

    /**
     * The name of the cluster mirror. This is unique within the destination cluster.
     */
    public String mirrorName() {
        return mirrorName;
    }

    /**
     * The bootstrap servers of the source cluster being mirrored.
     */
    public String sourceBootstrap() {
        return sourceBootstrap;
    }

    /**
     * The cluster ID of the source cluster being mirrored.
     */
    public String sourceClusterId() {
        return sourceClusterId;
    }

    /**
     * The leader states for each topic being mirrored.
     * <p>
     * The map is keyed by topic name. Each topic maps to a set of
     * {@link LeaderStateDescription} objects, one per partition in that topic.
     * The leader state describes the current replication progress, state machine state,
     * and any errors for the partition.
     * <p>
     * The returned map is unmodifiable.
     */
    public Map<String, Set<LeaderStateDescription>> leaderStates() {
        return leaderStates;
    }

    /**
     * The set of ACL operations the authenticated principal is authorized to perform on this mirror.
     * <p>
     * This field will be empty if no ACL matches the principal or if authorization checks are disabled.
     */
    public Set<AclOperation> authorizedOperations() {
        return authorizedOperations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterMirrorDescription that = (ClusterMirrorDescription) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap) &&
               Objects.equals(sourceClusterId, that.sourceClusterId) &&
               Objects.equals(leaderStates, that.leaderStates) &&
               Objects.equals(authorizedOperations, that.authorizedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap, sourceClusterId, leaderStates, authorizedOperations);
    }

    @Override
    public String toString() {
        return "ClusterMirrorDescription{" +
               "mirrorName='" + mirrorName + '\'' +
               ", sourceBootstrap='" + sourceBootstrap + '\'' +
               ", sourceClusterId='" + sourceClusterId + '\'' +
               ", leaderStates=" + leaderStates +
               ", authorizedOperations=" + authorizedOperations +
               '}';
    }

    /**
     * Represents the detailed mirroring state of a partition in a cluster mirror.
     * <p>
     * This includes the current replication progress (source and destination offsets),
     * the state of the mirroring state machine, and error information if applicable.
     * The lag between source and destination can be computed using {@link #lag()}.
     */
    public static class LeaderStateDescription {
        private final TopicPartition topicPartition;
        private final long sourceOffset;
        private final long destinationOffset;
        private final String state;
        private final int retryAttempt;
        private final String errorMessage;

        public LeaderStateDescription(TopicPartition topicPartition, long sourceOffset, long destinationOffset,
                                      String state, int retryAttempt, String errorMessage) {
            this.topicPartition = topicPartition;
            this.sourceOffset = sourceOffset;
            this.destinationOffset = destinationOffset;
            this.state = state;
            this.retryAttempt = retryAttempt;
            this.errorMessage = errorMessage;
        }

        /**
         * The topic and partition being mirrored.
         */
        public TopicPartition topicPartition() {
            return topicPartition;
        }

        /**
         * The source cluster's current end offset for this partition.
         * <p>
         * This is the high watermark (latest committed offset) in the source cluster.
         * Returns -1 if the offset is unavailable.
         */
        public long sourceOffset() {
            return sourceOffset;
        }

        /**
         * The destination cluster's current replicated offset for this partition.
         * <p>
         * This represents how far replication has progressed; messages up to this offset
         * have been replicated from the source cluster.
         * Returns -1 if the offset is unavailable.
         */
        public long destinationOffset() {
            return destinationOffset;
        }

        /**
         * The replication lag in terms of number of messages behind the source.
         * <p>
         * Computed as max(0, sourceOffset - destinationOffset).
         * Returns -1 if either the source or destination offset is unavailable.
         */
        public long lag() {
            if (sourceOffset < 0 || destinationOffset < 0) {
                return -1;
            }
            return Math.max(0, sourceOffset - destinationOffset);
        }

        /**
         * The current state of the mirroring state machine for this partition.
         * <p>
         * See {@link org.apache.kafka.server.mirror.MirrorPartitionState} for the list of
         * possible state values and their meanings.
         */
        public String state() {
            return state;
        }

        /**
         * The number of times replication has been retried for this partition.
         * <p>
         * Incremented when replication fails and is automatically retried.
         */
        public int retryAttempt() {
            return retryAttempt;
        }

        /**
         * The error message associated with this partition, if any.
         * <p>
         * Non-empty when the partition is in the FAILED state or when an error has occurred.
         * Empty otherwise.
         */
        public String errorMessage() {
            return errorMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeaderStateDescription that = (LeaderStateDescription) o;
            return sourceOffset == that.sourceOffset &&
                   destinationOffset == that.destinationOffset &&
                   retryAttempt == that.retryAttempt &&
                   Objects.equals(topicPartition, that.topicPartition) &&
                   Objects.equals(state, that.state) &&
                   Objects.equals(errorMessage, that.errorMessage);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, sourceOffset, destinationOffset, state, retryAttempt, errorMessage);
        }

        @Override
        public String toString() {
            return "LeaderStateDescription{" +
                   "topicPartition=" + topicPartition +
                   ", sourceOffset=" + sourceOffset +
                   ", destinationOffset=" + destinationOffset +
                   ", state='" + state + '\'' +
                   ", retryAttempt=" + retryAttempt +
                   ", errorMessage='" + errorMessage + '\'' +
                   '}';
        }
    }
}
