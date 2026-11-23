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

package org.apache.kafka.image;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class LocalReplicaChanges {
    // partitions for which the broker is not a replica anymore
    private final Set<TopicPartition> deletes;
    // partitions for which the broker is now a leader (leader epoch bump on the leader)
    private final Map<TopicPartition, PartitionInfo> electedLeaders;
    // partitions for which the isr or replicas change if the broker is a leader (partition epoch bump on the leader)
    private final Map<TopicPartition, PartitionInfo> leaders;
    // partitions for which the broker is now a follower or follower with isr or replica updates (partition epoch bump on follower)
    private final Map<TopicPartition, PartitionInfo> followers;
    // The topic name -> topic id map in leaders and followers changes
    private final Map<String, Uuid> topicIds;
    // partitions for which directory id changes or newly added to the broker
    private final Map<TopicIdPartition, Uuid> directoryIds;
    // topic id -> mirror topic state map
    private final Map<Uuid, MirrorTopicState> mirrorTopicStates;

    LocalReplicaChanges(
        Set<TopicPartition> deletes,
        Map<TopicPartition, PartitionInfo> electedLeaders,
        Map<TopicPartition, PartitionInfo> leaders,
        Map<TopicPartition, PartitionInfo> followers,
        Map<String, Uuid> topicIds,
        Map<TopicIdPartition, Uuid> directoryIds,
        Map<Uuid, MirrorTopicState> mirrorTopicStates
    ) {
        this.deletes = deletes;
        this.electedLeaders = electedLeaders;
        this.leaders = leaders;
        this.followers = followers;
        this.topicIds = topicIds;
        this.directoryIds = directoryIds;
        this.mirrorTopicStates = mirrorTopicStates;
    }

    public Set<TopicPartition> deletes() {
        return deletes;
    }

    public Map<TopicPartition, PartitionInfo> electedLeaders() {
        return electedLeaders;
    }

    public Map<TopicPartition, PartitionInfo> leaders() {
        return leaders;
    }

    public Map<TopicPartition, PartitionInfo> followers() {
        return followers;
    }

    public Map<String, Uuid> topicIds() {
        return topicIds;
    }

    public Map<TopicIdPartition, Uuid> directoryIds() {
        return directoryIds;
    }

    public Map<Uuid, MirrorTopicState> mirrorTopicStates() {
        return mirrorTopicStates;
    }

    @Override
    public String toString() {
        return String.format(
            "LocalReplicaChanges(deletes = %s, newly elected leaders = %s, leaders = %s, followers = %s, topicIds = %s, directoryIds = %s, mirrorTopicStates = %s)",
            deletes,
            electedLeaders,
            leaders,
            followers,
            topicIds,
            directoryIds,
            mirrorTopicStates
        );
    }

    public record PartitionInfo(Uuid topicId, PartitionRegistration partition) {
    }

    /**
     * The mirror state of a topic after a delta is applied.
     */
    public static final class MirrorTopicState {
        private final TopicImage.Mirror mirror;

        /**
         * @param mirror The mirror state, or null if the topic is no longer associated with a mirror.
         */
        public MirrorTopicState(TopicImage.Mirror mirror) {
            this.mirror = mirror;
        }

        /**
         * Returns the mirror state, or empty if the topic is no longer associated with a mirror.
         */
        public Optional<TopicImage.Mirror> mirror() {
            return Optional.ofNullable(mirror);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MirrorTopicState other)) return false;
            return Objects.equals(mirror, other.mirror);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(mirror);
        }

        @Override
        public String toString() {
            return "MirrorTopicState(mirror=" + mirror + ")";
        }
    }
}
