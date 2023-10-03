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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Set;
import java.util.Map;

public final class LocalReplicaChanges {
    private final Set<TopicPartition> deletes;
    private final Map<TopicPartition, PartitionInfo> electedLeaders;
    private final Map<TopicPartition, PartitionInfo> updatedLeaders;
    private final Map<TopicPartition, PartitionInfo> followers;
    // The topic name -> topic id map in leaders and followers changes
    private final Map<String, Uuid> topicIds;

    LocalReplicaChanges(
        Set<TopicPartition> deletes,
        Map<TopicPartition, PartitionInfo> electedLeaders,
        Map<TopicPartition, PartitionInfo> updatedLeaders,
        Map<TopicPartition, PartitionInfo> followers,
        Map<String, Uuid> topicIds
    ) {
        this.deletes = deletes;
        this.electedLeaders = electedLeaders;
        this.updatedLeaders = updatedLeaders;
        this.followers = followers;
        this.topicIds = topicIds;
    }

    public Set<TopicPartition> deletes() {
        return deletes;
    }

    public Map<TopicPartition, PartitionInfo> electedLeaders() {
        return electedLeaders;
    }

    public Map<TopicPartition, PartitionInfo> updatedLeaders() {
        return updatedLeaders;
    }

    public Map<TopicPartition, PartitionInfo> followers() {
        return followers;
    }

    public Map<String, Uuid> topicIds() {
        return topicIds;
    }

    @Override
    public String toString() {
        return String.format(
            "LocalReplicaChanges(deletes = %s, newly elected leaders = %s, updated leaders = %s, followers = %s)",
            deletes,
            electedLeaders,
            updatedLeaders,
            followers
        );
    }

    public static final class PartitionInfo {
        private final Uuid topicId;
        private final PartitionRegistration partition;

        public PartitionInfo(Uuid topicId, PartitionRegistration partition) {
            this.topicId = topicId;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return String.format("PartitionInfo(topicId = %s, partition = %s)", topicId, partition);
        }

        public Uuid topicId() {
            return topicId;
        }

        public PartitionRegistration partition() {
            return partition;
        }
    }
}
