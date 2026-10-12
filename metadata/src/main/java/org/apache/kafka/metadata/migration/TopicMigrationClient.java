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

package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TopicMigrationClient {

    enum TopicVisitorInterest {
        TOPICS,
        PARTITIONS
    }

    interface TopicVisitor {
        void visitTopic(String topicName, Uuid topicId, Map<Integer, List<Integer>> assignments);
        default void visitPartition(TopicIdPartition topicIdPartition, PartitionRegistration partitionRegistration) {

        }

        /**
         * Visit a partition which is included in a topic's replica assignment, but which has no partition state
         * (leader and ISR) in ZooKeeper. Such partitions are left behind when a controller fails in between
         * writing the topic assignment and the partition states.
         * <p>
         * The given record only has the topic ID, partition ID and replica assignment set. Callers for which
         * ZooKeeper is the source of truth (i.e. the ZK to KRaft migration) should override this method, since
         * skipping such a partition would drop it from the migrated metadata. Callers for which KRaft is the
         * source of truth (i.e. the KRaft to ZK dual-write) should not, since the missing partition state is
         * meant to be (re)created from the KRaft state rather than inferred from ZooKeeper.
         * <p>
         * Only invoked if {@link TopicVisitorInterest#PARTITIONS} is included in the interests.
         */
        default void visitPartitionWithoutState(TopicIdPartition topicIdPartition, PartitionRecord partitionRecord) {

        }
    }

    void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor);

    Set<String> readPendingTopicDeletions();

    ZkMigrationLeadershipState clearPendingTopicDeletions(
        Set<String> pendingTopicDeletions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState deleteTopic(
        String topicName,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState createTopic(
        String topicName,
        Uuid topicId,
        Map<Integer, PartitionRegistration> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState updateTopic(
        String topicName,
        Uuid topicId,
        Map<Integer, PartitionRegistration> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState createTopicPartitions(
        Map<String, Map<Integer, PartitionRegistration>> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState updateTopicPartitions(
        Map<String, Map<Integer, PartitionRegistration>> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState deleteTopicPartitions(
        Map<String, Set<Integer>> topicPartitions,
        ZkMigrationLeadershipState state
    );
}
