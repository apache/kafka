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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CapturingTopicMigrationClient implements TopicMigrationClient {
    public List<String> deletedTopics = new ArrayList<>();
    public List<String> createdTopics = new ArrayList<>();
    public LinkedHashMap<String, Map<Integer, PartitionRegistration>> updatedTopics = new LinkedHashMap<>();
    public LinkedHashMap<String, Set<Integer>> newTopicPartitions = new LinkedHashMap<>();
    public LinkedHashMap<String, Set<Integer>> updatedTopicPartitions = new LinkedHashMap<>();
    public LinkedHashMap<String, Set<Integer>> deletedTopicPartitions = new LinkedHashMap<>();


    public void reset() {
        createdTopics.clear();
        updatedTopicPartitions.clear();
        deletedTopics.clear();
        updatedTopics.clear();
        deletedTopicPartitions.clear();
    }


    @Override
    public void iterateTopics(EnumSet<TopicVisitorInterest> interests, TopicVisitor visitor) {

    }

    @Override
    public Set<String> readPendingTopicDeletions() {
        return Collections.emptySet();
    }

    @Override
    public ZkMigrationLeadershipState clearPendingTopicDeletions(
        Set<String> pendingTopicDeletions,
        ZkMigrationLeadershipState state
    ) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState deleteTopic(
        String topicName,
        ZkMigrationLeadershipState state
    ) {
        deletedTopics.add(topicName);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState createTopic(String topicName, Uuid topicId, Map<Integer, PartitionRegistration> topicPartitions, ZkMigrationLeadershipState state) {
        createdTopics.add(topicName);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState updateTopic(
        String topicName,
        Uuid topicId,
        Map<Integer, PartitionRegistration> topicPartitions,
        ZkMigrationLeadershipState state
    ) {
        updatedTopics.put(topicName, topicPartitions);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState createTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, ZkMigrationLeadershipState state) {
        topicPartitions.forEach((topicName, partitionMap) ->
            newTopicPartitions.put(topicName, partitionMap.keySet())
        );
        return state;
    }

    @Override
    public ZkMigrationLeadershipState updateTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, ZkMigrationLeadershipState state) {
        topicPartitions.forEach((topicName, partitionMap) ->
            updatedTopicPartitions.put(topicName, partitionMap.keySet())
        );
        return state;
    }

    @Override
    public ZkMigrationLeadershipState deleteTopicPartitions(Map<String, Set<Integer>> topicPartitions, ZkMigrationLeadershipState state) {
        deletedTopicPartitions.putAll(topicPartitions);
        return state;
    }
}
