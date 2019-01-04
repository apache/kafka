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
package org.apache.kafka.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A representation of a subset of the nodes, topics, and partitions in the Kafka cluster.
 */
public interface Cluster {

    Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions);

    List<Node> nodes();

    Node nodeById(int id);

    Node leaderFor(TopicPartition topicPartition);

    PartitionInfo partition(TopicPartition topicPartition);

    List<PartitionInfo> partitionsForTopic(String topic);

    int partitionCountForTopic(String topic);

    List<PartitionInfo> availablePartitionsForTopic(String topic);

    List<PartitionInfo> partitionsForNode(int nodeId);

    boolean removePartition(TopicPartition topicPartition);

    Set<String> topics();

    Set<String> unauthorizedTopics();

    Set<String> invalidTopics();

    Set<String> internalTopics();

    boolean isBootstrapConfigured();

    ClusterResource clusterResource();

    Node controller();
}
