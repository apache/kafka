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
package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RackAwareTaskAssignor {
    private static final Logger log = LoggerFactory.getLogger(RackAwareTaskAssignor.class);

    private final Cluster fullMetadata;
    private final Map<TaskId, Set<TopicPartition>> partitionsForTask;
    private final Map<UUID, Map<String, Optional<String>>> processRacks;
    private final AssignmentConfigs assignmentConfigs;
    private final Map<TopicPartition, Set<String>> racksForPartition;
    private final InternalTopicManager internalTopicManager;

    public RackAwareTaskAssignor(final Cluster fullMetadata,
                                 final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                                 final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                                 final Map<UUID, Map<String, Optional<String>>> processRacks,
                                 final InternalTopicManager internalTopicManager,
                                 final AssignmentConfigs assignmentConfigs) {
        this.fullMetadata = fullMetadata;
        this.partitionsForTask = partitionsForTask;
        this.processRacks = processRacks;
        this.internalTopicManager = internalTopicManager;
        this.assignmentConfigs = assignmentConfigs;
        this.racksForPartition = new HashMap<>();
    }

    public synchronized boolean canEnableRackAwareAssignorForActiveTasks() {
        /*
        TODO: enable this after we add the config
        if (StreamsConfig.RACK_AWARE_ASSSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
            canEnableForActive = false;
            return false;
        }
         */

        if (!validateClientRack()) {
            return false;
        }

        return validateTopicPartitionRack();
    }

    public boolean canEnableRackAwareAssignorForStandbyTasks() {
        // TODO
        return false;
    }

    // Visible for testing. This method also checks if all TopicPartitions exist in cluster
    public boolean populateTopicsToDiscribe(final Set<String> topicsToDescribe) {
        // Make sure rackId exist for all TopicPartitions needed
        for (final Set<TopicPartition> topicPartitions : partitionsForTask.values()) {
            for (final TopicPartition topicPartition : topicPartitions) {
                final PartitionInfo partitionInfo = fullMetadata.partition(topicPartition);
                if (partitionInfo == null) {
                    log.error("TopicPartition {} doesn't exist in cluster", topicPartition);
                    return false;
                }
                final Node[] replica = partitionInfo.replicas();
                if (replica == null || replica.length == 0) {
                    topicsToDescribe.add(topicPartition.topic());
                    continue;
                }
                for (final Node node : replica) {
                    if (node.hasRack()) {
                        racksForPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(node.rack());
                    } else {
                        log.warn("Node {} for topic partition {} doesn't have rack", node, topicPartition);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean validateTopicPartitionRack() {
        // Make sure rackId exist for all TopicPartitions needed
        final Set<String> topicsToDescribe = new HashSet<>();
        if (!populateTopicsToDiscribe(topicsToDescribe)) {
            return false;
        }

        if (!topicsToDescribe.isEmpty()) {
            log.info("Fetching PartitionInfo for topics {}", topicsToDescribe);
            try {
                final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = internalTopicManager.getTopicPartitionInfo(topicsToDescribe);
                if (topicsToDescribe.size() > topicPartitionInfo.size()) {
                    topicsToDescribe.removeAll(topicPartitionInfo.keySet());
                    log.error("Failed to describe topic for {}", topicsToDescribe);
                    return false;
                }
                for (final Map.Entry<String, List<TopicPartitionInfo>> entry : topicPartitionInfo.entrySet()) {
                    final List<TopicPartitionInfo> partitionInfos = entry.getValue();
                    for (final TopicPartitionInfo partitionInfo : partitionInfos) {
                        final int partition = partitionInfo.partition();
                        final List<Node> replicas = partitionInfo.replicas();
                        if (replicas == null || replicas.isEmpty()) {
                            log.error("No replicas found for topic partition {}: {}", entry.getKey(), partition);
                            return false;
                        }
                        final TopicPartition topicPartition = new TopicPartition(entry.getKey(), partition);
                        for (final Node node : replicas) {
                            if (node.hasRack()) {
                                racksForPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(node.rack());
                            } else {
                                return false;
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                log.error("Failed to describe topics {}", topicsToDescribe, e);
                return false;
            }
        }
        return true;
    }

    // Visible for testing
    public boolean validateClientRack() {
        /*
         * Check rack information is populated correctly in clients
         * 1. RackId exist for all clients
         * 2. Different consumerId for same process should have same rackId
         */
        for (final Map.Entry<UUID, Map<String, Optional<String>>> entry : processRacks.entrySet()) {
            final UUID processId = entry.getKey();
            KeyValue<String, String> previousRackInfo = null;
            for (final Map.Entry<String, Optional<String>> rackEntry : entry.getValue().entrySet()) {
                if (!rackEntry.getValue().isPresent()) {
                    log.warn("RackId doesn't exist for process {} and consumer {}. Disable {}",
                        processId, rackEntry.getKey(), getClass().getName());
                    return false;
                }
                if (previousRackInfo == null) {
                    previousRackInfo = KeyValue.pair(rackEntry.getKey(), rackEntry.getValue().get());
                } else if (!previousRackInfo.value.equals(rackEntry.getValue().get())) {
                    log.error(
                        "Consumers {} and {} for same process {} has different rackId {} and {}. File a ticket for this bug. Disable {}",
                        previousRackInfo.key,
                        rackEntry.getKey(),
                        entry.getKey(),
                        previousRackInfo.value,
                        rackEntry.getValue().get(),
                        getClass().getName());
                    return false;
                }
            }
        }
        return true;
    }
}
