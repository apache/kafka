package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentConfigs;
import org.slf4j.Logger;

public class RackAwareTaskAssignor {

  private final Cluster fullMetadata;
  private final Map<TaskId, Set<TopicPartition>> partitionsForTask;
  private final Map<UUID, Map<String, Optional<String>>> processRacks;
  private final AssignmentConfigs assignmentConfigs;
  private final Logger log;
  private final Map<TopicPartition, Set<String>> racksForPartition;
  private final InternalTopicManager internalTopicManager;

  public RackAwareTaskAssignor(final Cluster fullMetadata,
                               final Map<TaskId, Set<TopicPartition>> partitionsForTask,
                               final Map<Subtopology, Set<TaskId>> tasksForTopicGroup,
                               final Map<UUID, Map<String, Optional<String>>> processRacks,
                               final InternalTopicManager internalTopicManager,
                               final AssignmentConfigs assignmentConfigs,
                               final String logPrefix) {
    this.fullMetadata = fullMetadata;
    this.partitionsForTask = partitionsForTask;
    this.processRacks = processRacks;
    this.internalTopicManager = internalTopicManager;
    this.assignmentConfigs = assignmentConfigs;
    this.racksForPartition = new HashMap<>();
    final LogContext logContext = new LogContext(logPrefix);
    log = logContext.logger(getClass());
  }

  public boolean canEnable() {
    if (StreamsConfig.RACK_AWARE_ASSSIGNMENT_STRATEGY_NONE.equals(assignmentConfigs.rackAwareAssignmentStrategy)) {
      return false;
    }

    if (!validateClientRack()) {
      return false;
    }

    if (!validateTopicPartitionRack()) {
      return false;
    }

    return true;
  }

  private boolean validateTopicPartitionRack() {
     // Make sure rackId exist for all TopicPartitions needed
    final Set<String> topicsToDescribe = new HashSet<>();
    for (final Set<TopicPartition> topicPartitions : partitionsForTask.values()) {
      for (TopicPartition topicPartition : topicPartitions) {
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
        for (final Node r : replica) {
          if (r.hasRack()) {
            racksForPartition.computeIfAbsent(topicPartition, k -> new HashSet<>()).add(r.rack());
          }
        }
      }
    }

    if (!topicsToDescribe.isEmpty()) {
      log.info("Fetching PartitionInfo for topics {}", topicsToDescribe);
      try {
        final Map<String, List<TopicPartitionInfo>> topicPartitionInfo = internalTopicManager.getTopicPartitionInfo(topicsToDescribe);
        
      } catch (Exception e) {
        log.error("Failed to describe topics {}", topicsToDescribe);
        return false;
      }
    }

    return true;
  }

  private boolean validateClientRack() {
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
          log.warn(
              "Consumers {} and {} for same process {} has different rackId {} and {}. Disable {}",
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
