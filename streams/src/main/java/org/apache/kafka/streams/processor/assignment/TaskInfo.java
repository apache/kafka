package org.apache.kafka.streams.processor.assignment;

import java.util.Set;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.TaskId;

/**
 * A simple container class corresponding to a given {@link TaskId}.
 * Includes metadata such as whether it's stateful and the names of all state stores
 * belonging to this task, the set of input topic partitions and changelog topic partitions
 * for all logged state stores, and the rack ids of all replicas of each topic partition
 * in the task.
 */
public interface TaskInfo {

    TaskId id();

    boolean isStateful();
    
    Set<String> stateStoreNames();
    
    Set<TopicPartition> inputTopicPartitions();
    
    Set<TopicPartition> changelogTopicPartitions();

    Map<TopicPartition, Set<String>> partitionToRackId();
}
