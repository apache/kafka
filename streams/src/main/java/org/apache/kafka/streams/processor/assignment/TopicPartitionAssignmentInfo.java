package org.apache.kafka.streams.processor.assignment;

import java.util.Optional;
import java.util.Set;

/**
 * This is a simple container class used during the assignment process to distinguish
 * TopicPartitions type. Since the assignment logic can depend on the type of topic we're
 * looking at, and the rack information of the partition, this container class should have
 * everything necessary to make informed task assignment decisions.
 */
public interface TopicPartitionAssignmentInfo {
    /**
     *
     * @return the string name of the topic.
     */
    String topic();

    /**
     *
     * @return the partition id of this topic partition.
     */
    int partition();

    /**
     *
     * @return whether the underlying topic is a source topic or not. Source changelog topics
     *         are both source topics and changelog topics.
     */
    boolean isSource();

    /**
     *
     * @return whether the underlying topic is a changelog topic or not. Source changelog topics
     *         are both source topics and changelog topics.
     */
    boolean isChangelog();

    /**
     *
     * @return the broker rack ids on which this topic partition resides. If no information could
     *         be found, this will return an empty optional value.
     */
    Optional<Set<String>> rackIds();
}
