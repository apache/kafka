package org.apache.kafka.streams.processor.internals.assignment;

import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.assignment.TopicPartitionAssignmentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple container class used during the assignment process to distinguish
 * TopicPartitions type. Since the assignment logic can depend on the type of topic we're
 * looking at, and the rack information of the partition, this container class should have
 * everything necessary to make informed task assignment decisions.
 */
public class DefaultTopicPartitionAssignmentInfo implements TopicPartitionAssignmentInfo {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTopicPartitionAssignmentInfo.class);

    private final TopicPartition topicPartition;
    private final boolean isSourceTopic;
    private final boolean isChangelogTopic;
    private final Optional<Set<String>> rackIds;

    public DefaultTopicPartitionAssignmentInfo(final TopicPartition topicPartition,
                                               final boolean isSourceTopic,
                                               final boolean isChangelogTopic,
                                               final Set<String> rackIds) {
        this.topicPartition = topicPartition;
        this.isSourceTopic = isSourceTopic;
        this.isChangelogTopic = isChangelogTopic;
        this.rackIds = Optional.ofNullable(rackIds);
    }

    @Override
    public String topic() {
        return topicPartition.topic();
    }

    @Override
    public int partition() {
        return topicPartition.partition();
    }

    @Override
    public boolean isSource() {
        return isSourceTopic;
    }

    @Override
    public boolean isChangelog() {
        return isChangelogTopic;
    }

    @Override
    public Optional<Set<String>> rackIds() {
        return rackIds;
    }
}
