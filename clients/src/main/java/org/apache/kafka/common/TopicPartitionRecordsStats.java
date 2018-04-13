package org.apache.kafka.common;

import org.apache.kafka.common.record.RecordsProcessingStats;

public class TopicPartitionRecordsStats {
    private final TopicPartition topicPartition;
    private final RecordsProcessingStats recordsProcessingStats;

    public TopicPartitionRecordsStats(TopicPartition topicPartition, RecordsProcessingStats recordsProcessingStats) {
        this.topicPartition = topicPartition;
        this.recordsProcessingStats = recordsProcessingStats;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public RecordsProcessingStats recordsProcessingStats() {
        return recordsProcessingStats;
    }
}
