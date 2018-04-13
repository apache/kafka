package org.apache.kafka.common.network;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionRecordsStats;
import org.apache.kafka.common.record.RecordsProcessingStats;
import org.apache.kafka.common.requests.RecordsSend;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * An extension of {@link MultiSend}, specifically for cases where {@link RecordsSend} are nested in. This abstraction
 * has the ability to track and maintain {@link RecordsProcessingStats} for each of the underlying {@link RecordsSend}.
 * This is needed when the {@link RecordsSend} needs to further process the enclosed {@link org.apache.kafka.common.record.Records Records},
 * for example when messages are lazily down-converted using {@link org.apache.kafka.common.record.LazyDownConversionRecords LazyDownConversionRecords}.
 */
public class MultiRecordsSend extends MultiSend {
    private final Map<TopicPartition, RecordsProcessingStats> processingStats = new HashMap<>();

    public MultiRecordsSend(String dest, Queue<Send> sends) {
        super(dest, sends);
    }

    public Map<TopicPartition, RecordsProcessingStats> processingStats() {
        return processingStats;
    }

    private void addTopicPartitionProcessingStats(TopicPartitionRecordsStats stats) {
        if (stats != null)
            processingStats.put(stats.topicPartition(), stats.recordsProcessingStats());
    }

    @Override
    protected void onComplete(Send completedSend) {
        // Pull out and record any processing statistics from underlying RecordsSend
        if (completedSend.getClass() == RecordsSend.class)
            addTopicPartitionProcessingStats(((RecordsSend) completedSend).recordsProcessingStats());
    }
}
