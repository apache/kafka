package kafka.clients.producer;

import kafka.common.TopicPartition;

/**
 * The metadata for a record that has been acknowledged by the server
 */
public final class RecordMetadata {

    private final long offset;
    private final TopicPartition topicPartition;

    public RecordMetadata(TopicPartition topicPartition, long offset) {
        super();
        this.offset = offset;
        this.topicPartition = topicPartition;
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return this.offset;
    }

    /**
     * The topic the record was appended to
     */
    public String topic() {
        return this.topicPartition.topic();
    }

    /**
     * The partition the record was sent to
     */
    public int partition() {
        return this.topicPartition.partition();
    }
}
