package org.apache.kafka.common.record;

import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;

public class DefaultRecordBatchModify extends DefaultRecordBatch {

    private final RecordBatch recordBatch;
    private TopicPartition partition;

    public DefaultRecordBatchModify(ByteBuffer buffer, RecordBatch recordBatch, TopicPartition partition) {
        super(buffer);
        this.recordBatch = recordBatch;
        this.partition = partition;
    }

    public RecordBatch getRecordBatch() {
        return recordBatch;
    }

    public TopicPartition getPartition() {
        return partition;
    }

    public void setPartition(TopicPartition partition) {
        this.partition = partition;
    }

    public ByteBuffer getBuffer() {
        return super.getBuffer();
    }

    @Override
    public String toString() {
        return recordBatch.toString();
    }

}
