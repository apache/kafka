package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.producer.ProducerRecord;

class SuccessfulSend extends RecordEvent {
    protected SuccessfulSend(
        ProducerRecord<Long, Long> record
    ) {
        super(record);
    }

    @Override
    public String toString() {
        return String.format("SuccessfulSend(value=%d)", record.value());
    }
}
