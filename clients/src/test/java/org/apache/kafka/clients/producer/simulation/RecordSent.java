package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.producer.ProducerRecord;

class RecordSent extends RecordEvent {
    protected RecordSent(ProducerRecord<Long, Long> record) {
        super(record);
    }

    @Override
    public String toString() {
        return String.format("RecordSent(value=%d)", record.value());
    }
}
