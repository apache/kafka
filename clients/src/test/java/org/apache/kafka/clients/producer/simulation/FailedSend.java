package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.producer.ProducerRecord;

class FailedSend extends RecordEvent {
    final Exception exception;

    protected FailedSend(
        ProducerRecord<Long, Long> record,
        Exception exception
    ) {
        super(record);
        this.exception = exception;
    }

    @Override
    public String toString() {
        return String.format("FailedSend(value=%d, exception=%s)",
            record.value(),
            exception.getClass().getName()
        );
    }
}
