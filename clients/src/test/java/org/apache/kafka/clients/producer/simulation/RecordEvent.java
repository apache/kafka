package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.producer.ProducerRecord;

abstract class RecordEvent implements SimulationEvent {
    final ProducerRecord<Long, Long> record;

    RecordEvent(ProducerRecord<Long, Long> record) {
        this.record = record;
    }
}
