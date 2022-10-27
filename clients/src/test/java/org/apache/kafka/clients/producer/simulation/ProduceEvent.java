package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.record.RecordBatch;

abstract class ProduceEvent implements SimulationEvent {
    final RecordBatch batch;
    final int partitionId;
    final long connectionId;

    ProduceEvent(
        RecordBatch batch,
        int partitionId,
        long connectionId
    ) {
        this.batch = batch;
        this.partitionId = partitionId;
        this.connectionId = connectionId;
    }
}
