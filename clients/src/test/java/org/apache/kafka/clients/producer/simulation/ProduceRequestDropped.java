package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.record.RecordBatch;

class ProduceRequestDropped extends ProduceEvent {
    ProduceRequestDropped(
        RecordBatch batch,
        int partitionId,
        long connectionId
    ) {
        super(batch, partitionId, connectionId);
    }

    @Override
    public String toString() {
        return String.format("ProduceRequestDropped(" +
                "partitionId=%d, " +
                "connectionId=%d, " +
                "epoch=%d, " +
                "seq=[%d, %d], " +
                "value=%s)",
            partitionId,
            connectionId,
            batch.producerEpoch(),
            batch.baseSequence(),
            batch.lastSequence(),
            SimulationUtils.batchValueRangeAsString(batch)
        );
    }
}
