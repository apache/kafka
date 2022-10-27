package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

class ProduceRequestHandled extends ProduceEvent {
    private final Errors error;

    ProduceRequestHandled(
        RecordBatch batch,
        int partitionId,
        long connectionId,
        Errors error
    ) {
        super(batch, partitionId, connectionId);
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format("ProduceRequestHandled(" +
                "partitionId=%d, " +
                "connectionId=%d, " +
                "epoch=%d, " +
                "seq=[%d, %d], " +
                "value=%s, " +
                "offset=%d, " +
                "error=%s)",
            partitionId,
            connectionId,
            batch.producerEpoch(),
            batch.baseSequence(),
            batch.lastSequence(),
            SimulationUtils.batchValueRangeAsString(batch),
            batch.baseOffset(),
            error
        );
    }
}
