package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.OptionalLong;

class ProducerStateModel {
    final ArrayDeque<RecordBatch> cache = new ArrayDeque<>();
    private long producerId;
    private short epoch;
    private int sequence;

    ProducerStateModel(
        long producerId,
        short epoch,
        int sequence
    ) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.sequence = sequence;
    }

    int sequence() {
        return sequence;
    }

    short epoch() {
        return epoch;
    }

    long producerId() {
        return producerId;
    }

    OptionalLong checkDuplicateSequence(
        short epoch,
        int sequence,
        int numRecords
    ) {
        for (RecordBatch batch : cache) {
            if (batch.producerEpoch() == epoch
                && batch.baseSequence() == sequence
                && batch.countOrNull() == numRecords) {
                return OptionalLong.of(batch.baseOffset());
            }
        }
        return OptionalLong.empty();
    }

    Optional<Errors> checkNextSequence(
        short epoch,
        int sequence
    ) {
        if (epoch < this.epoch) {
            return Optional.of(Errors.INVALID_PRODUCER_EPOCH);
        } else if (epoch == this.epoch && sequence != this.sequence) {
            return Optional.of(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
        } else if (epoch > this.epoch && sequence != 0) {
            return Optional.of(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
        } else {
            return Optional.empty();
        }
    }

    void onAppend(RecordBatch batch) {
        this.epoch = batch.producerEpoch();
        this.sequence = batch.lastSequence() + 1;

        cache.add(batch);
        if (cache.size() > 5) {
            cache.pop();
        }
    }
}
