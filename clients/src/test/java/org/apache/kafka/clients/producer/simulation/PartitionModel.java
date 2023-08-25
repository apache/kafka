package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.fail;

class PartitionModel {
    final int partitionId;
    final boolean enableStrictValidation;
    int leaderId;
    int leaderEpoch;
    long underMinIsrUntilTimestamp = -1;
    long invalidRecordsUntilTimestamp = -1;
    final List<RecordBatch> log = new ArrayList<>();
    final Map<Long, ProducerStateModel> producers;
    final Set<Long> invalidRecordValues = new HashSet<>();

    PartitionModel(
        int partitionId,
        boolean enableStrictValidation
    ) {
        this.partitionId = partitionId;
        this.enableStrictValidation = enableStrictValidation;
        this.leaderId = -1;
        this.leaderEpoch = 0;
        this.producers = new HashMap<>();
    }

    void setNewLeader(int brokerId) {
        this.leaderId = brokerId;
        this.leaderEpoch++;
    }

    private long nextOffset() {
        if (log.isEmpty()) {
            return 0;
        } else {
            return log.get(log.size() - 1).nextOffset();
        }
    }

    void enableUnderMinIsrUntil(long underMinIsrUntilTimestamp) {
        this.underMinIsrUntilTimestamp = underMinIsrUntilTimestamp;
    }

    void enableRandomInvalidRecordErrors(double probability) {
    }

    void enableInvalidRecordErrorsUntil(long invalidRecordsUntilTimestamp) {
        this.invalidRecordsUntilTimestamp = invalidRecordsUntilTimestamp;
    }

    boolean isUnderMinIsr(long currentTimeMs) {
        return underMinIsrUntilTimestamp > currentTimeMs;
    }

    boolean hasInvalidRecord(long currentTimeMs, RecordBatch batch) {
        Record firstRecord = batch.iterator().next();
        Long recordValue = firstRecord.value().duplicate().getLong();
        if (invalidRecordValues.contains(recordValue)) {
            return true;
        } else if (invalidRecordsUntilTimestamp > currentTimeMs) {
            invalidRecordValues.add(recordValue);
            return true;
        } else {
            return false;
        }
    }

    private long logStartOffset() {
        return 0;
    }

    ProduceResponseData.PartitionProduceResponse tryProduce(DefaultRecordBatch batch) {
        long producerId = batch.producerId();
        short epoch = batch.producerEpoch();
        int sequence = batch.baseSequence();
        int numRecords = batch.countOrNull();

        ProduceResponseData.PartitionProduceResponse response =
            new ProduceResponseData.PartitionProduceResponse()
                .setIndex(this.partitionId);

        ProducerStateModel currentState = producers.get(producerId);
        if (currentState == null) {
            if (enableStrictValidation && sequence != 0) {
                response.setErrorCode(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.code());
                return response;
            } else {
                currentState = new ProducerStateModel(
                    producerId,
                    epoch,
                    sequence
                );
                producers.put(producerId, currentState);
            }
        } else {
            OptionalLong offsetOpt = currentState.checkDuplicateSequence(epoch, sequence, numRecords);
            if (offsetOpt.isPresent()) {
                response.setBaseOffset(offsetOpt.getAsLong());
                response.setErrorCode(Errors.NONE.code());
                return response;
            }

            Optional<Errors> errorOpt = currentState.checkNextSequence(epoch, sequence);
            if (errorOpt.isPresent()) {
                response.setErrorCode(errorOpt.get().code());
                return response;
            }
        }

        batch.setLastOffset(nextOffset() + batch.countOrNull() - 1);
        log.add(batch);
        currentState.onAppend(batch);

        response.setErrorCode(Errors.NONE.code());
        response.setLogStartOffset(logStartOffset());
        response.setBaseOffset(batch.baseOffset());
        return response;
    }

    void assertNoDuplicates() {
        Set<Long> storedValues = new HashSet<>();
        for (RecordBatch batch : log) {
            for (Long value : recordValues(batch)) {
                if (!storedValues.add(value)) {
                    fail("Log contained multiple entries of value " + value);
                }
            }
        }
    }

    void assertNoReordering() {
        Long lastValue = null;
        for (RecordBatch batch : log) {
            for (Long value : recordValues(batch)) {
                if (lastValue != null && value < lastValue) {
                    fail(String.format("Value %d was re-ordered after value %d", value, lastValue));
                }
                lastValue = value;
            }
        }
    }

    private static List<Long> recordValues(RecordBatch batch) {
        List<Long> values = new ArrayList<>(batch.countOrNull());
        for (Record record : batch) {
            values.add(record.value().duplicate().getLong());
        }
        return values;
    }
}
