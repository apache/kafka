/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class WriteTxnMarkersRequest extends AbstractRequest {
    private static final String COORDINATOR_EPOCH_KEY_NAME = "coordinator_epoch";
    private static final String TXN_MARKERS_KEY_NAME = "transaction_markers";

    private static final String PRODUCER_ID_KEY_NAME = "producer_id";
    private static final String PRODUCER_EPOCH_KEY_NAME = "producer_epoch";
    private static final String TRANSACTION_RESULT_KEY_NAME = "transaction_result";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema WRITE_TXN_MARKERS_ENTRY_V0 = new Schema(
            new Field(PRODUCER_ID_KEY_NAME, INT64, "Current producer id in use by the transactional id."),
            new Field(PRODUCER_EPOCH_KEY_NAME, INT16, "Current epoch associated with the producer id."),
            new Field(TRANSACTION_RESULT_KEY_NAME, BOOLEAN, "The result of the transaction to write to the " +
                    "partitions (false = ABORT, true = COMMIT)."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(INT32)))), "The partitions to write markers for."),
            new Field(COORDINATOR_EPOCH_KEY_NAME, INT32, "Epoch associated with the transaction state partition " +
                    "hosted by this transaction coordinator"));

    private static final Schema WRITE_TXN_MARKERS_REQUEST_V0 = new Schema(
            new Field(TXN_MARKERS_KEY_NAME, new ArrayOf(WRITE_TXN_MARKERS_ENTRY_V0), "The transaction markers to " +
                    "be written."));

    public static Schema[] schemaVersions() {
        return new Schema[]{WRITE_TXN_MARKERS_REQUEST_V0};
    }

    public static class TxnMarkerEntry {
        private final long producerId;
        private final short producerEpoch;
        private final int coordinatorEpoch;
        private final TransactionResult result;
        private final List<TopicPartition> partitions;

        public TxnMarkerEntry(long producerId,
                              short producerEpoch,
                              int coordinatorEpoch,
                              TransactionResult result,
                              List<TopicPartition> partitions) {
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.coordinatorEpoch = coordinatorEpoch;
            this.result = result;
            this.partitions = partitions;
        }

        public long producerId() {
            return producerId;
        }

        public short producerEpoch() {
            return producerEpoch;
        }

        public int coordinatorEpoch() {
            return coordinatorEpoch;
        }

        public TransactionResult transactionResult() {
            return result;
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }


        @Override
        public String toString() {
            return "TxnMarkerEntry{" +
                    "producerId=" + producerId +
                    ", producerEpoch=" + producerEpoch +
                    ", coordinatorEpoch=" + coordinatorEpoch +
                    ", result=" + result +
                    ", partitions=" + partitions +
                    '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final TxnMarkerEntry that = (TxnMarkerEntry) o;
            return producerId == that.producerId &&
                    producerEpoch == that.producerEpoch &&
                    coordinatorEpoch == that.coordinatorEpoch &&
                    result == that.result &&
                    Objects.equals(partitions, that.partitions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(producerId, producerEpoch, coordinatorEpoch, result, partitions);
        }
    }

    public static class Builder extends AbstractRequest.Builder<WriteTxnMarkersRequest> {
        private final List<TxnMarkerEntry> markers;

        public Builder(List<TxnMarkerEntry> markers) {
            super(ApiKeys.WRITE_TXN_MARKERS);
            this.markers = markers;
        }

        @Override
        public WriteTxnMarkersRequest build(short version) {
            return new WriteTxnMarkersRequest(version, markers);
        }
    }

    private final List<TxnMarkerEntry> markers;

    private WriteTxnMarkersRequest(short version, List<TxnMarkerEntry> markers) {
        super(version);

        this.markers = markers;
    }

    public WriteTxnMarkersRequest(Struct struct, short version) {
        super(version);
        List<TxnMarkerEntry> markers = new ArrayList<>();
        Object[] markersArray = struct.getArray(TXN_MARKERS_KEY_NAME);
        for (Object markerObj : markersArray) {
            Struct markerStruct = (Struct) markerObj;

            long producerId = markerStruct.getLong(PRODUCER_ID_KEY_NAME);
            short producerEpoch = markerStruct.getShort(PRODUCER_EPOCH_KEY_NAME);
            int coordinatorEpoch = markerStruct.getInt(COORDINATOR_EPOCH_KEY_NAME);
            TransactionResult result = TransactionResult.forId(markerStruct.getBoolean(TRANSACTION_RESULT_KEY_NAME));

            List<TopicPartition> partitions = new ArrayList<>();
            Object[] topicPartitionsArray = markerStruct.getArray(TOPICS_KEY_NAME);
            for (Object topicPartitionObj : topicPartitionsArray) {
                Struct topicPartitionStruct = (Struct) topicPartitionObj;
                String topic = topicPartitionStruct.get(TOPIC_NAME);
                for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                    partitions.add(new TopicPartition(topic, (Integer) partitionObj));
                }
            }

            markers.add(new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, partitions));
        }

        this.markers = markers;
    }


    public List<TxnMarkerEntry> markers() {
        return markers;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.WRITE_TXN_MARKERS.requestSchema(version()));

        Object[] markersArray = new Object[markers.size()];
        int i = 0;
        for (TxnMarkerEntry entry : markers) {
            Struct markerStruct = struct.instance(TXN_MARKERS_KEY_NAME);
            markerStruct.set(PRODUCER_ID_KEY_NAME, entry.producerId);
            markerStruct.set(PRODUCER_EPOCH_KEY_NAME, entry.producerEpoch);
            markerStruct.set(COORDINATOR_EPOCH_KEY_NAME, entry.coordinatorEpoch);
            markerStruct.set(TRANSACTION_RESULT_KEY_NAME, entry.result.id);

            Map<String, List<Integer>> mappedPartitions = CollectionUtils.groupDataByTopic(entry.partitions);
            Object[] partitionsArray = new Object[mappedPartitions.size()];
            int j = 0;
            for (Map.Entry<String, List<Integer>> topicAndPartitions : mappedPartitions.entrySet()) {
                Struct topicPartitionsStruct = markerStruct.instance(TOPICS_KEY_NAME);
                topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());
                topicPartitionsStruct.set(PARTITIONS_KEY_NAME, topicAndPartitions.getValue().toArray());
                partitionsArray[j++] = topicPartitionsStruct;
            }
            markerStruct.set(TOPICS_KEY_NAME, partitionsArray);
            markersArray[i++] = markerStruct;
        }
        struct.set(TXN_MARKERS_KEY_NAME, markersArray);

        return struct;
    }

    @Override
    public WriteTxnMarkersResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        Map<Long, Map<TopicPartition, Errors>> errors = new HashMap<>(markers.size());
        for (TxnMarkerEntry entry : markers) {
            Map<TopicPartition, Errors> errorsPerPartition = new HashMap<>(entry.partitions.size());
            for (TopicPartition partition : entry.partitions)
                errorsPerPartition.put(partition, error);

            errors.put(entry.producerId, errorsPerPartition);
        }

        return new WriteTxnMarkersResponse(errors);
    }

    public static WriteTxnMarkersRequest parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkersRequest(ApiKeys.WRITE_TXN_MARKERS.parseRequest(version, buffer), version);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final WriteTxnMarkersRequest that = (WriteTxnMarkersRequest) o;
        return Objects.equals(markers, that.markers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(markers);
    }
}
