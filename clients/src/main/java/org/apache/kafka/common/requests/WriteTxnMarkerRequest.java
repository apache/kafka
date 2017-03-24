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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteTxnMarkerRequest extends AbstractRequest {
    private static final String COORDINATOR_EPOCH_KEY_NAME = "coordinator_epoch";
    private static final String TXN_MARKER_ENTRY_KEY_NAME = "transaction_markers";

    private static final String PID_KEY_NAME = "pid";
    private static final String EPOCH_KEY_NAME = "epoch";
    private static final String TRANSACTION_RESULT_KEY_NAME = "transaction_result";
    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    public class TxnMarkerEntry {
        private final long pid;
        private final short epoch;
        private final TransactionResult result;
        private final List<TopicPartition> partitions;

        public TxnMarkerEntry(long pid, short epoch, TransactionResult result, List<TopicPartition> partitions) {
            this.pid = pid;
            this.epoch = epoch;
            this.result = result;
            this.partitions = partitions;
        }

        public long pid() {
            return pid;
        }

        public short epoch() {
            return epoch;
        }

        public TransactionResult transactionResult() {
            return result;
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }
    }

    public static class Builder extends AbstractRequest.Builder<WriteTxnMarkerRequest> {
        private final int coordinatorEpoch;
        private final List<TxnMarkerEntry> markers;

        public Builder(int coordinatorEpoch, List<TxnMarkerEntry> markers) {
            super(ApiKeys.WRITE_TXN_MARKER);

            this.markers = markers;
            this.coordinatorEpoch = coordinatorEpoch;
        }

        @Override
        public WriteTxnMarkerRequest build(short version) {
            return new WriteTxnMarkerRequest(version, coordinatorEpoch, markers);
        }
    }

    private final int coordinatorEpoch;
    private final List<TxnMarkerEntry> markers;

    private WriteTxnMarkerRequest(short version, int coordinatorEpoch, List<TxnMarkerEntry> markers) {
        super(version);

        this.markers = markers;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    public WriteTxnMarkerRequest(Struct struct, short version) {
        super(version);
        this.coordinatorEpoch = struct.getInt(COORDINATOR_EPOCH_KEY_NAME);

        List<TxnMarkerEntry> markers = new ArrayList<>();
        Object[] markersArray = struct.getArray(TXN_MARKER_ENTRY_KEY_NAME);
        for (Object markerObj : markersArray) {
            Struct markerStruct = (Struct) markerObj;

            long pid = markerStruct.getLong(PID_KEY_NAME);
            short epoch = markerStruct.getShort(EPOCH_KEY_NAME);
            TransactionResult result = TransactionResult.forId(markerStruct.getByte(TRANSACTION_RESULT_KEY_NAME));

            List<TopicPartition> partitions = new ArrayList<>();
            Object[] topicPartitionsArray = markerStruct.getArray(TOPIC_PARTITIONS_KEY_NAME);
            for (Object topicPartitionObj : topicPartitionsArray) {
                Struct topicPartitionStruct = (Struct) topicPartitionObj;
                String topic = topicPartitionStruct.getString(TOPIC_KEY_NAME);
                for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                    partitions.add(new TopicPartition(topic, (Integer) partitionObj));
                }
            }

            markers.add(new TxnMarkerEntry(pid, epoch, result, partitions));
        }

        this.markers = markers;
    }

    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.WRITE_TXN_MARKER.requestSchema(version()));
        struct.set(COORDINATOR_EPOCH_KEY_NAME, coordinatorEpoch);


        Object[] markersArray = new Object[markers.size()];
        int i = 0;
        for (Map.Entry<String, List<Integer>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, topicAndPartitions.getValue().toArray());
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(PID_KEY_NAME, pid);
        struct.set(EPOCH_KEY_NAME, epoch);
        struct.set(TRANSACTION_RESULT_KEY_NAME, result.id);

        Map<String, List<Integer>> mappedPartitions = CollectionUtils.groupDataByTopic(partitions);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, List<Integer>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPIC_PARTITIONS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_KEY_NAME, topicAndPartitions.getKey());
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, topicAndPartitions.getValue().toArray());
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPIC_PARTITIONS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public WriteTxnMarkerResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        Map<TopicPartition, Errors> errors = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions)
            errors.put(partition, error);
        return new WriteTxnMarkerResponse(errors);
    }

    public static WriteTxnMarkerRequest parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkerRequest(ApiKeys.WRITE_TXN_MARKER.parseRequest(version, buffer), version);
    }

}
