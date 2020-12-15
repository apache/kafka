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
import org.apache.kafka.common.message.WriteTxnMarkersRequestData;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarker;
import org.apache.kafka.common.message.WriteTxnMarkersRequestData.WritableTxnMarkerTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class WriteTxnMarkersRequest extends AbstractRequest {

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

        public final WriteTxnMarkersRequestData data;

        public Builder(short version, final List<TxnMarkerEntry> markers) {
            super(ApiKeys.WRITE_TXN_MARKERS, version);
            List<WritableTxnMarker> dataMarkers = new ArrayList<>();
            for (TxnMarkerEntry marker : markers) {
                final Map<String, WritableTxnMarkerTopic> topicMap = new HashMap<>();
                for (TopicPartition topicPartition : marker.partitions) {
                    WritableTxnMarkerTopic topic = topicMap.getOrDefault(topicPartition.topic(),
                                                                         new WritableTxnMarkerTopic()
                                                                             .setName(topicPartition.topic()));
                    topic.partitionIndexes().add(topicPartition.partition());
                    topicMap.put(topicPartition.topic(), topic);
                }

                dataMarkers.add(new WritableTxnMarker()
                                    .setProducerId(marker.producerId)
                                    .setProducerEpoch(marker.producerEpoch)
                                    .setCoordinatorEpoch(marker.coordinatorEpoch)
                                    .setTransactionResult(marker.transactionResult().id)
                                    .setTopics(new ArrayList<>(topicMap.values())));
            }
            this.data = new WriteTxnMarkersRequestData().setMarkers(dataMarkers);
        }

        @Override
        public WriteTxnMarkersRequest build(short version) {
            return new WriteTxnMarkersRequest(data, version);
        }
    }

    private final WriteTxnMarkersRequestData data;

    private WriteTxnMarkersRequest(WriteTxnMarkersRequestData data, short version) {
        super(ApiKeys.WRITE_TXN_MARKERS, version);
        this.data = data;
    }

    @Override
    public WriteTxnMarkersRequestData data() {
        return data;
    }

    @Override
    public WriteTxnMarkersResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);

        final Map<Long, Map<TopicPartition, Errors>> errors = new HashMap<>(data.markers().size());
        for (WritableTxnMarker markerEntry : data.markers()) {
            Map<TopicPartition, Errors> errorsPerPartition = new HashMap<>();
            for (WritableTxnMarkerTopic topic : markerEntry.topics()) {
                for (Integer partitionIdx : topic.partitionIndexes()) {
                    errorsPerPartition.put(new TopicPartition(topic.name(), partitionIdx), error);
                }
            }
            errors.put(markerEntry.producerId(), errorsPerPartition);
        }

        return new WriteTxnMarkersResponse(errors);
    }

    public List<TxnMarkerEntry> markers() {
        List<TxnMarkerEntry> markers = new ArrayList<>();
        for (WritableTxnMarker markerEntry : data.markers()) {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (WritableTxnMarkerTopic topic : markerEntry.topics()) {
                for (Integer partitionIdx : topic.partitionIndexes()) {
                    topicPartitions.add(new TopicPartition(topic.name(), partitionIdx));
                }
            }
            markers.add(new TxnMarkerEntry(
                markerEntry.producerId(),
                markerEntry.producerEpoch(),
                markerEntry.coordinatorEpoch(),
                TransactionResult.forId(markerEntry.transactionResult()),
                topicPartitions)
            );
        }
        return markers;
    }

    public static WriteTxnMarkersRequest parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkersRequest(new WriteTxnMarkersRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final WriteTxnMarkersRequest that = (WriteTxnMarkersRequest) o;
        return Objects.equals(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data);
    }
}
