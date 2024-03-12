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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.List;

public class ShareInFlightBatch<K, V> {
    final TopicIdPartition partition;
    private final Acknowledgements acknowledgements;
    private final List<ConsumerRecord<K, V>> inFlightRecords;
    private KafkaException exception;

    public ShareInFlightBatch(TopicIdPartition partition) {
        this.partition = partition;
        acknowledgements = Acknowledgements.empty();
        inFlightRecords = new ArrayList<>();
    }

    public void addAcknowledgement(long offset, AcknowledgeType acknowledgeType) {
        acknowledgements.add(offset, acknowledgeType);
    }

    public void acknowledgeAll(AcknowledgeType type) {
        inFlightRecords.forEach(record -> {
            acknowledgements.addIfAbsent(record.offset(), type);
        });
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        inFlightRecords.add(record);
    }

    public void merge(ShareInFlightBatch<K, V> other) {
        inFlightRecords.addAll(other.inFlightRecords);
        acknowledgements.merge(other.acknowledgements);
    }

    List<ConsumerRecord<K, V>> getInFlightRecords() {
        return inFlightRecords;
    }

    Acknowledgements getAcknowledgements() {
        return acknowledgements;
    }

    public boolean isEmpty() {
        return inFlightRecords.isEmpty() && acknowledgements.isEmpty();
    }

    public void setException(KafkaException exception) {
        this.exception = exception;
    }

    public KafkaException getException() {
        return exception;
    }
}
