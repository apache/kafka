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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ShareInFlightBatch<K, V> {
    private final int nodeId;
    final TopicIdPartition partition;
    private final Map<Long, ConsumerRecord<K, V>> inFlightRecords;
    private final Set<Long> acknowledgedRecords;
    private Acknowledgements acknowledgements;
    private KafkaException exception;
    private boolean hasCachedException = false;

    public ShareInFlightBatch(int nodeId, TopicIdPartition partition) {
        this.nodeId = nodeId;
        this.partition = partition;
        inFlightRecords = new TreeMap<>();
        acknowledgedRecords = new TreeSet<>();
        acknowledgements = Acknowledgements.empty();
    }

    public void addAcknowledgement(long offset, AcknowledgeType acknowledgeType) {
        acknowledgements.add(offset, acknowledgeType);
    }

    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        if (inFlightRecords.get(record.offset()) != null) {
            acknowledgements.add(record.offset(), type);
            acknowledgedRecords.add(record.offset());
            return;
        }
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    public int acknowledgeAll(AcknowledgeType type) {
        int recordsAcknowledged = 0;
        for (Map.Entry<Long, ConsumerRecord<K, V>> entry : inFlightRecords.entrySet()) {
            if (acknowledgements.addIfAbsent(entry.getKey(), type)) {
                acknowledgedRecords.add(entry.getKey());
                recordsAcknowledged++;
            }
        }
        return recordsAcknowledged;
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        inFlightRecords.put(record.offset(), record);
    }

    public void addGap(long offset) {
        acknowledgements.addGap(offset);
    }

    public void merge(ShareInFlightBatch<K, V> other) {
        inFlightRecords.putAll(other.inFlightRecords);
    }

    List<ConsumerRecord<K, V>> getInFlightRecords() {
        return new ArrayList<>(inFlightRecords.values());
    }

    int numRecords() {
        return inFlightRecords.size();
    }

    int nodeId() {
        return nodeId;
    }

    Acknowledgements takeAcknowledgedRecords() {
        // Usually, all records will be acknowledged, so we can just clear the in-flight records leaving
        // an empty batch, which will trigger more fetching
        if (acknowledgedRecords.size() == inFlightRecords.size()) {
            inFlightRecords.clear();
        } else {
            acknowledgedRecords.forEach(inFlightRecords::remove);
        }
        acknowledgedRecords.clear();

        Acknowledgements currentAcknowledgements = acknowledgements;
        acknowledgements = Acknowledgements.empty();
        return currentAcknowledgements;
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

    public void setHasCachedException(boolean hasCachedException) {
        this.hasCachedException = hasCachedException;
    }

    public boolean hasCachedException() {
        return hasCachedException;
    }
}
