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
import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ShareInFlightBatch<K, V> {
    private final int nodeId;
    private final TopicIdPartition partition;
    private final Map<Long, ConsumerRecord<K, V>> inFlightRecords;
    private Map<Long, ConsumerRecord<K, V>> renewingRecords;
    private Map<Long, ConsumerRecord<K, V>> renewedRecords;
    private final Set<Long> acknowledgedRecords;
    private Acknowledgements acknowledgements;
    private ShareInFlightBatchException exception;
    private boolean hasCachedException = false;
    private boolean checkForRenewAcknowledgements = false;

    public ShareInFlightBatch(int nodeId, TopicIdPartition partition) {
        this.nodeId = nodeId;
        this.partition = partition;
        this.inFlightRecords = new TreeMap<>();
        this.acknowledgedRecords = new TreeSet<>();
        this.acknowledgements = Acknowledgements.empty();
    }

    public void addAcknowledgement(long offset, AcknowledgeType type) {
        acknowledgements.add(offset, type);
        if (type == AcknowledgeType.RENEW) {
            checkForRenewAcknowledgements = true;
        }
    }

    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        if (inFlightRecords.get(record.offset()) != null) {
            acknowledgements.add(record.offset(), type);
            acknowledgedRecords.add(record.offset());
            if (type == AcknowledgeType.RENEW) {
                checkForRenewAcknowledgements = true;
            }
            return;
        }
        throw new IllegalStateException("The record cannot be acknowledged.");
    }

    public void acknowledgeAll(AcknowledgeType type) {
        for (Map.Entry<Long, ConsumerRecord<K, V>> entry : inFlightRecords.entrySet()) {
            if (acknowledgements.addIfAbsent(entry.getKey(), type)) {
                acknowledgedRecords.add(entry.getKey());
            }
        }
        if (type == AcknowledgeType.RENEW) {
            checkForRenewAcknowledgements = true;
        }
    }

    public boolean checkAllInFlightAreAcknowledged() {
        return inFlightRecords.size() == acknowledgedRecords.size();
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        inFlightRecords.put(record.offset(), record);
    }

    public void addGap(long offset) {
        acknowledgements.addGap(offset);
    }

    public void merge(ShareInFlightBatch<K, V> other) {
        inFlightRecords.putAll(other.inFlightRecords);
        if (other.checkForRenewAcknowledgements) {
            checkForRenewAcknowledgements = true;
        }
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
        if (checkForRenewAcknowledgements) {
            if (renewingRecords == null) {
                renewingRecords = new HashMap<>();
            }
            if (renewedRecords == null) {
                renewedRecords = new HashMap<>();
            }
            Map<Long, AcknowledgeType> ackTypeMap = acknowledgements.getAcknowledgementsTypeMap();
            acknowledgedRecords.forEach(offset -> {
                if (ackTypeMap.get(offset) == AcknowledgeType.RENEW) {
                    renewingRecords.put(offset, inFlightRecords.get(offset));
                }
            });
        }

        // Usually, all records will be acknowledged, so we can just clear the in-flight records leaving
        // an empty batch, which will trigger more fetching
        if (acknowledgedRecords.size() == inFlightRecords.size()) {
            inFlightRecords.clear();
        } else {
            acknowledgedRecords.forEach(inFlightRecords::remove);
        }
        acknowledgedRecords.clear();
        exception = null;

        Acknowledgements currentAcknowledgements = acknowledgements;
        acknowledgements = Acknowledgements.empty();
        checkForRenewAcknowledgements = false;
        return currentAcknowledgements;
    }

    int renew(Acknowledgements acknowledgements) {
        int recordsRenewed = 0;
        boolean isCompletedExceptionally = acknowledgements.isCompletedExceptionally();
        if (acknowledgements.isCompleted()) {
            Map<Long, AcknowledgeType> ackTypeMap = acknowledgements.getAcknowledgementsTypeMap();
            for (Map.Entry<Long, AcknowledgeType> ackTypeEntry : ackTypeMap.entrySet()) {
                long offset = ackTypeEntry.getKey();
                AcknowledgeType ackType = ackTypeEntry.getValue();
                ConsumerRecord<K, V> record = renewingRecords.remove(offset);
                if (ackType == AcknowledgeType.RENEW) {
                    if (record != null && !isCompletedExceptionally) {
                        // The record is moved into renewed state, and will then become in-flight later.
                        renewedRecords.put(offset, record);
                        recordsRenewed++;
                    }
                }
            }
        } else {
            throw new IllegalStateException("Renewing with uncompleted acknowledgements");
        }
        return recordsRenewed;
    }

    boolean hasRenewals() {
        if (renewingRecords == null) {
            return false;
        }
        return !renewingRecords.isEmpty() || !renewedRecords.isEmpty();
    }

    void takeRenewals() {
        if (renewedRecords != null) {
            inFlightRecords.putAll(renewedRecords);
            renewedRecords.clear();
        }
    }

    Acknowledgements getAcknowledgements() {
        return acknowledgements;
    }

    public boolean isEmpty() {
        return inFlightRecords.isEmpty() && acknowledgements.isEmpty();
    }

    public void setException(ShareInFlightBatchException exception) {
        this.exception = exception;
    }

    public ShareInFlightBatchException getException() {
        return exception;
    }

    public void setHasCachedException(boolean hasCachedException) {
        this.hasCachedException = hasCachedException;
    }

    public boolean hasCachedException() {
        return hasCachedException;
    }
}
