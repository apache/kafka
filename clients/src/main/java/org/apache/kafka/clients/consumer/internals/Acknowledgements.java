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
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class maintains the acknowledgement and gap information for a set of records on a single
 * topic-partition being delivered to a consumer in a share group.
 */
public class Acknowledgements {
    public static final byte ACKNOWLEDGE_TYPE_GAP = (byte) 0;

    // The acknowledgements keyed by offset. If the record is a gap, the AcknowledgeType will be null.
    private final Map<Long, AcknowledgeType> acknowledgements;

    // When the broker responds to the acknowledgements, this is the error code returned.
    private Errors acknowledgeErrorCode;

    public static Acknowledgements empty() {
        return new Acknowledgements(new TreeMap<>());
    }

    private Acknowledgements(Map<Long, AcknowledgeType> acknowledgements) {
        this.acknowledgements = acknowledgements;
    }

    /**
     * Adds an acknowledgement for a specific offset. Will overwrite an existing
     * acknowledgement for the same offset.
     *
     * @param offset The record offset.
     * @param type   The AcknowledgeType.
     */
    public void add(long offset, AcknowledgeType type) {
        this.acknowledgements.put(offset, type);
    }

    /**
     * Adds an acknowledgement for a specific offset. Will <b>not</b> overwrite an existing
     * acknowledgement for the same offset.
     *
     * @param offset The record offset.
     * @param type   The AcknowledgeType.
     */
    public void addIfAbsent(long offset, AcknowledgeType type) {
        acknowledgements.putIfAbsent(offset, type);
    }

    /**
     * Adds a gap for the specified offset. This means the broker expected a record at this offset
     * but when the record batch was parsed, the desired offset was missing. This occurs when the
     * record has been removed by the log compactor.
     *
     * @param offset The record offset.
     */
    public void addGap(long offset) {
        acknowledgements.put(offset, null);
    }

    /**
     * Gets the acknowledgement type for an offset.
     *
     * @param offset The record offset.
     * @return The AcknowledgeType, or null if no acknowledgement is present.
     */
    public AcknowledgeType get(long offset) {
        return acknowledgements.get(offset);
    }

    /**
     * Whether the set of acknowledgements is empty.
     *
     * @return Whether the set of acknowledgements is empty.
     */
    public boolean isEmpty() {
        return acknowledgements.isEmpty();
    }

    /**
     * Returns the size of the set of acknowledgements.
     *
     * @return The size of the set of acknowledgements.
     */
    public int size() {
        return acknowledgements.size();
    }

    /**
     * Whether the acknowledgements were sent to the broker and a response received.
     *
     * @return Whether the acknowledgements were sent to the broker and a response received
     */
    public boolean isCompleted() {
        return acknowledgeErrorCode != null;
    }

    /**
     * Set the acknowledgement error code when the response has been received from the broker.
     *
     * @param acknowledgeErrorCode the error code
     */
    public void setAcknowledgeErrorCode(Errors acknowledgeErrorCode) {
        this.acknowledgeErrorCode = acknowledgeErrorCode;
    }

    /**
     * Get the acknowledgement error code when the response has been received from the broker.
     *
     * @return the error code
     */
    public Errors getAcknowledgeErrorCode() {
        return acknowledgeErrorCode;
    }

    /**
     * Merges two sets of acknowledgements. If there are overlapping acknowledgements, the
     * merged set wins.
     *
     * @param other The set of acknowledgement to merge.
     */
    public Acknowledgements merge(Acknowledgements other) {
        acknowledgements.putAll(other.acknowledgements);
        return this;
    }

    /**
     * Returns the Map of Acknowledgements for the offsets.
     */
    public Map<Long, AcknowledgeType> getAcknowledgementsTypeMap() {
        return acknowledgements;
    }

    public List<ShareFetchRequestData.AcknowledgementBatch> getAcknowledgmentBatches() {
        List<ShareFetchRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (acknowledgements.isEmpty()) return batches;
        ShareFetchRequestData.AcknowledgementBatch currentBatch = null;
        Iterator<Map.Entry<Long, AcknowledgeType>> iterator = acknowledgements.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, AcknowledgeType> entry = iterator.next();
            if (currentBatch == null) {
                currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                currentBatch.setFirstOffset(entry.getKey());
                currentBatch.setLastOffset(entry.getKey());
                if (entry.getValue() != null) {
                    currentBatch.setAcknowledgeType(entry.getValue().id);
                    currentBatch.acknowledgeTypes().add(entry.getValue().id);
                } else {
                    // Put a marker value of -1 while this batch only has gaps
                    currentBatch.setAcknowledgeType((byte) -1);
                    currentBatch.gapOffsets().add(entry.getKey());
                    currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                }
            } else {
                if (entry.getValue() != null) {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        if (entry.getValue().id == currentBatch.acknowledgeType()) {
                            currentBatch.setLastOffset(entry.getKey());
                            currentBatch.acknowledgeTypes().add(entry.getValue().id);
                        } else if (currentBatch.acknowledgeType() == (byte) -1) {
                            currentBatch.setAcknowledgeType(entry.getValue().id);
                            currentBatch.setLastOffset(entry.getKey());
                            currentBatch.acknowledgeTypes().add(entry.getValue().id);
                        } else {
                            if (currentBatch.acknowledgeType() == -1) {
                                // Even though this batch is just gaps, we mark it as ACCEPT.
                                currentBatch.setAcknowledgeType(AcknowledgeType.ACCEPT.id);
                            }
                            batches.add(currentBatch);

                            currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                            currentBatch.setFirstOffset(entry.getKey());
                            currentBatch.setLastOffset(entry.getKey());
                            currentBatch.setAcknowledgeType(entry.getValue().id);
                            currentBatch.acknowledgeTypes().add(entry.getValue().id);
                        }
                    } else {
                        if (currentBatch.acknowledgeType() == -1) {
                            // Even though this batch is just gaps, we mark it as ACCEPT.
                            currentBatch.setAcknowledgeType(AcknowledgeType.ACCEPT.id);
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.setAcknowledgeType(entry.getValue().id);
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    }
                } else {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.gapOffsets().add(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    } else {
                        if (currentBatch.acknowledgeType() == -1) {
                            // Even though this batch is just gaps, we mark it as ACCEPT.
                            currentBatch.setAcknowledgeType(AcknowledgeType.ACCEPT.id);
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        // Put a marker value of -1 while this batch only has gaps
                        currentBatch.setAcknowledgeType((byte) -1);
                        currentBatch.gapOffsets().add(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    }
                }
            }
        }
        if (currentBatch.acknowledgeType() == -1) {
            // Even though this batch is just gaps, we mark it as ACCEPT.
            currentBatch.setAcknowledgeType(AcknowledgeType.ACCEPT.id);
        }
        batches.add(currentBatch);
        return batches;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Acknowledgements(");
        sb.append(acknowledgements);
        if (acknowledgeErrorCode != null) {
            sb.append(", errorCode=");
            sb.append(acknowledgeErrorCode.code());
        }
        sb.append(")");
        return sb.toString();
    }
}
