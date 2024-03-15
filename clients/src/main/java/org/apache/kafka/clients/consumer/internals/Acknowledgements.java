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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class maintains the acknowledgement and gap information for a set of records on a single
 * topic-partition being delivered to a consumer in a share group.
 */
public class Acknowledgements {
    // The acknowledgements keyed by offset. If the record is a gap, the AcknowledgeType will be null.
    private final Map<Long, AcknowledgeType> acknowledgements;

    // When the broker responds to the acknowledgements, this is the error code returned.
    private Errors acknowledgeErrorCode;

    public static Acknowledgements empty() {
        return new Acknowledgements(new LinkedHashMap<>());
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

    public List<ShareFetchRequestData.AcknowledgementBatch> getAcknowledgmentBatches() {
        List<ShareFetchRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        ShareFetchRequestData.AcknowledgementBatch currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
        if (acknowledgements.isEmpty()) return batches;
        acknowledgements.forEach((offset, acknowledgeType) -> {
            if (batches.isEmpty()) {
                currentBatch.setBaseOffset(offset);
                currentBatch.setAcknowledgeType(acknowledgeType.id);
                currentBatch.setLastOffset(offset);
            } else if (acknowledgeType.id == currentBatch.acknowledgeType()) {
                currentBatch.setLastOffset(offset);
            } else {
                batches.add(currentBatch);
                currentBatch.setBaseOffset(offset);
                currentBatch.setAcknowledgeType(acknowledgeType.id);
                currentBatch.setLastOffset(offset);
            }
        });
        batches.add(currentBatch);
        return batches;
    }
}
