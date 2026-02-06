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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.record.internal.RecordBatch;

/**
 * Class containing the state of a transaction after it has been prepared for a two-phase commit.
 * This state includes the producer ID and epoch, which are needed to commit or abort the transaction.
 */
public class PreparedTxnState {
    private final long producerId;
    private final short epoch;

    /**
     * Creates a new empty PreparedTxnState
     */
    public PreparedTxnState() {
        this.producerId = RecordBatch.NO_PRODUCER_ID;
        this.epoch = RecordBatch.NO_PRODUCER_EPOCH;
    }

    /**
     * Creates a new PreparedTxnState from a serialized string representation
     *
     * @param serializedState               The serialized string to deserialize.
     * @throws IllegalArgumentException if the serialized string is not in the expected format
     */
    public PreparedTxnState(String serializedState) {
        if (serializedState == null || serializedState.isEmpty()) {
            this.producerId = RecordBatch.NO_PRODUCER_ID;
            this.epoch = RecordBatch.NO_PRODUCER_EPOCH;
            return;
        }

        try {
            String[] parts = serializedState.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid serialized transaction state format: " + serializedState);
            }

            this.producerId = Long.parseLong(parts[0]);
            this.epoch = Short.parseShort(parts[1]);

            // Validate the producerId and epoch values.
            if (!(this.producerId >= 0 && this.epoch >= 0)) {
                throw new IllegalArgumentException("Invalid producer ID and epoch values: " +
                    producerId + ":" + epoch + ". Both must be >= 0");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid serialized transaction state format: " + serializedState, e);
        }
    }

    /**
     * Creates a new PreparedTxnState with the given producer ID and epoch
     *
     * @param producerId        The producer ID
     * @param epoch             The producer epoch
     */
    PreparedTxnState(long producerId, short epoch) {
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public long producerId() {
        return producerId;
    }

    public short epoch() {
        return epoch;
    }

    /**
     * Checks if this preparedTxnState represents an initialized transaction with a valid producer ID
     * that is not -1 (the uninitialized value).
     *
     * @return true if the state has an initialized transaction, false otherwise.
     */
    public boolean hasTransaction() {
        return producerId != RecordBatch.NO_PRODUCER_ID;
    }

    /**
     * Returns a serialized string representation of this transaction state.
     * The format is "producerId:epoch" for an initialized state, or an empty string
     * for an uninitialized state (where producerId and epoch are both -1).
     *
     * @return a serialized string representation
     */
    @Override
    public String toString() {
        if (!hasTransaction()) {
            return "";
        }
        return producerId + ":" + epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparedTxnState that = (PreparedTxnState) o;
        return producerId == that.producerId && epoch == that.epoch;
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + Long.hashCode(producerId);
        result = 31 * result + (int) epoch;
        return result;
    }
}
