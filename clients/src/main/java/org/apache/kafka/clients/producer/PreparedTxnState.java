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
 * Class containing the owner fence of a transaction after it has been prepared for a two-phase commit.
 */
public class PreparedTxnState {
    private final long txnOwnerId;
    private final short txnOwnerEpoch;

    /**
     * Creates a new empty PreparedTxnState
     */
    public PreparedTxnState() {
        this.txnOwnerId = RecordBatch.NO_PRODUCER_ID;
        this.txnOwnerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
    }

    /**
     * Creates a new PreparedTxnState from a serialized string representation
     *
     * @param serializedState               The serialized string to deserialize.
     * @throws IllegalArgumentException if the serialized string is not in the expected format
     */
    public PreparedTxnState(String serializedState) {
        if (serializedState == null || serializedState.isEmpty()) {
            this.txnOwnerId = RecordBatch.NO_PRODUCER_ID;
            this.txnOwnerEpoch = RecordBatch.NO_PRODUCER_EPOCH;
            return;
        }

        try {
            String[] parts = serializedState.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid serialized transaction state format: " + serializedState);
            }

            this.txnOwnerId = Long.parseLong(parts[0]);
            this.txnOwnerEpoch = Short.parseShort(parts[1]);

            if (!(this.txnOwnerId >= 0 && this.txnOwnerEpoch >= 0)) {
                throw new IllegalArgumentException("Invalid transaction owner ID and epoch values: " +
                    txnOwnerId + ":" + txnOwnerEpoch + ". Both must be >= 0");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid serialized transaction state format: " + serializedState, e);
        }
    }

    /**
     * Creates a new PreparedTxnState with the given transaction owner ID and epoch
     *
     * @param txnOwnerId        The transaction owner ID
     * @param txnOwnerEpoch     The transaction owner epoch
     */
    PreparedTxnState(long txnOwnerId, short txnOwnerEpoch) {
        this.txnOwnerId = txnOwnerId;
        this.txnOwnerEpoch = txnOwnerEpoch;
    }

    /**
     * Gets the transaction owner ID associated with this prepared transaction state.
     *
     * @return The transaction owner ID
     */
    public long txnOwnerId() {
        return txnOwnerId;
    }

    /**
     * Gets the transaction owner epoch associated with this prepared transaction state.
     *
     * @return The transaction owner epoch
     */
    public short txnOwnerEpoch() {
        return txnOwnerEpoch;
    }

    public long producerId() {
        return txnOwnerId;
    }

    public short epoch() {
        return txnOwnerEpoch;
    }

    /**
     * Checks if this preparedTxnState represents an initialized transaction with a valid owner ID
     * that is not -1 (the uninitialized value).
     *
     * @return true if the state has an initialized transaction, false otherwise.
     */
    public boolean hasTransaction() {
        return txnOwnerId != RecordBatch.NO_PRODUCER_ID;
    }

    /**
     * Returns a serialized string representation of this transaction state.
     * The format is "txnOwnerId:txnOwnerEpoch" for an initialized state, or an empty string
     * for an uninitialized state.
     *
     * @return a serialized string representation
     */
    @Override
    public String toString() {
        if (!hasTransaction()) {
            return "";
        }
        return txnOwnerId + ":" + txnOwnerEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparedTxnState that = (PreparedTxnState) o;
        return txnOwnerId == that.txnOwnerId && txnOwnerEpoch == that.txnOwnerEpoch;
    }

    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + Long.hashCode(txnOwnerId);
        result = 31 * result + (int) txnOwnerEpoch;
        return result;
    }
}
