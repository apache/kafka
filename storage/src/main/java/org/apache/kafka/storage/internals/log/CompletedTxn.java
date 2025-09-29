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
package org.apache.kafka.storage.internals.log;

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 */
public class CompletedTxn {
    public final long producerId;
    public final long firstOffset;
    public final long lastOffset;
    public final boolean isAborted;

    /**
     * Create an instance of this class.
     *
     * @param producerId  The ID of the producer
     * @param firstOffset The first offset (inclusive) of the transaction
     * @param lastOffset  The last offset (inclusive) of the transaction. This is always the offset of the
     *                    COMMIT/ABORT control record which indicates the transaction's completion.
     * @param isAborted   Whether the transaction was aborted
     */
    public CompletedTxn(long producerId, long firstOffset, long lastOffset, boolean isAborted) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.isAborted = isAborted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompletedTxn that = (CompletedTxn) o;

        return producerId == that.producerId
            && firstOffset == that.firstOffset
            && lastOffset == that.lastOffset
            && isAborted == that.isAborted;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(producerId);
        result = 31 * result + Long.hashCode(firstOffset);
        result = 31 * result + Long.hashCode(lastOffset);
        result = 31 * result + Boolean.hashCode(isAborted);
        return result;
    }

    @Override
    public String toString() {
        return "CompletedTxn(producerId=" + producerId +
            ", firstOffset=" + firstOffset +
            ", lastOffset=" + lastOffset +
            ", isAborted=" + isAborted +
            ')';
    }
}
