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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.requests.TransactionResult;

/**
 * The CoordinatorPlayback interface. This interface is used to replay
 * records to the coordinator in order to update its state. This is
 * also used to rebuild a coordinator state from scratch by replaying
 * all records stored in the partition.
 *
 * @param <U> The type of the record.
 */
public interface CoordinatorPlayback<U> {
    /**
     * Applies the given record to this object.
     *
     * @param offset        The offset of the record in the log.
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param record        A record.
     * @throws RuntimeException if the record cannot be applied.
     */
    void replay(
        long offset,
        long producerId,
        short producerEpoch,
        U record
    ) throws RuntimeException;

    /**
     * Applies the given transaction marker.
     *
     * @param producerId    The producer id.
     * @param producerEpoch The producer epoch.
     * @param result        The result of the transaction.
     * @throws RuntimeException if the transaction cannot be completed.
     */
    void replayEndTransactionMarker(
        long producerId,
        short producerEpoch,
        TransactionResult result
    ) throws RuntimeException;

    /**
     * Called when records have been written to the log up to the given offset. The loader calls
     * it after replaying a batch that reaches the high watermark, or that ends a commit interval
     * after the last committed offset. The runtime calls it after it has appended a batch of
     * records or an end transaction marker to the log.
     *
     * @param offset The offset of the last written record plus one.
     */
    void updateLastWrittenOffset(long offset);

    /**
     * Called when the high watermark advances.
     *
     * @param offset The offset of the new high watermark.
     */
    void updateLastCommittedOffset(long offset);
}
