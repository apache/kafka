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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import java.util.concurrent.CompletableFuture;

/**
 * A simple interface to write records to Partitions/Logs. It contains the minimum
 * required for coordinators.
 */
public interface PartitionWriter {

    /**
     * Listener allowing to listen to high watermark changes. This is meant
     * to be used in conjunction with {@link PartitionWriter#append(TopicPartition, VerificationGuard, MemoryRecords, short)}.
     * <p>
     * A registered listener observes a single leadership tenure of the partition. It is
     * delivered high watermark updates only while this broker is the leader of the
     * partition. Once the partition is no longer led by this broker (it transitions to
     * follower, is deleted, or fails), the listener is permanently retired: no further
     * updates are delivered to it, even if the broker later regains leadership. A new
     * listener must be registered to observe a subsequent tenure.
     * <p>
     * Retiring the listener is required because, after a leadership change, the local log
     * can be truncated and re-replicated from the new leader, so a high watermark observed
     * afterwards may advance over records that this broker never wrote. This guarantees
     * that every delivered update advances only over records that this broker wrote as
     * leader.
     */
    interface Listener {
        /**
         * Called when the high watermark of the partition advances. Only invoked while
         * this broker is the leader of the partition (see {@link Listener}).
         *
         * @param tp        The topic partition.
         * @param offset    The new high watermark.
         */
        void onHighWatermarkUpdated(
            TopicPartition tp,
            long offset
        );
    }

    /**
     * Register a {@link Listener}.
     * <p>
     * The listener observes only the current leadership tenure: as described on
     * {@link Listener}, it stops receiving updates once the partition is no longer led by
     * this broker and is not re-armed if leadership is regained. A new listener must be
     * registered to observe a later tenure.
     *
     * @param tp        The partition to register the listener to.
     * @param listener  The listener.
     */
    void registerListener(
        TopicPartition tp,
        Listener listener
    );

    /**
     * Deregister a {@link Listener}.
     *
     * @param tp        The partition to deregister the listener from.
     * @param listener  The listener.
     */
    void deregisterListener(
        TopicPartition tp,
        Listener listener
    );

    /**
     * Return the LogConfig of the partition.
     *
     * @param tp    The partition.
     * @return The LogConfig.
     */
    LogConfig config(
        TopicPartition tp
    );

    /**
     * Write records to the partitions.
     *
     * @param tp                The partition to write records to.
     * @param verificationGuard The verification guard.
     * @param records           The MemoryRecords.
     * @param transactionVersion  The transaction version (1 = TV1, 2 = TV2 etc.).
     *                            Use TV_UNKNOWN (-1) for non-transaction writes.
     * @return The log end offset right after the written records.
     */
    long append(
        TopicPartition tp,
        VerificationGuard verificationGuard,
        MemoryRecords records,
        short transactionVersion
    ) throws KafkaException;

    /**
     * Verify the transaction.
     *
     * @param tp                The partition to write records to.
     * @param transactionalId   The transactional id.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param apiVersion        The version of the Request used.
     * @return A future failed with any error encountered; or the {@link VerificationGuard}
     *         if the transaction required verification and {@link VerificationGuard#SENTINEL}
     *         if it did not.
     * @throws KafkaException Any KafkaException caught during the operation.
     */
    CompletableFuture<VerificationGuard> maybeStartTransactionVerification(
        TopicPartition tp,
        String transactionalId,
        long producerId,
        short producerEpoch,
        int apiVersion
    ) throws KafkaException;

    /**
     * Delete records from a topic partition until specified offset
     * @param tp                    The partition to delete records from
     * @param deleteBeforeOffset    Offset to delete until, starting from the beginning
     * @throws KafkaException       Any KafkaException caught during the operation.
     */
    CompletableFuture<Void> deleteRecords(
        TopicPartition tp,
        long deleteBeforeOffset
    ) throws KafkaException;
}
