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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A simple interface to write records to Partitions/Logs. It contains the minimum
 * required for coordinators.
 *
 * @param <T> The record type.
 */
public interface PartitionWriter<T> {

    /**
     * Serializer to translate T to bytes.
     *
     * @param <T> The record type.
     */
    interface Serializer<T> {
        /**
         * Serializes the key of the record.
         */
        byte[] serializeKey(T record);

        /**
         * Serializes the value of the record.
         */
        byte[] serializeValue(T record);
    }

    /**
     * Listener allowing to listen to high watermark changes. This is meant
     * to be used in conjunction with {{@link PartitionWriter#append(TopicPartition, List)}}.
     */
    interface Listener {
        void onHighWatermarkUpdated(
            TopicPartition tp,
            long offset
        );
    }

    /**
     * Register a {{@link Listener}}.
     *
     * @param tp        The partition to register the listener to.
     * @param listener  The listener.
     */
    void registerListener(
        TopicPartition tp,
        Listener listener
    );

    /**
     * Deregister a {{@link Listener}}.
     *
     * @param tp        The partition to deregister the listener from.
     * @param listener  The listener.
     */
    void deregisterListener(
        TopicPartition tp,
        Listener listener
    );

    /**
     * Write records to the partitions. Records are written in one batch so
     * atomicity is guaranteed.
     *
     * @param tp                The partition to write records to.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param verificationGuard The verification guard.
     * @param records           The list of records. The records are written in a single batch.
     * @return The log end offset right after the written records.
     * @throws KafkaException Any KafkaException caught during the write operation.
     */
    long append(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        VerificationGuard verificationGuard,
        List<T> records
    ) throws KafkaException;

    /**
     * Write the transaction end marker.
     *
     * @param tp                The partition to write records to.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param coordinatorEpoch  The epoch of the transaction coordinator.
     * @param result            The transaction result.
     * @return The log end offset right after the written records.
     * @throws KafkaException Any KafkaException caught during the write operation.
     */
    long appendEndTransactionMarker(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        TransactionResult result
    ) throws KafkaException;

    /**
     * Verify the transaction.
     *
     * @param tp                The partition to write records to.
     * @param transactionalId   The transactional id.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @return A future failed with any error encountered; or the {@link VerificationGuard}
     *         if the transaction required verification and {@link VerificationGuard#SENTINEL}
     *         if it did not.
     * @throws KafkaException Any KafkaException caught during the operation.
     */
    CompletableFuture<VerificationGuard> maybeStartTransactionVerification(
        TopicPartition tp,
        String transactionalId,
        long producerId,
        short producerEpoch
    ) throws KafkaException;
}
