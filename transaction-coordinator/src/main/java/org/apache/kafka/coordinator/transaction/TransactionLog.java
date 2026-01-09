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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.server.common.TransactionVersion;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Messages stored for the transaction topic represent the producer id and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [producer_id, producer_epoch, expire_timestamp, status, [topic, [partition] ], timestamp]
 */
public class TransactionLog {

    // enforce always using
    //  1. cleanup policy = compact
    //  2. compression = none
    //  3. unclean leader election = disabled
    //  4. required acks = -1 when writing
    public static final Compression ENFORCED_COMPRESSION = Compression.NONE;
    public static final short ENFORCED_REQUIRED_ACKS = (short) -1;

    /**
     * Generates the bytes for transaction log message key
     *
     * @return key bytes
     */
    public static byte[] keyToBytes(String transactionalId) {
        return MessageUtil.toCoordinatorTypePrefixedBytes(
                new TransactionLogKey().setTransactionalId(transactionalId)
        );
    }

    /**
     * Generates the payload bytes for transaction log message value
     *
     * @return value payload bytes
     */
    public static byte[] valueToBytes(TxnTransitMetadata txnMetadata,
                                      TransactionVersion transactionVersionLevel) {
        if (txnMetadata.txnState() == TransactionState.EMPTY && !txnMetadata.topicPartitions().isEmpty()) {
            throw new IllegalStateException("Transaction is not expected to have any partitions since its state is "
                    + txnMetadata.txnState() + ": " + txnMetadata);
        }

        List<TransactionLogValue.PartitionsSchema> transactionPartitions = null;

        if (txnMetadata.txnState() != TransactionState.EMPTY) {
            transactionPartitions = txnMetadata.topicPartitions().stream()
                    .collect(Collectors.groupingBy(TopicPartition::topic))
                    .entrySet().stream()
                    .map(entry ->
                        new TransactionLogValue.PartitionsSchema().setTopic(entry.getKey())
                            .setPartitionIds(entry.getValue().stream().map(TopicPartition::partition).toList())).toList();
        }

        return MessageUtil.toVersionPrefixedBytes(
                transactionVersionLevel.transactionLogValueVersion(),
                new TransactionLogValue()
                        .setProducerId(txnMetadata.producerId())
                        .setProducerEpoch(txnMetadata.producerEpoch())
                        .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs())
                        .setTransactionStatus(txnMetadata.txnState().id())
                        .setTransactionLastUpdateTimestampMs(txnMetadata.txnLastUpdateTimestamp())
                        .setTransactionStartTimestampMs(txnMetadata.txnStartTimestamp())
                        .setTransactionPartitions(transactionPartitions)
                        .setClientTransactionVersion(txnMetadata.clientTransactionVersion().featureLevel())
        );
    }

    /**
     * Decodes the transaction log messages' key
     *
     * @return the transactional id
     * @throws IllegalStateException if the version is not a valid transaction log key version
     */
    public static String readTxnRecordKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version == CoordinatorRecordType.TRANSACTION_LOG.id()) {
            return new TransactionLogKey(new ByteBufferAccessor(buffer), (short) 0).transactionalId();
        } else {
            throw new IllegalStateException("Unknown version " + version + " from the transaction log message key");
        }
    }

    /**
     * Decodes the transaction log messages' payload and retrieves the transaction metadata from it
     *
     * @return a transaction metadata object from the message, or null if tombstone
     */
    public static TransactionMetadata readTxnRecordValue(String transactionalId, ByteBuffer buffer) {
        if (buffer == null) {
            return null; // tombstone
        } else {
            short version = buffer.getShort();
            if (version >= TransactionLogValue.LOWEST_SUPPORTED_VERSION
                    && version <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {

                TransactionLogValue value = new TransactionLogValue(new ByteBufferAccessor(buffer), version);
                TransactionState state = TransactionState.fromId(value.transactionStatus());

                Set<TopicPartition> tps = new HashSet<>();
                if (state != TransactionState.EMPTY) {
                    for (TransactionLogValue.PartitionsSchema partitionsSchema : value.transactionPartitions()) {
                        for (int partitionId : partitionsSchema.partitionIds()) {
                            tps.add(new TopicPartition(partitionsSchema.topic(), partitionId));
                        }
                    }
                }

                return new TransactionMetadata(
                        transactionalId,
                        value.producerId(),
                        value.previousProducerId(),
                        value.nextProducerId(),
                        value.producerEpoch(),
                        RecordBatch.NO_PRODUCER_EPOCH,
                        value.transactionTimeoutMs(),
                        state,
                        tps,
                        value.transactionStartTimestampMs(),
                        value.transactionLastUpdateTimestampMs(),
                        TransactionVersion.fromFeatureLevel(value.clientTransactionVersion())
                );
            } else {
                throw new IllegalStateException("Unknown version " + version + " from the transaction log message value");
            }
        }
    }
}
