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
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.server.common.TransactionVersion;

import java.util.HashSet;

/**
 * Represent the target transition of the transaction metadata. The topicPartitions field is mutable.
 */
public record TxnTransitMetadata(
        long producerId,
        long prevProducerId,
        long nextProducerId,
        short producerEpoch,
        short lastProducerEpoch,
        short nextProducerEpoch,
        int txnTimeoutMs,
        TransactionState txnState,
        // The TransactionMetadata#topicPartitions field is mutable.
        // To avoid deep copy when assigning value from TxnTransitMetadata to TransactionMetadata, use HashSet here.
        HashSet<TopicPartition> topicPartitions,
        long txnStartTimestamp,
        long txnLastUpdateTimestamp,
        TransactionVersion clientTransactionVersion
) {
    private boolean hasNextProducerEpoch() {
        return nextProducerEpoch != RecordBatch.NO_PRODUCER_EPOCH;
    }

    // When InitProducerId keeps an Ongoing transaction (which is "prepared" from
    // the 2PC protocol perspective) it needs to preserve the producerId / epoch of
    // the transaction (that will be used for sending markers), but update the
    // producerId / epoch that's going to be used by client (so that we could fence
    // stale client requests):
    //  - without the nextProducer* fields, the producer* fields are both the
    //    transaction's and the client's identity;
    //  - with the nextProducer* fields set, the producer* fields stay the identity
    //    of the ongoing transaction (frozen for the markers), and the nextProducer*
    //    fields are the client's identity.
    // The producer* fields always describe the ongoing transaction, so if the server
    // is downgraded (and ignores nextProducerEpoch), the transaction is still completed
    // with the right markers (e.g. aborted on a new InitProducerId); only the
    // client-facing identity is lost, so stale client instances aren't fenced by it.
    // Note that we check hasNextProducerEpoch because nextProducerId could
    // be set in other conditions, not related to "prepared" 2PC transactions
    public long clientProducerId() {
        return hasNextProducerEpoch() ? nextProducerId : producerId;
    }

    public short clientProducerEpoch() {
        return hasNextProducerEpoch() ? nextProducerEpoch : producerEpoch;
    }

    public long ongoingTxnProducerId() {
        return hasNextProducerEpoch() ? producerId : RecordBatch.NO_PRODUCER_ID;
    }

    public short ongoingTxnProducerEpoch() {
        return hasNextProducerEpoch() ? producerEpoch : RecordBatch.NO_PRODUCER_EPOCH;
    }
}
