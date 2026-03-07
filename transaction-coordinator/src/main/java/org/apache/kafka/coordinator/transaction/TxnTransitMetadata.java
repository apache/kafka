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
    // producerId / epoch that's going ot be used by client (so that we could fence
    // stale client requests).  For backward compatibility (in case of server
    // downgrade) we keep the producerId / epoch of the transaction in the
    // producer* fields and the other pair in the nextProducer* fields, so
    // we have to do a little bit of switcheroo:
    //  - in the absence of nextProducer* fields the producer* represent both
    //  - the presence of nextProducer* fields means that producer* represent ongoing transaction
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
