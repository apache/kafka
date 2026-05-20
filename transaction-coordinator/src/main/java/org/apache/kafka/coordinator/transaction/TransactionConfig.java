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

import org.apache.kafka.server.config.ServerConfigs;

public record TransactionConfig(
        int transactionalIdExpirationMs,
        int transactionMaxTimeoutMs,
        int transactionLogNumPartitions,
        short transactionLogReplicationFactor,
        int transactionLogSegmentBytes,
        int transactionLogLoadBufferSize,
        int transactionLogMinInsyncReplicas,
        int abortTimedOutTransactionsIntervalMs,
        int removeExpiredTransactionalIdsIntervalMs,
        boolean transaction2PCEnable,
        int requestTimeoutMs) {

    public TransactionConfig() {
        this(TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT,
                TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT,
                TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_DEFAULT,
                TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                TransactionLogConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_DEFAULT,
                TransactionLogConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_DEFAULT,
                TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_DEFAULT,
                TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT,
                TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_DEFAULT,
                TransactionStateManagerConfig.TRANSACTIONS_2PC_ENABLED_DEFAULT,
                ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT);
    }
}
