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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public final class TransactionStateManagerConfig {
    // Transaction management configs and default values
    public static final String TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG = "transaction.max.timeout.ms";
    public static final int TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT = (int) TimeUnit.MINUTES.toMillis(15);
    public static final String TRANSACTIONS_MAX_TIMEOUT_MS_DOC = "The maximum allowed timeout for transactions. " +
            "If a client’s requested transaction time exceed this, then the broker will return an error in InitProducerIdRequest. " +
            "This prevents a client from too large of a timeout, which can stall consumers reading from topics included in the transaction.";

    public static final String TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG = "transactional.id.expiration.ms";
    public static final int TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT = (int) TimeUnit.DAYS.toMillis(7);
    public static final String TRANSACTIONAL_ID_EXPIRATION_MS_DOC = "The time in ms that the transaction coordinator will wait without receiving any transaction status updates " +
            "for the current transaction before expiring its transactional id. Transactional IDs will not expire while a the transaction is still ongoing.";

    public static final String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG = "transaction.abort.timed.out.transaction.cleanup.interval.ms";
    public static final int TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT = (int) TimeUnit.SECONDS.toMillis(10);
    public static final String TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to rollback transactions that have timed out";

    public static final String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG = "transaction.remove.expired.transaction.cleanup.interval.ms";
    public static final int TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_DEFAULT = (int) TimeUnit.HOURS.toMillis(1);
    public static final String TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC = "The interval at which to remove transactions that have expired due to <code>transactional.id.expiration.ms</code> passing";

    public static final String METRICS_GROUP = "transaction-coordinator-metrics";
    public static final String LOAD_TIME_SENSOR = "TransactionsPartitionLoadTime";
    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG, INT, TRANSACTIONAL_ID_EXPIRATION_MS_DEFAULT, atLeast(1), HIGH, TRANSACTIONAL_ID_EXPIRATION_MS_DOC)
            .define(TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG, INT, TRANSACTIONS_MAX_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, TRANSACTIONS_MAX_TIMEOUT_MS_DOC)
            .define(TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, INT, TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTIONS_INTERVAL_MS_DOC)
            .define(TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG, INT, TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_DEFAULT, atLeast(1), LOW, TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONS_INTERVAL_MS_DOC);

    private final int transactionalIdExpirationMs;
    private final int transactionMaxTimeoutMs;
    private final int transactionAbortTimedOutTransactionCleanupIntervalMs;
    private final int transactionRemoveExpiredTransactionalIdCleanupIntervalMs;

    public TransactionStateManagerConfig(AbstractConfig config) {
        transactionalIdExpirationMs = config.getInt(TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG);
        transactionMaxTimeoutMs = config.getInt(TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG);
        transactionAbortTimedOutTransactionCleanupIntervalMs = config.getInt(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG);
        transactionRemoveExpiredTransactionalIdCleanupIntervalMs = config.getInt(TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG);
    }
    public int transactionalIdExpirationMs() {
        return transactionalIdExpirationMs;
    }

    public int transactionMaxTimeoutMs() {
        return transactionMaxTimeoutMs;
    }

    public int transactionAbortTimedOutTransactionCleanupIntervalMs() {
        return transactionAbortTimedOutTransactionCleanupIntervalMs;
    }

    public int transactionRemoveExpiredTransactionalIdCleanupIntervalMs() {
        return transactionRemoveExpiredTransactionalIdCleanupIntervalMs;
    }
}
