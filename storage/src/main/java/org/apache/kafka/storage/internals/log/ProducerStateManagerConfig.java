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

import org.apache.kafka.common.utils.Utils;

import java.util.Set;

public class ProducerStateManagerConfig {
    public static final String PRODUCER_ID_EXPIRATION_MS = "producer.id.expiration.ms";
    public static final String TRANSACTION_VERIFICATION_ENABLED = "transaction.partition.verification.enable";
    public static final Set<String> RECONFIGURABLE_CONFIGS = Utils.mkSet(PRODUCER_ID_EXPIRATION_MS, TRANSACTION_VERIFICATION_ENABLED);

    private volatile int producerIdExpirationMs;
    private volatile boolean transactionVerificationEnabled;

    public ProducerStateManagerConfig(int producerIdExpirationMs, boolean transactionVerificationEnabled) {
        this.producerIdExpirationMs = producerIdExpirationMs;
        this.transactionVerificationEnabled = transactionVerificationEnabled;
    }

    public void setProducerIdExpirationMs(int producerIdExpirationMs) {
        this.producerIdExpirationMs = producerIdExpirationMs;
    }

    public void setTransactionVerificationEnabled(boolean transactionVerificationEnabled) {
        this.transactionVerificationEnabled = transactionVerificationEnabled;
    }

    public int producerIdExpirationMs() {
        return producerIdExpirationMs;
    }

    public boolean transactionVerificationEnabled() {
        return transactionVerificationEnabled;
    }
}
