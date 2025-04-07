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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.config.BrokerReconfigurable;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class DynamicProducerStateManagerConfig implements BrokerReconfigurable {
    private final Logger log = LoggerFactory.getLogger(DynamicProducerStateManagerConfig.class);
    private final ProducerStateManagerConfig producerStateManagerConfig;

    public DynamicProducerStateManagerConfig(ProducerStateManagerConfig producerStateManagerConfig) {
        this.producerStateManagerConfig = producerStateManagerConfig;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.of(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG, TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG);
    }

    @Override
    public void validateReconfiguration(AbstractConfig newConfig) {
        TransactionLogConfig transactionLogConfig = new TransactionLogConfig(newConfig);
        if (transactionLogConfig.producerIdExpirationMs() < 0)
            throw new ConfigException(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG + "cannot be less than 0, current value is " +
                                      producerStateManagerConfig.producerIdExpirationMs() + ", and new value is " + transactionLogConfig.producerIdExpirationMs());
    }

    @Override
    public void reconfigure(AbstractConfig oldConfig, AbstractConfig newConfig) {
        TransactionLogConfig transactionLogConfig = new TransactionLogConfig(newConfig);
        if (producerStateManagerConfig.producerIdExpirationMs() != transactionLogConfig.producerIdExpirationMs()) {
            log.info("Reconfigure {} from {} to {}",
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG,
                producerStateManagerConfig.producerIdExpirationMs(),
                transactionLogConfig.producerIdExpirationMs());
            producerStateManagerConfig.setProducerIdExpirationMs(transactionLogConfig.producerIdExpirationMs());
        }
        if (producerStateManagerConfig.transactionVerificationEnabled() != transactionLogConfig.transactionPartitionVerificationEnable()) {
            log.info("Reconfigure {} from {} to {}",
                TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG,
                producerStateManagerConfig.transactionVerificationEnabled(),
                transactionLogConfig.transactionPartitionVerificationEnable());
            producerStateManagerConfig.setTransactionVerificationEnabled(transactionLogConfig.transactionPartitionVerificationEnable());
        }
    }
}
