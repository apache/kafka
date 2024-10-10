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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TransactionStateManagerConfigTest {
    @Test
    void ShouldDefineAllConfigInConfigDef() {
        Set<String> declaredConfigs = new HashSet<>();
        for (Field field : TransactionStateManagerConfig.class.getDeclaredFields()) {
            if (field.getName().endsWith("_CONFIG")) {
                field.setAccessible(true);
                try {
                    declaredConfigs.add((String) field.get(null));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Set<String> definedInConfigDef = TransactionStateManagerConfig.CONFIG_DEF.names();
        assertTrue(declaredConfigs.containsAll(definedInConfigDef));
        assertTrue(definedInConfigDef.containsAll(declaredConfigs));
    }

    @Test
    void ShouldGetStaticValueFromClassAttribute() {
        AbstractConfig config = mock(AbstractConfig.class);
        doReturn(1).when(config).getInt(TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG);
        doReturn(2).when(config).getInt(TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG);
        doReturn(3).when(config).getInt(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG);
        doReturn(4).when(config).getInt(TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG);

        TransactionStateManagerConfig transactionStateManagerConfig = new TransactionStateManagerConfig(config);

        assertEquals(1, transactionStateManagerConfig.transactionMaxTimeoutMs());
        assertEquals(2, transactionStateManagerConfig.transactionalIdExpirationMs());
        assertEquals(3, transactionStateManagerConfig.transactionAbortTimedOutTransactionCleanupIntervalMs());
        assertEquals(4, transactionStateManagerConfig.transactionRemoveExpiredTransactionalIdCleanupIntervalMs());


        // If the following calls are missing, we won’t be able to distinguish whether the value is set in the constructor or if
        // it fetches the latest value from AbstractConfig with each call.
        transactionStateManagerConfig.transactionMaxTimeoutMs();
        transactionStateManagerConfig.transactionalIdExpirationMs();
        transactionStateManagerConfig.transactionAbortTimedOutTransactionCleanupIntervalMs();
        transactionStateManagerConfig.transactionRemoveExpiredTransactionalIdCleanupIntervalMs();

        verify(config, times(1)).getInt(TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG);
        verify(config, times(1)).getInt(TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG);
        verify(config, times(1)).getInt(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG);
        verify(config, times(1)).getInt(TransactionStateManagerConfig.TRANSACTIONS_REMOVE_EXPIRED_TRANSACTIONAL_ID_CLEANUP_INTERVAL_MS_CONFIG);
    }

}