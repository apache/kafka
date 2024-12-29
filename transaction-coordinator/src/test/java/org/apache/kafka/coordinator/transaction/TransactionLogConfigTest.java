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

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TransactionLogConfigTest {
    @Test
    void ShouldDefineAllConfigInConfigDef() {
        Set<String> declaredConfigs = Arrays.stream(TransactionLogConfig.class.getDeclaredFields())
                .filter(field -> field.getName().endsWith("_CONFIG"))
                .peek(field -> field.setAccessible(true))
                .map(field -> assertDoesNotThrow(() -> (String) field.get(null)))
                .collect(Collectors.toSet());
        assertEquals(declaredConfigs,  TransactionLogConfig.CONFIG_DEF.names());
    }

    @Test
    void ShouldGetStaticValueFromClassAttribute() {
        AbstractConfig config = mock(AbstractConfig.class);
        doReturn(1).when(config).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG);
        doReturn(2).when(config).getInt(TransactionLogConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG);
        doReturn((short) 3).when(config).getShort(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG);
        doReturn(4).when(config).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG);
        doReturn(5).when(config).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG);
        doReturn(6).when(config).getInt(TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG);

        TransactionLogConfig transactionLogConfig = new TransactionLogConfig(config);

        assertEquals(1, transactionLogConfig.transactionTopicMinISR());
        assertEquals(2, transactionLogConfig.transactionLoadBufferSize());
        assertEquals((short) 3, transactionLogConfig.transactionTopicReplicationFactor());
        assertEquals(4, transactionLogConfig.transactionTopicPartitions());
        assertEquals(5, transactionLogConfig.transactionTopicSegmentBytes());
        assertEquals(6, transactionLogConfig.producerIdExpirationCheckIntervalMs());


        // If the following calls are missing, we won’t be able to distinguish whether the value is set in the constructor or if
        // it fetches the latest value from AbstractConfig with each call.
        transactionLogConfig.transactionTopicMinISR();
        transactionLogConfig.transactionLoadBufferSize();
        transactionLogConfig.transactionTopicReplicationFactor();
        transactionLogConfig.transactionTopicPartitions();
        transactionLogConfig.transactionTopicSegmentBytes();
        transactionLogConfig.producerIdExpirationCheckIntervalMs();

        verify(config, times(1)).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG);
        verify(config, times(1)).getInt(TransactionLogConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG);
        verify(config, times(1)).getShort(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG);
        verify(config, times(1)).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG);
        verify(config, times(1)).getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG);
        verify(config, times(1)).getInt(TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_CONFIG);
    }

    @Test
    void ShouldGetDynamicValueFromAbstractConfig() {
        AbstractConfig config = mock(AbstractConfig.class);
        doReturn(false).when(config).getBoolean(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG);
        doReturn(88).when(config).getInt(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG);

        TransactionLogConfig transactionLogConfig = new TransactionLogConfig(config);

        assertFalse(transactionLogConfig.transactionPartitionVerificationEnable());
        assertEquals(88, transactionLogConfig.producerIdExpirationMs());

        // If the following calls are missing, we won’t be able to distinguish whether the value is set in the constructor or if
        // it fetches the latest value from AbstractConfig with each call.
        transactionLogConfig.transactionPartitionVerificationEnable();
        transactionLogConfig.producerIdExpirationMs();

        verify(config, times(2)).getBoolean(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG);
        verify(config, times(2)).getInt(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG);
    }
}