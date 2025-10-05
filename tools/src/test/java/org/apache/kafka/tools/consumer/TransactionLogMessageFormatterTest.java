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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;

import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class TransactionLogMessageFormatterTest extends CoordinatorRecordMessageFormatterTest {

    private static final TransactionLogKey TXN_LOG_KEY = new TransactionLogKey()
        .setTransactionalId("TXNID");
    private static final TransactionLogValue TXN_LOG_VALUE = new TransactionLogValue()
        .setProducerId(100)
        .setProducerEpoch((short) 50)
        .setTransactionStatus((byte) 4)
        .setTransactionStartTimestampMs(750L)
        .setTransactionLastUpdateTimestampMs(1000L)
        .setTransactionTimeoutMs(500)
        .setTransactionPartitions(List.of());

    @Override
    protected CoordinatorRecordMessageFormatter formatter() {
        return new TransactionLogMessageFormatter();
    }

    @Override
    protected Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, TXN_LOG_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, TXN_LOG_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, TXN_LOG_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_VALUE).array(),
                """
                    {"key":{"type":0,"data":{"transactionalId":"TXNID"}},
                     "value":{"version":1,
                              "data":{"producerId":100,
                                      "producerEpoch":50,
                                      "transactionTimeoutMs":500,
                                      "transactionStatus":4,
                                      "transactionPartitions":[],
                                      "transactionLastUpdateTimestampMs":1000,
                                      "transactionStartTimestampMs":750}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, TXN_LOG_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_VALUE).array(),
                """
                    {"key":{"type":0,"data":{"transactionalId":"TXNID"}},
                     "value":{"version":1,
                              "data":{"producerId":100,
                                      "producerEpoch":50,
                                      "transactionTimeoutMs":500,
                                      "transactionStatus":4,
                                      "transactionPartitions":[],
                                      "transactionLastUpdateTimestampMs":1000,
                                      "transactionStartTimestampMs":750}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, TXN_LOG_KEY).array(),
                null,
                """
                    {"key":{"type":0,"data":{"transactionalId":"TXNID"}},"value":null}
                """
            ),
            Arguments.of(
                null,
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_VALUE).array(),
                ""
            ),
            Arguments.of(null, null, ""),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer(Short.MAX_VALUE, TXN_LOG_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, TXN_LOG_VALUE).array(),
                ""
            )
        );
    }
}
