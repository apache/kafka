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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TransactionLogMessageFormatterTest {

    private static TransactionLogKey txnLogKey;
    private static TransactionLogValue txnLogValue;
    private static final String TOPIC = "TOPIC";

    @BeforeAll
    public static void setUp() {
        txnLogKey = new TransactionLogKey()
                .setTransactionalId("TXNID");
        txnLogValue = new TransactionLogValue()
                .setProducerId(100)
                .setProducerEpoch((short) 50)
                .setTransactionStatus((byte) 4)
                .setTransactionStartTimestampMs(750L)
                .setTransactionLastUpdateTimestampMs(1000L)
                .setTransactionTimeoutMs(500)
                .setTransactionPartitions(Collections.emptyList());
    }

    private static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of((short) 10, (short) 10,
                    "{\"key\":{\"version\":\"10\",\"data\":\"unknown\"}," +
                        "\"value\":{\"version\":\"10\",\"data\":\"unknown\"}}"),
                Arguments.of((short) 0, (short) 1,
                    "{\"key\":{\"version\":\"0\",\"data\":{\"transactionalId\":\"TXNID\"}}," +
                        "\"value\":{\"version\":\"1\",\"data\":{\"producerId\":100,\"producerEpoch\":50,\"transactionTimeoutMs\":500," +
                        "\"transactionStatus\":4,\"transactionPartitions\":[],\"transactionLastUpdateTimestampMs\":1000," +
                        "\"transactionStartTimestampMs\":750}}}"),
                Arguments.of((short) 0, (short) 5,
                    "{\"key\":{\"version\":\"0\",\"data\":{\"transactionalId\":\"TXNID\"}}," +
                        "\"value\":{\"version\":\"5\",\"data\":\"unknown\"}}"),
                Arguments.of((short) 1, (short) 1,
                    "{\"key\":{\"version\":\"1\",\"data\":\"unknown\"}," +
                        "\"value\":{\"version\":\"1\",\"data\":{\"producerId\":100,\"producerEpoch\":50,\"transactionTimeoutMs\":500," +
                        "\"transactionStatus\":4,\"transactionPartitions\":[],\"transactionLastUpdateTimestampMs\":1000," +
                        "\"transactionStartTimestampMs\":750}}}")
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testTransactionLogMessageFormatter(short keyVersion, short valueVersion, String expectedOutput) {
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(keyVersion, txnLogKey);
        ByteBuffer valueBuffer = MessageUtil.toVersionPrefixedByteBuffer(valueVersion, txnLogValue);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TOPIC, 0, 0,
                0L, TimestampType.CREATE_TIME, 0,
                0, keyBuffer.array(), valueBuffer.array(),
                new RecordHeaders(), Optional.empty());

        try (MessageFormatter formatter = new TransactionLogMessageFormatter()) {
            formatter.configure(new HashMap<>());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            formatter.writeTo(record, new PrintStream(out));
            assertEquals(expectedOutput, out.toString());
        }
    }
}
