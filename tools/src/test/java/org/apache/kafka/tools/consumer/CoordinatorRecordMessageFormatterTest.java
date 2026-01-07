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
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class CoordinatorRecordMessageFormatterTest {
    private static final String TOPIC = "TOPIC";

    protected abstract CoordinatorRecordMessageFormatter formatter();
    protected abstract Stream<Arguments> parameters();

    @ParameterizedTest
    @MethodSource("parameters")
    public void testMessageFormatter(byte[] keyBuffer, byte[] valueBuffer, String expectedOutput) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            TOPIC,
            0,
            0,
            0L,
            TimestampType.CREATE_TIME,
            0,
            0,
            keyBuffer,
            valueBuffer,
            new RecordHeaders(),
            Optional.empty()
        );

        try (MessageFormatter formatter = formatter()) {
            formatter.configure(Map.of());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            formatter.writeTo(record, new PrintStream(out));
            assertEquals(expectedOutput.replaceAll("\\s+", ""), out.toString());
        }
    }
}
