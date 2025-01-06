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
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetMessageFormatterTest {

    private static final OffsetCommitKey OFFSET_COMMIT_KEY = new OffsetCommitKey()
            .setGroup("group-id")
            .setTopic("foo")
            .setPartition(1);
    private static final OffsetCommitValue OFFSET_COMMIT_VALUE = new OffsetCommitValue()
            .setOffset(100L)
            .setLeaderEpoch(10)
            .setMetadata("metadata")
            .setCommitTimestamp(1234L)
            .setExpireTimestamp(-1L);
    private static final GroupMetadataKey GROUP_METADATA_KEY = new GroupMetadataKey().setGroup("group-id");
    private static final GroupMetadataValue GROUP_METADATA_VALUE = new GroupMetadataValue()
            .setProtocolType("consumer")
            .setGeneration(1)
            .setProtocol("range")
            .setLeader("leader")
            .setMembers(Collections.emptyList());
    private static final String TOPIC = "TOPIC";

    private static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 10, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 10, OFFSET_COMMIT_VALUE).array(),
                        ""
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":0,\"data\":{\"offset\":100,\"metadata\":\"metadata\"," +
                            "\"commitTimestamp\":1234}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":1,\"data\":{\"offset\":100,\"metadata\":\"metadata\"," +
                            "\"commitTimestamp\":1234,\"expireTimestamp\":-1}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":2,\"data\":{\"offset\":100,\"metadata\":\"metadata\"," +
                            "\"commitTimestamp\":1234}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 3, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":3,\"data\":{\"offset\":100,\"leaderEpoch\":10," +
                            "\"metadata\":\"metadata\",\"commitTimestamp\":1234}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 4, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":4,\"data\":{\"offset\":100,\"leaderEpoch\":10," +
                            "\"metadata\":\"metadata\",\"commitTimestamp\":1234}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 5, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 4, OFFSET_COMMIT_VALUE).array(),
                        ""
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 5, OFFSET_COMMIT_VALUE).array(),
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":{\"version\":5,\"data\":\"unknown\"}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        null,
                        "{\"key\":{\"type\":0,\"data\":{\"group\":\"group-id\",\"topic\":\"foo\",\"partition\":1}}," +
                            "\"value\":null}"),
                Arguments.of(
                        null,
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_VALUE).array(),
                        ""),
                Arguments.of(null, null, ""),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_VALUE).array(),
                        ""
                )
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testTransactionLogMessageFormatter(byte[] keyBuffer, byte[] valueBuffer, String expectedOutput) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                TOPIC, 0, 0,
                0L, TimestampType.CREATE_TIME, 0,
                0, keyBuffer, valueBuffer,
                new RecordHeaders(), Optional.empty());
        
        try (MessageFormatter formatter = new OffsetsMessageFormatter()) {
            formatter.configure(emptyMap());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            formatter.writeTo(record, new PrintStream(out));
            assertEquals(expectedOutput, out.toString());
        }
    }
}
