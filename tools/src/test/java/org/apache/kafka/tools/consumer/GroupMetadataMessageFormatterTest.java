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
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupMetadataMessageFormatterTest {

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
    private static final GroupMetadataValue.MemberMetadata MEMBER_METADATA = new GroupMetadataValue.MemberMetadata()
            .setMemberId("member-1")
            .setClientId("client-1")
            .setClientHost("host-1")
            .setRebalanceTimeout(1000)
            .setSessionTimeout(1500)
            .setGroupInstanceId("group-instance-1")
            .setSubscription(new byte[]{0, 1})
            .setAssignment(new byte[]{1, 2});
    private static final GroupMetadataKey GROUP_METADATA_KEY = new GroupMetadataKey()
            .setGroup("group-id");
    private static final GroupMetadataValue GROUP_METADATA_VALUE = new GroupMetadataValue()
            .setProtocolType("consumer")
            .setGeneration(1)
            .setProtocol("range")
            .setLeader("leader")
            .setMembers(singletonList(MEMBER_METADATA))
            .setCurrentStateTimestamp(1234L);
    private static final String TOPIC = "TOPIC";

    private static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 10, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 10, GROUP_METADATA_VALUE).array(),
                        ""
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, GROUP_METADATA_VALUE).array(),
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":{\"version\":0," +
                            "\"data\":{\"protocolType\":\"consumer\",\"generation\":1,\"protocol\":\"range\"," +
                            "\"leader\":\"leader\",\"members\":[{\"memberId\":\"member-1\",\"clientId\":\"client-1\"," +
                            "\"clientHost\":\"host-1\",\"sessionTimeout\":1500,\"subscription\":\"AAE=\"," +
                            "\"assignment\":\"AQI=\"}]}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 1, GROUP_METADATA_VALUE).array(),
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":{\"version\":1," +
                            "\"data\":{\"protocolType\":\"consumer\",\"generation\":1,\"protocol\":\"range\"," +
                            "\"leader\":\"leader\",\"members\":[{\"memberId\":\"member-1\",\"clientId\":\"client-1\"," +
                            "\"clientHost\":\"host-1\",\"rebalanceTimeout\":1000,\"sessionTimeout\":1500," +
                            "\"subscription\":\"AAE=\",\"assignment\":\"AQI=\"}]}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_VALUE).array(),
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":{\"version\":2," +
                            "\"data\":{\"protocolType\":\"consumer\",\"generation\":1,\"protocol\":\"range\"," +
                            "\"leader\":\"leader\",\"currentStateTimestamp\":1234,\"members\":[{\"memberId\":\"member-1\"," +
                            "\"clientId\":\"client-1\",\"clientHost\":\"host-1\",\"rebalanceTimeout\":1000," +
                            "\"sessionTimeout\":1500,\"subscription\":\"AAE=\",\"assignment\":\"AQI=\"}]}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 3, GROUP_METADATA_VALUE).array(),
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":{\"version\":3," +
                            "\"data\":{\"protocolType\":\"consumer\",\"generation\":1,\"protocol\":\"range\"," +
                            "\"leader\":\"leader\",\"currentStateTimestamp\":1234,\"members\":[{\"memberId\":\"member-1\"," +
                            "\"groupInstanceId\":\"group-instance-1\",\"clientId\":\"client-1\",\"clientHost\":\"host-1\"," +
                            "\"rebalanceTimeout\":1000,\"sessionTimeout\":1500,\"subscription\":\"AAE=\",\"assignment\":\"AQI=\"}]}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 4, GROUP_METADATA_VALUE).array(),
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":{\"version\":4," +
                            "\"data\":{\"protocolType\":\"consumer\",\"generation\":1,\"protocol\":\"range\"," +
                            "\"leader\":\"leader\",\"currentStateTimestamp\":1234,\"members\":[{\"memberId\":\"member-1\"," +
                            "\"groupInstanceId\":\"group-instance-1\",\"clientId\":\"client-1\",\"clientHost\":\"host-1\"," +
                            "\"rebalanceTimeout\":1000,\"sessionTimeout\":1500,\"subscription\":\"AAE=\",\"assignment\":\"AQI=\"}]}}}"
                ),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                        null,
                        "{\"key\":{\"type\":2,\"data\":{\"group\":\"group-id\"}},\"value\":null}"),
                Arguments.of(
                        null,
                        MessageUtil.toVersionPrefixedByteBuffer((short) 4, GROUP_METADATA_VALUE).array(),
                        ""),
                Arguments.of(null, null, ""),
                Arguments.of(
                        MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_KEY).array(),
                        MessageUtil.toVersionPrefixedByteBuffer((short) 4, OFFSET_COMMIT_VALUE).array(),
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

        try (MessageFormatter formatter = new GroupMetadataMessageFormatter()) {
            formatter.configure(emptyMap());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            formatter.writeTo(record, new PrintStream(out));
            assertEquals(expectedOutput, out.toString());
        }
    }
}
