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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorStateMessageFormatterTest extends CoordinatorRecordMessageFormatterTest {
    private static final Uuid TOPIC_ID = Uuid.fromString("1Y8fjhRoQsqFkIrwd1FIeg");
    private static final MirrorPartitionStateKey STATE_KEY = new MirrorPartitionStateKey()
        .setMirrorName("a-to-b")
        .setTopicId(TOPIC_ID)
        .setPartition(0);
    private static final MirrorPartitionStateValue STATE_VALUE = new MirrorPartitionStateValue()
        .setState(MirrorPartitionState.FAILED.value())
        .setLeaderEpoch(42)
        .setStateEpoch(7)
        .setErrorMessage("error")
        .setRetryAttempt((short) 3)
        .setPreviousState(MirrorPartitionState.MIRRORING.value());
    private static final LastMirrorEpochsKey EPOCHS_KEY = new LastMirrorEpochsKey()
        .setMirrorName("a-to-b")
        .setTopicId(TOPIC_ID)
        .setPartition(0);
    private static final LastMirrorEpochsValue EPOCHS_VALUE = new LastMirrorEpochsValue()
        .setLastMirrorEpoch(42)
        .setLastMirrorOffset(4_294_967_313L); // Verify offsets larger than a 32-bit integer.

    @Override
    protected CoordinatorRecordMessageFormatter formatter() {
        return new MirrorStateMessageFormatter();
    }

    @Override
    protected Stream<Arguments> parameters() {
        byte[] stateKey = MessageUtil.toCoordinatorTypePrefixedBytes(STATE_KEY);
        byte[] epochsKey = MessageUtil.toCoordinatorTypePrefixedBytes(EPOCHS_KEY);
        return Stream.of(
            Arguments.of(
                stateKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, STATE_VALUE),
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,
                              "data":{"state":8,
                                      "leaderEpoch":42,
                                      "stateEpoch":7,
                                      "errorMessage":"error",
                                      "retryAttempt":3,
                                      "previousState":3,
                                      "stateName":"FAILED",
                                      "previousStateName":"MIRRORING"}}}
                """
            ),
            Arguments.of(
                stateKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, new MirrorPartitionStateValue()),
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,
                              "data":{"state":0,
                                      "leaderEpoch":-1,
                                      "stateEpoch":0,
                                      "errorMessage":null,
                                      "retryAttempt":0,
                                      "previousState":-1,
                                      "stateName":"LOG_ALIGNMENT",
                                      "previousStateName":"UNKNOWN"}}}
                """
            ),
            Arguments.of(
                stateKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, new MirrorPartitionStateValue()
                    .setState(Byte.MAX_VALUE)
                    .setPreviousState(MirrorPartitionState.MIRRORING.value())),
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,
                              "data":{"state":127,
                                      "leaderEpoch":-1,
                                      "stateEpoch":0,
                                      "errorMessage":null,
                                      "retryAttempt":0,
                                      "previousState":3,
                                      "stateName":"127",
                                      "previousStateName":"MIRRORING"}}}
                """
            ),
            Arguments.of(
                stateKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, new MirrorPartitionStateValue()
                    .setState(MirrorPartitionState.FAILED.value())
                    .setPreviousState(Byte.MIN_VALUE)),
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,
                              "data":{"state":8,
                                      "leaderEpoch":-1,
                                      "stateEpoch":0,
                                      "errorMessage":null,
                                      "retryAttempt":0,
                                      "previousState":-128,
                                      "stateName":"FAILED",
                                      "previousStateName":"-128"}}}
                """
            ),
            Arguments.of(
                stateKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, new MirrorPartitionStateValue()
                    .setState(Byte.MAX_VALUE)
                    .setPreviousState(Byte.MIN_VALUE)),
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,
                              "data":{"state":127,
                                      "leaderEpoch":-1,
                                      "stateEpoch":0,
                                      "errorMessage":null,
                                      "retryAttempt":0,
                                      "previousState":-128,
                                      "stateName":"127",
                                      "previousStateName":"-128"}}}
                """
            ),
            Arguments.of(
                epochsKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, EPOCHS_VALUE),
                """
                    {"key":{"type":1,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,"data":{"lastMirrorEpoch":42,"lastMirrorOffset":4294967313}}}
                """
            ),
            Arguments.of(
                epochsKey,
                MessageUtil.toVersionPrefixedBytes((short) 0, new LastMirrorEpochsValue()),
                """
                    {"key":{"type":1,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},
                     "value":{"version":0,"data":{"lastMirrorEpoch":-1,"lastMirrorOffset":-1}}}
                """
            ),
            Arguments.of(
                stateKey,
                null,
                """
                    {"key":{"type":0,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},"value":null}
                """
            ),
            Arguments.of(
                epochsKey,
                null,
                """
                    {"key":{"type":1,"data":{"mirrorName":"a-to-b","topicId":"1Y8fjhRoQsqFkIrwd1FIeg","partition":0}},"value":null}
                """
            ),
            Arguments.of(null, MessageUtil.toVersionPrefixedBytes((short) 0, STATE_VALUE), ""),
            Arguments.of(null, null, ""),
            Arguments.of(ByteBuffer.allocate(Short.BYTES).putShort((short) 2).array(), null, ""),
            Arguments.of(ByteBuffer.allocate(Short.BYTES).putShort(Short.MAX_VALUE).array(), null, "")
        );
    }

    private static Stream<ApiMessage> keys() {
        return Stream.of(STATE_KEY, EPOCHS_KEY);
    }

    @ParameterizedTest
    @MethodSource("keys")
    public void testUnsupportedValueVersion(ApiMessage key) {
        for (short version : new short[] {-1, 1}) {
            RuntimeException exception = assertFormattingFails(
                MessageUtil.toCoordinatorTypePrefixedBytes(key),
                ByteBuffer.allocate(Short.BYTES).putShort(version).array()
            );
            Deserializer.UnknownRecordVersionException cause = assertInstanceOf(
                Deserializer.UnknownRecordVersionException.class, exception.getCause());
            assertEquals(key.apiKey(), cause.type());
            assertEquals(version, cause.unknownVersion());
        }
    }

    private static Stream<Arguments> malformedRecords() {
        byte[] stateKey = MessageUtil.toCoordinatorTypePrefixedBytes(STATE_KEY);
        byte[] epochsKey = MessageUtil.toCoordinatorTypePrefixedBytes(EPOCHS_KEY);
        return Stream.of(
            Arguments.of(new byte[0], null),
            Arguments.of(new byte[] {0}, null),
            Arguments.of(Arrays.copyOf(stateKey, stateKey.length - 1), null),
            Arguments.of(Arrays.copyOf(epochsKey, epochsKey.length - 1), null),
            Arguments.of(stateKey, new byte[0]),
            Arguments.of(epochsKey, new byte[] {0}),
            Arguments.of(stateKey, ByteBuffer.allocate(Short.BYTES).putShort((short) 0).array()),
            Arguments.of(epochsKey, ByteBuffer.allocate(Short.BYTES).putShort((short) 0).array())
        );
    }

    @ParameterizedTest
    @MethodSource("malformedRecords")
    public void testMalformedRecord(byte[] key, byte[] value) {
        assertFormattingFails(key, value);
    }

    @Test
    public void testRoundTrip() throws Exception {
        byte[] key = MessageUtil.toCoordinatorTypePrefixedBytes(STATE_KEY);
        byte[] value = MessageUtil.toVersionPrefixedBytes((short) 0, STATE_VALUE);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("__mirror_state", 0, 0, key, value);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (MessageFormatter formatter = formatter(); PrintStream stream = new PrintStream(output, false, UTF_8)) {
            formatter.writeTo(record, stream);
        }

        JsonNode data = new ObjectMapper().readTree(output.toByteArray()).path("value").path("data");
        MirrorPartitionStateValue roundTripped = new MirrorPartitionStateValue()
            .setState((byte) data.get("state").intValue())
            .setLeaderEpoch(data.get("leaderEpoch").intValue())
            .setStateEpoch(data.get("stateEpoch").intValue())
            .setErrorMessage(data.get("errorMessage").textValue())
            .setRetryAttempt((short) data.get("retryAttempt").intValue())
            .setPreviousState((byte) data.get("previousState").intValue());
        assertEquals(STATE_VALUE, roundTripped);
        assertEquals(MirrorPartitionState.FAILED, MirrorPartitionState.fromValue(roundTripped.state()));
        assertEquals(MirrorPartitionState.MIRRORING, MirrorPartitionState.fromValue(roundTripped.previousState()));
    }

    private RuntimeException assertFormattingFails(byte[] key, byte[] value) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("__mirror_state", 0, 42, key, value);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (MessageFormatter formatter = formatter(); PrintStream stream = new PrintStream(output, false, UTF_8)) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> formatter.writeTo(record, stream));
            assertTrue(exception.getMessage().startsWith("Could not read record at offset 42 due to: "));
            assertEquals("", output.toString(UTF_8));
            return exception;
        }
    }
}
