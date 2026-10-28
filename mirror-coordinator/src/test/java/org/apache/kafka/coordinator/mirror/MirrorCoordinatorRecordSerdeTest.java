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

package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MirrorCoordinatorRecordSerdeTest {
    private static final Uuid TOPIC_ID = new Uuid(1L, 2L);
    private final MirrorCoordinatorRecordSerde serde = new MirrorCoordinatorRecordSerde();

    @Test
    void testMirrorPartitionStateWireFormat() {
        CoordinatorRecord record = stateRecord("error");
        // Version 0, fixed-width fields, compact nullable error string, and no tagged fields.
        byte[] value = ByteBuffer.allocate(21)
            .putShort((short) 0) // Value schema version.
            .put((byte) 8) // State: FAILED.
            .putInt(42) // LeaderEpoch.
            .putInt(7) // StateEpoch.
            .put((byte) 6) // Compact ErrorMessage length: five UTF-8 bytes plus one.
            .put("error".getBytes(UTF_8)) // ErrorMessage contents.
            .putShort((short) 3) // RetryAttempt.
            .put((byte) 3) // PreviousState: MIRRORING.
            .put((byte) 0) // Number of tagged fields.
            .array();

        assertWireFormat(record, keyBytes((short) 0), value);
    }

    @Test
    void testLastMirrorEpochsWireFormat() {
        CoordinatorRecord record = epochRecord();
        byte[] value = ByteBuffer.allocate(15)
            .putShort((short) 0) // Value schema version.
            .putInt(42) // LastMirrorEpoch.
            .putLong(4_294_967_313L) // LastMirrorOffset: exceeds the 32-bit integer range.
            .put((byte) 0) // Number of tagged fields.
            .array();

        assertWireFormat(record, keyBytes((short) 1), value);
    }

    @ParameterizedTest
    @MethodSource("records")
    void testRoundTrip(CoordinatorRecord record) {
        assertRoundTrip(record);
    }

    @Test
    void testSchemaDefaults() {
        MirrorPartitionStateValue state = new MirrorPartitionStateValue();
        assertEquals((byte) 0, state.state());
        assertEquals(-1, state.leaderEpoch());
        assertEquals(0, state.stateEpoch());
        assertNull(state.errorMessage());
        assertEquals((short) 0, state.retryAttempt());
        assertEquals((byte) -1, state.previousState());

        LastMirrorEpochsValue epochs = new LastMirrorEpochsValue();
        assertEquals(0, epochs.lastMirrorEpoch());
        assertEquals(-1L, epochs.lastMirrorOffset());

        assertRoundTrip(CoordinatorRecord.record(stateRecord(null).key(), new ApiMessageAndVersion(state, (short) 0)));
        assertRoundTrip(CoordinatorRecord.record(epochRecord().key(), new ApiMessageAndVersion(epochs, (short) 0)));
    }

    @ParameterizedTest
    @MethodSource("records")
    void testTombstone(CoordinatorRecord record) {
        CoordinatorRecord tombstone = CoordinatorRecord.tombstone(record.key());
        assertNull(serde.serializeValue(tombstone));
        assertEquals(tombstone, deserialize(serde.serializeKey(tombstone), null));
    }

    @ParameterizedTest
    @ValueSource(shorts = {-1, 2, 3, 255, Short.MAX_VALUE})
    void testUnknownRecordType(short recordType) {
        byte[] key = ByteBuffer.allocate(Short.BYTES).putShort(recordType).array();
        Deserializer.UnknownRecordTypeException exception = assertThrows(
            Deserializer.UnknownRecordTypeException.class,
            () -> deserialize(key, null)
        );
        assertEquals(recordType, exception.unknownType());
        assertThrows(Deserializer.UnknownRecordTypeException.class, () -> serde.apiMessageValueFor(recordType));
    }

    @ParameterizedTest
    @MethodSource("records")
    void testUnsupportedValueVersion(CoordinatorRecord record) {
        for (short version : new short[] {-1, 1}) {
            byte[] value = ByteBuffer.allocate(Short.BYTES).putShort(version).array();
            Deserializer.UnknownRecordVersionException exception = assertThrows(
                Deserializer.UnknownRecordVersionException.class,
                () -> deserialize(serde.serializeKey(record), value)
            );
            assertEquals(record.key().apiKey(), exception.type());
            assertEquals(version, exception.unknownVersion());
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testTruncatedKey(CoordinatorRecord record) {
        byte[] key = serde.serializeKey(record);
        byte[] value = serde.serializeValue(record);
        for (int length = 0; length < key.length; length++) {
            byte[] truncated = Arrays.copyOf(key, length);
            assertThrows(RuntimeException.class, () -> deserialize(truncated, value), "Key length " + length);
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testTruncatedValue(CoordinatorRecord record) {
        byte[] key = serde.serializeKey(record);
        byte[] value = serde.serializeValue(record);
        for (int length = 0; length < value.length; length++) {
            byte[] truncated = Arrays.copyOf(value, length);
            assertThrows(RuntimeException.class, () -> deserialize(key, truncated), "Value length " + length);
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testUnknownTaggedFields(CoordinatorRecord record) {
        ApiMessage value = record.value().message();
        value.unknownTaggedFields().add(new RawTaggedField(10, new byte[] {1, 2, 3}));
        assertRoundTrip(record);
    }

    private void assertRoundTrip(CoordinatorRecord record) {
        assertEquals(record, deserialize(serde.serializeKey(record), serde.serializeValue(record)));
    }

    private void assertWireFormat(CoordinatorRecord record, byte[] key, byte[] value) {
        assertArrayEquals(key, serde.serializeKey(record));
        assertArrayEquals(value, serde.serializeValue(record));
        assertEquals(record, deserialize(key, value));
    }

    private CoordinatorRecord deserialize(byte[] key, byte[] value) {
        return serde.deserialize(ByteBuffer.wrap(key), value == null ? null : ByteBuffer.wrap(value));
    }

    private static byte[] keyBytes(short recordType) {
        return ByteBuffer.allocate(30)
            .putShort(recordType) // Coordinator record type: state (0) or last mirror epochs (1).
            .putShort((short) 6) // MirrorName length in UTF-8 bytes.
            .put("mirror".getBytes(UTF_8)) // MirrorName contents.
            .putLong(1L) // TopicId: most significant bits.
            .putLong(2L) // TopicId: least significant bits.
            .putInt(4) // Partition index.
            .array();
    }

    private static Stream<CoordinatorRecord> records() {
        return Stream.of(stateRecord(null), stateRecord("error"), epochRecord());
    }

    private static CoordinatorRecord stateRecord(String errorMessage) {
        return CoordinatorRecord.record(
            new MirrorPartitionStateKey().setMirrorName("mirror").setTopicId(TOPIC_ID).setPartition(4),
            new ApiMessageAndVersion(
                new MirrorPartitionStateValue()
                    .setState((byte) 8)
                    .setLeaderEpoch(42)
                    .setStateEpoch(7)
                    .setErrorMessage(errorMessage)
                    .setRetryAttempt((short) 3)
                    .setPreviousState((byte) 3),
                (short) 0
            )
        );
    }

    private static CoordinatorRecord epochRecord() {
        return CoordinatorRecord.record(
            new LastMirrorEpochsKey().setMirrorName("mirror").setTopicId(TOPIC_ID).setPartition(4),
            new ApiMessageAndVersion(
                new LastMirrorEpochsValue().setLastMirrorEpoch(42).setLastMirrorOffset(4_294_967_313L),
                (short) 0
            )
        );
    }
}
