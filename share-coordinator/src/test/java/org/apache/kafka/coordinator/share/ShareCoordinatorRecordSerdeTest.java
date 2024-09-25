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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareCoordinatorRecordSerdeTest {
    private ShareCoordinatorRecordSerde serde;

    @BeforeEach
    public void setup() {
        serde = new ShareCoordinatorRecordSerde();
    }

    @Test
    public void testSerializeKey() {
        CoordinatorRecord record = getShareSnapshotRecord("groupId", Uuid.randomUuid(), 1);

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.key().version(), record.key().message()),
            serde.serializeKey(record)
        );
    }

    @Test
    public void testSerializeValue() {
        CoordinatorRecord record = getShareSnapshotRecord("groupId", Uuid.randomUuid(), 1);

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.value().version(), record.value().message()),
            serde.serializeValue(record)
        );
    }

    @Test
    public void testSerializeNullValue() {
        CoordinatorRecord record = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ShareSnapshotKey()
                    .setGroupId("group")
                    .setTopicId(Uuid.randomUuid())
                    .setPartition(1),
                ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION
            ),
            null
        );

        assertNull(serde.serializeValue(record));
    }

    @Test
    public void testDeserialize() {
        CoordinatorRecord record = getShareSnapshotRecord("groupId", Uuid.randomUuid(), 1);
        ApiMessageAndVersion key = record.key();
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        ApiMessageAndVersion value = record.value();
        ByteBuffer valueBuffer = MessageUtil.toVersionPrefixedByteBuffer(value.version(), value.message());

        CoordinatorRecord desRecord = serde.deserialize(keyBuffer, valueBuffer);
        assertEquals(key, desRecord.key());
        assertEquals(value, desRecord.value());
    }

    @Test
    public void testDeserializeWithTombstoneForValue() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ShareSnapshotKey()
                .setGroupId("groupId")
                .setTopicId(Uuid.randomUuid())
                .setPartition(1),
            ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        CoordinatorRecord record = serde.deserialize(keyBuffer, null);
        assertEquals(key, record.key());
        assertNull(record.value());
    }

    @Test
    public void testDeserializeWithInvalidRecordType() {
        ByteBuffer keyBuffer = ByteBuffer.allocate(64);
        keyBuffer.putShort((short) 255);
        keyBuffer.rewind();

        ByteBuffer valueBuffer = ByteBuffer.allocate(64);

        CoordinatorLoader.UnknownRecordTypeException ex =
            assertThrows(CoordinatorLoader.UnknownRecordTypeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertEquals((short) 255, ex.unknownType());
    }

    @Test
    public void testDeserializeWithKeyEmptyBuffer() {
        ByteBuffer keyBuffer = ByteBuffer.allocate(0);
        ByteBuffer valueBuffer = ByteBuffer.allocate(64);

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertEquals("Could not read version from key's buffer.", ex.getMessage());
    }

    @Test
    public void testDeserializeWithValueEmptyBuffer() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ShareSnapshotKey()
                .setGroupId("foo")
                .setTopicId(Uuid.randomUuid())
                .setPartition(1),
            ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        ByteBuffer valueBuffer = ByteBuffer.allocate(0);

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertEquals("Could not read version from value's buffer.", ex.getMessage());
    }

    @Test
    public void testDeserializeWithInvalidKeyBytes() {
        ByteBuffer keyBuffer = ByteBuffer.allocate(2);
        keyBuffer.putShort((short) 0);
        keyBuffer.rewind();

        ByteBuffer valueBuffer = ByteBuffer.allocate(2);
        valueBuffer.putShort((short) 0);
        valueBuffer.rewind();

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertTrue(ex.getMessage().startsWith("Could not read record with version 0 from key's buffer due to"),
            ex.getMessage());
    }

    @Test
    public void testDeserializeWithInvalidValueBytes() {
        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ShareSnapshotKey()
                .setGroupId("foo")
                .setTopicId(Uuid.randomUuid())
                .setPartition(1),
            ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        ByteBuffer valueBuffer = ByteBuffer.allocate(2);
        valueBuffer.putShort((short) 0);
        valueBuffer.rewind();

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertTrue(ex.getMessage().startsWith("Could not read record with version 0 from value's buffer due to"),
            ex.getMessage());
    }

    @Test
    public void testDeserializeAllRecordTypes() {
        roundTrip((short) 0, new ShareSnapshotKey(), new ShareSnapshotValue());
        roundTrip((short) 1, new ShareUpdateKey(), new ShareUpdateValue());
    }

    private void roundTrip(
        short recordType,
        ApiMessage key,
        ApiMessage val
    ) {
        for (short version = val.lowestSupportedVersion(); version < val.highestSupportedVersion(); version++) {
            ApiMessageAndVersion keyMessageAndVersion = new ApiMessageAndVersion(key, recordType);
            ApiMessageAndVersion valMessageAndVersion = new ApiMessageAndVersion(val, version);

            CoordinatorRecord record = serde.deserialize(
                MessageUtil.toVersionPrefixedByteBuffer(recordType, key),
                MessageUtil.toVersionPrefixedByteBuffer(version, val)
            );

            assertEquals(keyMessageAndVersion, record.key());
            assertEquals(valMessageAndVersion, record.value());
        }
    }

    private static CoordinatorRecord getShareSnapshotRecord(String groupId, Uuid topicId, int partitionId) {
        return new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ShareSnapshotKey()
                    .setGroupId(groupId)
                    .setTopicId(topicId)
                    .setPartition(partitionId),
                ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION
            ),
            new ApiMessageAndVersion(
                new ShareSnapshotValue()
                    .setStartOffset(1L)
                    .setLeaderEpoch(2)
                    .setStateEpoch(1)
                    .setSnapshotEpoch(1)
                    .setStateBatches(Collections.singletonList(new ShareSnapshotValue.StateBatch()
                        .setFirstOffset(1)
                        .setLastOffset(10)
                        .setDeliveryState((byte) 0)
                        .setDeliveryCount((short) 1))),
                (short) 0
            )
        );
    }
}
