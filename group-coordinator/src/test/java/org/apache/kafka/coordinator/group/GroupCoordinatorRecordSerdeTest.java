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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class GroupCoordinatorRecordSerdeTest {
    @Test
    public void testSerializeKey() {
        GroupCoordinatorRecordSerde serializer = new GroupCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 3
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue().setEpoch(10),
                (short) 0
            )
        );

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.key().version(), record.key().message()),
            serializer.serializeKey(record)
        );
    }

    @Test
    public void testSerializeValue() {
        GroupCoordinatorRecordSerde serializer = new GroupCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 3
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue().setEpoch(10),
                (short) 0
            )
        );

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.value().version(), record.value().message()),
            serializer.serializeValue(record)
        );
    }

    @Test
    public void testSerializeNullValue() {
        GroupCoordinatorRecordSerde serializer = new GroupCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey().setGroupId("group"),
                (short) 1
            ),
            null
        );

        assertNull(serializer.serializeValue(record));
    }

    @Test
    public void testDeserialize() {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ConsumerGroupMetadataKey().setGroupId("foo"),
            (short) 3
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        ApiMessageAndVersion value = new ApiMessageAndVersion(
            new ConsumerGroupMetadataValue().setEpoch(10),
            (short) 0
        );
        ByteBuffer valueBuffer = MessageUtil.toVersionPrefixedByteBuffer(value.version(), value.message());

        CoordinatorRecord record = serde.deserialize(keyBuffer, valueBuffer);
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testDeserializeWithTombstoneForValue() {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ConsumerGroupMetadataKey().setGroupId("foo"),
            (short) 3
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        CoordinatorRecord record = serde.deserialize(keyBuffer, null);
        assertEquals(key, record.key());
        assertNull(record.value());
    }

    @Test
    public void testDeserializeWithInvalidRecordType() {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

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
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ByteBuffer keyBuffer = ByteBuffer.allocate(0);
        ByteBuffer valueBuffer = ByteBuffer.allocate(64);

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertEquals("Could not read version from key's buffer.", ex.getMessage());
    }

    @Test
    public void testDeserializeWithValueEmptyBuffer() {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ConsumerGroupMetadataKey().setGroupId("foo"),
            (short) 3
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
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ByteBuffer keyBuffer = ByteBuffer.allocate(2);
        keyBuffer.putShort((short) 3);
        keyBuffer.rewind();

        ByteBuffer valueBuffer = ByteBuffer.allocate(2);
        valueBuffer.putShort((short) 0);
        valueBuffer.rewind();

        RuntimeException ex =
            assertThrows(RuntimeException.class,
                () -> serde.deserialize(keyBuffer, valueBuffer));
        assertTrue(ex.getMessage().startsWith("Could not read record with version 3 from key's buffer due to"),
            ex.getMessage());
    }

    @Test
    public void testDeserializeWithInvalidValueBytes() {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
            new ConsumerGroupMetadataKey().setGroupId("foo"),
            (short) 3
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
        roundTrip((short) 0, new OffsetCommitKey(), new OffsetCommitValue());
        roundTrip((short) 1, new OffsetCommitKey(), new OffsetCommitValue());
        roundTrip((short) 2, new GroupMetadataKey(), new GroupMetadataValue());
        roundTrip((short) 3, new ConsumerGroupMetadataKey(), new ConsumerGroupMetadataValue());
        roundTrip((short) 4, new ConsumerGroupPartitionMetadataKey(), new ConsumerGroupPartitionMetadataValue());
        roundTrip((short) 5, new ConsumerGroupMemberMetadataKey(), new ConsumerGroupMemberMetadataValue());
        roundTrip((short) 6, new ConsumerGroupTargetAssignmentMetadataKey(), new ConsumerGroupTargetAssignmentMetadataValue());
        roundTrip((short) 7, new ConsumerGroupTargetAssignmentMemberKey(), new ConsumerGroupTargetAssignmentMemberValue());
        roundTrip((short) 8, new ConsumerGroupCurrentMemberAssignmentKey(), new ConsumerGroupCurrentMemberAssignmentValue());
        roundTrip((short) 9, new ShareGroupPartitionMetadataKey(), new ShareGroupPartitionMetadataValue());
        roundTrip((short) 10, new ShareGroupMemberMetadataKey(), new ShareGroupMemberMetadataValue());
        roundTrip((short) 11, new ShareGroupMetadataKey(), new ShareGroupMetadataValue());
        roundTrip((short) 12, new ShareGroupTargetAssignmentMetadataKey(), new ShareGroupTargetAssignmentMetadataValue());
        roundTrip((short) 13, new ShareGroupTargetAssignmentMemberKey(), new ShareGroupTargetAssignmentMemberValue());
        roundTrip((short) 14, new ShareGroupCurrentMemberAssignmentKey(), new ShareGroupCurrentMemberAssignmentValue());
        roundTrip((short) 15, new ShareGroupStatePartitionMetadataKey(), new ShareGroupStatePartitionMetadataValue());
        roundTrip((short) 16, new ConsumerGroupRegularExpressionKey(), new ConsumerGroupRegularExpressionValue());
    }

    private void roundTrip(
        short recordType,
        ApiMessage key,
        ApiMessage val
    ) {
        GroupCoordinatorRecordSerde serde = new GroupCoordinatorRecordSerde();

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
}
