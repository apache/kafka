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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionCoordinatorRecordSerdeTest {

    @Test
    public void testSerializeKey() {
        TransactionCoordinatorRecordSerde serializer = new TransactionCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
                new ApiMessageAndVersion(
                        new TransactionLogKey().setTransactionalId("txnId"),
                        (short) 0
                ),
                new ApiMessageAndVersion(
                        new TransactionLogValue(),
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
        TransactionCoordinatorRecordSerde serializer = new TransactionCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
                new ApiMessageAndVersion(
                        new TransactionLogKey().setTransactionalId("txnId"),
                        (short) 0
                ),
                new ApiMessageAndVersion(
                        new TransactionLogValue(),
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
        TransactionCoordinatorRecordSerde serializer = new TransactionCoordinatorRecordSerde();
        CoordinatorRecord record = new CoordinatorRecord(
                new ApiMessageAndVersion(
                        new TransactionLogKey().setTransactionalId("txnId"),
                        (short) 0
                ),
                null
        );

        assertNull(serializer.serializeValue(record));
    }

    @Test
    public void testDeserialize() {
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
                new TransactionLogKey().setTransactionalId("txnId"),
                (short) 0
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        ApiMessageAndVersion value = new ApiMessageAndVersion(
                new TransactionLogValue(),
                (short) 0
        );
        ByteBuffer valueBuffer = MessageUtil.toVersionPrefixedByteBuffer(value.version(), value.message());

        CoordinatorRecord record = serde.deserialize(keyBuffer, valueBuffer);
        assertEquals(key, record.key());
        assertEquals(value, record.value());
    }

    @Test
    public void testDeserializeWithTombstoneForValue() {
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
                new TransactionLogKey().setTransactionalId("txnId"),
                (short) 0
        );
        ByteBuffer keyBuffer = MessageUtil.toVersionPrefixedByteBuffer(key.version(), key.message());

        CoordinatorRecord record = serde.deserialize(keyBuffer, null);
        assertEquals(key, record.key());
        assertNull(record.value());
    }

    @Test
    public void testDeserializeWithInvalidRecordType() {
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

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
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

        ByteBuffer keyBuffer = ByteBuffer.allocate(0);
        ByteBuffer valueBuffer = ByteBuffer.allocate(64);

        RuntimeException ex =
                assertThrows(RuntimeException.class,
                        () -> serde.deserialize(keyBuffer, valueBuffer));
        assertEquals("Could not read version from key's buffer.", ex.getMessage());
    }

    @Test
    public void testDeserializeWithValueEmptyBuffer() {
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
                new TransactionLogKey().setTransactionalId("txnId"),
                (short) 0
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
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

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
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

        ApiMessageAndVersion key = new ApiMessageAndVersion(
                new TransactionLogKey().setTransactionalId("txnId"),
                (short) 0
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
        roundTrip((short) 0, new TransactionLogKey().setTransactionalId("id"), new TransactionLogValue());
    }

    private void roundTrip(
            short recordType,
            ApiMessage key,
            ApiMessage val
    ) {
        TransactionCoordinatorRecordSerde serde = new TransactionCoordinatorRecordSerde();

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
