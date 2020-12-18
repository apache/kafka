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
package org.apache.kafka.common.record;

import org.apache.kafka.common.message.EndTxnMarker;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EndTransactionMarkerTest {

    // Old hard-coded schema, used to validate old hard-coded schema format is exactly the same as new auto generated protocol format
    private Schema v0Schema = new Schema(
            new Field("version", Type.INT16),
            new Field("coordinator_epoch", Type.INT32));

    private static final List<ControlRecordType> VALID_CONTROLLER_RECORD_TYPE = Arrays.asList(ControlRecordType.COMMIT, ControlRecordType.ABORT);

    @Test
    public void testUnknownControlTypeNotAllowed() {
        assertThrows(IllegalArgumentException.class,
            () -> new EndTransactionMarker(ControlRecordType.UNKNOWN, 24));
    }

    @Test
    public void testCannotDeserializeUnknownControlType() {
        assertThrows(IllegalArgumentException.class,
            () -> EndTransactionMarker.deserializeValue(ControlRecordType.UNKNOWN, ByteBuffer.wrap(new byte[0])));
    }

    @Test
    public void testSerde() {
        int coordinatorEpoch = 79;
        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        ByteBuffer buffer = marker.serializeValue();
        EndTransactionMarker deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer);
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch());
    }

    @Test
    public void testDeserializeNewerVersion() {
        int coordinatorEpoch = 79;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putShort((short) 5);
        buffer.putInt(coordinatorEpoch);
        buffer.putShort((short) 0); // unexpected data
        buffer.flip();
        EndTransactionMarker deserialized = EndTransactionMarker.deserializeValue(ControlRecordType.COMMIT, buffer);
        assertEquals(coordinatorEpoch, deserialized.coordinatorEpoch());
    }

    @Test
    public void testSerializeAndDeserialize() {
        for (ControlRecordType type: VALID_CONTROLLER_RECORD_TYPE) {
            for (short version = EndTxnMarker.LOWEST_SUPPORTED_VERSION;
                 version <= EndTxnMarker.HIGHEST_SUPPORTED_VERSION; version++) {
                EndTransactionMarker marker = new EndTransactionMarker(type, 1);

                ByteBuffer buffer = marker.serializeValue();
                EndTransactionMarker deserializedMarker = EndTransactionMarker.deserializeValue(type, buffer);
                assertEquals(marker, deserializedMarker);
            }
        }
    }

    @Test
    public void testBackwardDeserializeCompatibility() {
        int coordinatorEpoch = 10;
        for (ControlRecordType type: VALID_CONTROLLER_RECORD_TYPE) {
            for (short version = EndTxnMarker.LOWEST_SUPPORTED_VERSION;
                 version <= EndTxnMarker.HIGHEST_SUPPORTED_VERSION; version++) {

                Struct struct = new Struct(v0Schema);
                struct.set("version", version);
                struct.set("coordinator_epoch", coordinatorEpoch);

                ByteBuffer oldVersionBuffer = ByteBuffer.allocate(struct.sizeOf());
                struct.writeTo(oldVersionBuffer);
                oldVersionBuffer.flip();

                EndTransactionMarker deserializedMarker = EndTransactionMarker.deserializeValue(type, oldVersionBuffer);
                assertEquals(coordinatorEpoch, deserializedMarker.coordinatorEpoch());
                assertEquals(type, deserializedMarker.controlType());
            }
        }
    }

    @Test
    public void testForwardDeserializeCompatibility() {
        int coordinatorEpoch = 10;
        for (ControlRecordType type: VALID_CONTROLLER_RECORD_TYPE) {
            for (short version = EndTxnMarker.LOWEST_SUPPORTED_VERSION;
                 version <= EndTxnMarker.HIGHEST_SUPPORTED_VERSION; version++) {
                EndTransactionMarker marker = new EndTransactionMarker(type, coordinatorEpoch);
                ByteBuffer newVersionBuffer = marker.serializeValue();

                Struct struct = v0Schema.read(newVersionBuffer);
                EndTransactionMarker deserializedMarker = new EndTransactionMarker(type, struct.getInt("coordinator_epoch"));
                assertEquals(marker, deserializedMarker);
            }
        }
    }
}
