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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.message.ControlRecordTypeSchema;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControlRecordTypeTest {

    // Old hard-coded schema, used to validate old hard-coded schema format is exactly the same as new auto generated protocol format
    private final Schema v0Schema = new Schema(
            new Field("version", Type.INT16),
            new Field("type", Type.INT16));

    @Test
    public void testParseUnknownType() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putShort(ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION);
        buffer.putShort((short) 337);
        buffer.flip();
        ControlRecordType type = ControlRecordType.parse(buffer);
        assertEquals(ControlRecordType.UNKNOWN, type);
    }

    @Test
    public void testParseUnknownVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putShort((short) 5);
        buffer.putShort(ControlRecordType.ABORT.type());
        buffer.putInt(23432); // some field added in version 5
        buffer.flip();
        ControlRecordType type = ControlRecordType.parse(buffer);
        assertEquals(ControlRecordType.ABORT, type);
    }

    @ParameterizedTest
    @EnumSource(value = ControlRecordType.class)
    public void testRoundTrip(ControlRecordType expected) {
        if (expected == ControlRecordType.UNKNOWN) {
            return;
        }
        for (short version = ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION;
             version <= ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION; version++) {
            ByteBuffer buffer = expected.recordKey();
            ControlRecordType deserializedKey = ControlRecordType.parse(buffer);
            assertEquals(expected, deserializedKey);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ControlRecordType.class)
    public void testValueControlRecordKeySize(ControlRecordType type) {
        for (short version = ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION;
             version <= ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION; version++) {
            assertEquals(4, type.controlRecordKeySize());
        }
    }

    @ParameterizedTest
    @EnumSource(value = ControlRecordType.class)
    public void testBackwardDeserializeCompatibility(ControlRecordType type) {
        for (short version = ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION;
             version <= ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION; version++) {
            Struct struct = new Struct(v0Schema);
            struct.set("version", version);
            struct.set("type", type.type());

            ByteBuffer oldVersionBuffer = ByteBuffer.allocate(struct.sizeOf());
            struct.writeTo(oldVersionBuffer);
            oldVersionBuffer.flip();

            ControlRecordType deserializedType = ControlRecordType.parse(oldVersionBuffer);
            assertEquals(type, deserializedType);
        }
    }

    @ParameterizedTest
    @EnumSource(value = ControlRecordType.class)
    public void testForwardDeserializeCompatibility(ControlRecordType type) {
        if (type == ControlRecordType.UNKNOWN) {
            return;
        }
        for (short version = ControlRecordTypeSchema.LOWEST_SUPPORTED_VERSION;
             version <= ControlRecordTypeSchema.HIGHEST_SUPPORTED_VERSION; version++) {
            ByteBuffer newVersionBuffer = type.recordKey();

            Struct struct = v0Schema.read(newVersionBuffer);

            ControlRecordType deserializedType = ControlRecordType.fromTypeId(struct.getShort("type"));
            assertEquals(type, deserializedType);
        }
    }
}
