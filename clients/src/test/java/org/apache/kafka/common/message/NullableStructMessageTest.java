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
package org.apache.kafka.common.message;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NullableStructMessageTest {

    @Test
    public void testDefaultValues() {
        NullableStructMessageData message = new NullableStructMessageData();
        assertNull(message.nullableStruct);
        assertEquals(new NullableStructMessageData.MyStruct2(), message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertEquals(new NullableStructMessageData.MyStruct4(), message.nullableStruct4);

        message = roundTrip(message, (short) 2);
        assertNull(message.nullableStruct);
        assertEquals(new NullableStructMessageData.MyStruct2(), message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertEquals(new NullableStructMessageData.MyStruct4(), message.nullableStruct4);
    }

    @Test
    public void testRoundTrip() {
        NullableStructMessageData message = new NullableStructMessageData()
            .setNullableStruct(new NullableStructMessageData.MyStruct()
                .setMyInt(1)
                .setMyString("1"))
            .setNullableStruct2(new NullableStructMessageData.MyStruct2()
                .setMyInt(2)
                .setMyString("2"))
            .setNullableStruct3(new NullableStructMessageData.MyStruct3()
                .setMyInt(3)
                .setMyString("3"))
            .setNullableStruct4(new NullableStructMessageData.MyStruct4()
                .setMyInt(4)
                .setMyString("4"));

        NullableStructMessageData newMessage = roundTrip(message, (short) 2);
        assertEquals(message, newMessage);
    }

    @Test
    public void testNullForAllFields() {
        NullableStructMessageData message = new NullableStructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null);

        message = roundTrip(message, (short) 2);
        assertNull(message.nullableStruct);
        assertNull(message.nullableStruct2);
        assertNull(message.nullableStruct3);
        assertNull(message.nullableStruct4);
    }

    @Test
    public void testNullableStruct2CanNotBeNullInVersion0() {
        NullableStructMessageData message = new NullableStructMessageData()
            .setNullableStruct2(null);

        assertThrows(NullPointerException.class, () -> roundTrip(message, (short) 0));
    }

    @Test
    public void testToStringWithNullStructs() {
        NullableStructMessageData message = new NullableStructMessageData()
            .setNullableStruct(null)
            .setNullableStruct2(null)
            .setNullableStruct3(null)
            .setNullableStruct4(null);

        message.toString();
    }

    private NullableStructMessageData deserialize(ByteBuffer buf, short version) {
        NullableStructMessageData message = new NullableStructMessageData();
        message.read(new ByteBufferAccessor(buf.duplicate()), version);
        return message;
    }

    private ByteBuffer serialize(NullableStructMessageData message, short version) {
        return MessageUtil.toByteBuffer(message, version);
    }

    private NullableStructMessageData roundTrip(NullableStructMessageData message, short version) {
        ByteBuffer buffer = serialize(message, version);
        return deserialize(buffer.duplicate(), version);
    }
}
