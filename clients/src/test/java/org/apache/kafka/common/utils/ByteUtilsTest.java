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
package org.apache.kafka.common.utils;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class ByteUtilsTest {

    @Test
    public void testVarIntSerde() {
        assertVarIntSerde(0, 1);
        assertVarIntSerde(-1, 1);
        assertVarIntSerde(1, 1);
        assertVarIntSerde(Integer.MAX_VALUE, 5);
        assertVarIntSerde(Integer.MIN_VALUE, 5);
    }

    @Test
    public void testVarUnsignedIntSerde() {
        assertVarUnsignedIntSerde(0, 1);
        assertVarUnsignedIntSerde(1, 1);
        assertVarUnsignedIntSerde(Integer.MAX_VALUE * 2L, 5);
    }

    @Test
    public void testVarLongSerde() {
        assertVarLongSerde(0L, 1);
        assertVarLongSerde(-1L, 1);
        assertVarLongSerde(1L, 1);
        assertVarLongSerde(Integer.MAX_VALUE, 5);
        assertVarLongSerde(Integer.MIN_VALUE, 5);
        assertVarLongSerde(Long.MAX_VALUE, 10);
        assertVarLongSerde(Long.MIN_VALUE, 10);
    }

    private void assertVarIntSerde(int value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarInt(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarIntEncoding(value));
        assertEquals(value, ByteUtils.readVarInt(buf));
    }

    private void assertVarUnsignedIntSerde(long value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarUnsignedInt(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarUnsignedIntEncoding(value));
        assertEquals(value, ByteUtils.readVarUnsignedInt(buf));
    }

    private void assertVarLongSerde(long value, int expectedLength) {
        ByteBuffer buf = ByteBuffer.allocate(32);
        ByteUtils.writeVarLong(value, buf);
        buf.flip();
        assertEquals(expectedLength, buf.remaining());
        assertEquals(expectedLength, ByteUtils.bytesForVarLongEncoding(value));
        assertEquals(value, ByteUtils.readVarLong(buf));
    }

}
