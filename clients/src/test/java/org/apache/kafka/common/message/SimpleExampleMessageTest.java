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
import org.apache.kafka.common.protocol.MessageTestUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SimpleExampleMessageTest {

    @Test
    public void shouldStoreField() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});

        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        assertEquals(uuid, out.processId());
        assertEquals(buf, out.zeroCopyByteBuffer());

        out.setNullableZeroCopyByteBuffer(null);
        assertNull(out.nullableZeroCopyByteBuffer());
        out.setNullableZeroCopyByteBuffer(buf);
        assertEquals(buf, out.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldDefaultField() {
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000000"), out.processId());
        assertEquals(ByteUtils.EMPTY_BUF, out.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, out.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughStruct() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        final Struct struct = out.toStruct((short) 1);
        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.fromStruct(struct, (short) 1);

        buf.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf, in.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughStructWithNullable() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final ByteBuffer buf2 = ByteBuffer.wrap(new byte[] {4, 5, 6});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf1);
        out.setNullableZeroCopyByteBuffer(buf2);

        final Struct struct = out.toStruct((short) 1);
        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.fromStruct(struct, (short) 1);

        buf1.rewind();
        buf2.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf1, in.zeroCopyByteBuffer());
        assertEquals(buf2, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughBuffer() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.rewind();

        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        buf.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf, in.zeroCopyByteBuffer());
        assertEquals(ByteUtils.EMPTY_BUF, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldRoundTripFieldThroughBufferWithNullable() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf1 = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final ByteBuffer buf2 = ByteBuffer.wrap(new byte[] {4, 5, 6});
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        out.setZeroCopyByteBuffer(buf1);
        out.setNullableZeroCopyByteBuffer(buf2);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.rewind();

        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        buf1.rewind();
        buf2.rewind();

        assertEquals(uuid, in.processId());
        assertEquals(buf1, in.zeroCopyByteBuffer());
        assertEquals(buf2, in.nullableZeroCopyByteBuffer());
    }

    @Test
    public void shouldImplementJVMMethods() {
        final UUID uuid = UUID.randomUUID();
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final SimpleExampleMessageData a = new SimpleExampleMessageData();
        a.setProcessId(uuid);
        a.setZeroCopyByteBuffer(buf);

        final SimpleExampleMessageData b = new SimpleExampleMessageData();
        b.setProcessId(uuid);
        b.setZeroCopyByteBuffer(buf);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        // just tagging this on here
        assertEquals(a.toString(), b.toString());

        a.setNullableZeroCopyByteBuffer(buf);
        b.setNullableZeroCopyByteBuffer(buf);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());

        a.setNullableZeroCopyByteBuffer(null);
        b.setNullableZeroCopyByteBuffer(null);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());
    }

    @Test
    public void testMyTaggedIntArray() {
        final SimpleExampleMessageData data = new SimpleExampleMessageData();
        data.setMyTaggedIntArray(Arrays.asList(1, 2, 3));
        short version = 1;
        ByteBuffer buf = MessageTestUtil.messageToByteBuffer(data, version);
        final SimpleExampleMessageData data2 = new SimpleExampleMessageData();
        data2.read(new ByteBufferAccessor(buf.duplicate()), version);
        assertEquals(Arrays.asList(1, 2, 3), data.myTaggedIntArray());
        assertEquals(Arrays.asList(1, 2, 3), data2.myTaggedIntArray());
        assertEquals(data, data2);
    }
}
