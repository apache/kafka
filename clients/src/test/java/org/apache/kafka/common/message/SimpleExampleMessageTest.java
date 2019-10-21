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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class SimpleExampleMessageTest {

    @Test
    public void shouldStoreField() {
        final UUID uuid = UUID.randomUUID();
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);
        assertEquals(uuid, out.processId());
    }

    @Test
    public void shouldDefaultField() {
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000000"), out.processId());
    }

    @Test
    public void shouldRoundTripFieldThroughStruct() {
        final UUID uuid = UUID.randomUUID();
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);

        final Struct struct = out.toStruct((short) 1);
        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.fromStruct(struct, (short) 1);

        assertEquals(uuid, in.processId());
    }

    @Test
    public void shouldRoundTripFieldThroughBuffer() {
        final UUID uuid = UUID.randomUUID();
        final SimpleExampleMessageData out = new SimpleExampleMessageData();
        out.setProcessId(uuid);

        ObjectSerializationCache cache = new ObjectSerializationCache();
        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.rewind();

        final SimpleExampleMessageData in = new SimpleExampleMessageData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        assertEquals(uuid, in.processId());
    }

    @Test
    public void shouldImplementJVMMethods() {
        final UUID uuid = UUID.randomUUID();
        final SimpleExampleMessageData a = new SimpleExampleMessageData();
        a.setProcessId(uuid);

        final SimpleExampleMessageData b = new SimpleExampleMessageData();
        b.setProcessId(uuid);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        // just tagging this on here
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
