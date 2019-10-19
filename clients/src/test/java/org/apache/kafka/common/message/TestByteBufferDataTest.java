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

import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestByteBufferDataTest {
    @Test
    public void shouldUseByteBufferFieldInSchema() {
        Assert.assertEquals(Type.BYTE_BUFFER, TestByteBufferData.SCHEMA_1.get("record").def.type);
    }

    @Test
    public void shouldStoreField() {
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final TestByteBufferData out = new TestByteBufferData();
        out.setRecord(buf);
        out.setTest("test");

        Assert.assertEquals(buf, out.record());
        Assert.assertEquals("test", out.test());
    }

    @Test
    public void shouldDefaultField() {
        final TestByteBufferData out = new TestByteBufferData();
        Assert.assertEquals(ByteUtils.EMPTY_BUF, out.record());
    }

    @Test
    public void shouldRoundTripFieldThroughStruct() {
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final TestByteBufferData out = new TestByteBufferData();
        out.setRecord(buf);
        out.setTest("test");

        final Struct struct = out.toStruct((short) 1);
        final TestByteBufferData in = new TestByteBufferData();
        in.fromStruct(struct, (short) 1);

        Assert.assertEquals(buf, in.record());
        Assert.assertEquals("test", in.test());
    }

    @Test
    public void shouldRoundTripFieldThroughBuffer() {
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final TestByteBufferData out = new TestByteBufferData();
        out.setRecord(buf);
        out.setTest("test");

        ObjectSerializationCache cache = new ObjectSerializationCache();

        final ByteBuffer buffer = ByteBuffer.allocate(out.size(cache, (short) 1));
        out.write(new ByteBufferAccessor(buffer), cache, (short) 1);

        buffer.rewind();
        buf.rewind();

        final TestByteBufferData in = new TestByteBufferData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        Assert.assertEquals(buf, in.record());
        Assert.assertEquals("test", in.test());
    }

    @Test
    public void shouldImplementJVMMethods() {
        final ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
        final TestByteBufferData a = new TestByteBufferData();
        a.setRecord(buf);
        a.setTest("test");

        final TestByteBufferData b = new TestByteBufferData();
        b.setRecord(buf);
        b.setTest("test");

        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertEquals(a.toString(), b.toString());
    }
}
