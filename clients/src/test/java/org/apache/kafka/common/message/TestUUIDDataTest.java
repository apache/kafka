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
import org.apache.kafka.common.protocol.types.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

public class TestUUIDDataTest {

    @Test
    public void shouldStoreField() {
        final UUID uuid = UUID.randomUUID();
        final TestUUIDData out = new TestUUIDData();
        out.setProcessId(uuid);
        Assert.assertEquals(uuid, out.processId());
    }

    @Test
    public void shouldDefaultField() {
        final TestUUIDData out = new TestUUIDData();
        Assert.assertEquals(UUID.fromString("00000000-0000-0000-0000-000000000000"), out.processId());
    }

    @Test
    public void shouldRoundTripFieldThroughStruct() {
        final UUID uuid = UUID.randomUUID();
        final TestUUIDData out = new TestUUIDData();
        out.setProcessId(uuid);

        final Struct struct = out.toStruct((short) 1);
        final TestUUIDData in = new TestUUIDData();
        in.fromStruct(struct, (short) 1);

        Assert.assertEquals(uuid, in.processId());
    }

    @Test
    public void shouldRoundTripFieldThroughBuffer() {
        final UUID uuid = UUID.randomUUID();
        final TestUUIDData out = new TestUUIDData();
        out.setProcessId(uuid);

        final ByteBuffer buffer = ByteBuffer.allocate(out.size((short) 1));
        out.write(new ByteBufferAccessor(buffer), (short) 1);
        buffer.rewind();

        final TestUUIDData in = new TestUUIDData();
        in.read(new ByteBufferAccessor(buffer), (short) 1);

        Assert.assertEquals(uuid, in.processId());
    }

    @Test
    public void shouldImplementJVMMethods() {
        final UUID uuid = UUID.randomUUID();
        final TestUUIDData a = new TestUUIDData();
        a.setProcessId(uuid);

        final TestUUIDData b = new TestUUIDData();
        b.setProcessId(uuid);

        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        // just tagging this on here
        Assert.assertEquals(a.toString(), b.toString());
    }
}
