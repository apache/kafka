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

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;

public class RecordsWritableTest {
    @Test
    public void testBufferSlice() {
        Queue<Send> sends = new ArrayDeque<>();
        RecordsWritable writer = new RecordsWritable("dest", 10000 /* enough for tests */, sends::add);
        for (int i = 0; i < 4; i++) {
            writer.writeInt(i);
        }
        writer.flush();
        Assert.assertEquals(sends.size(), 1);
        ByteBufferSend send = (ByteBufferSend) sends.remove();
        Assert.assertEquals(send.size(), 16);
        Assert.assertEquals(send.remaining(), 16);

        // No new data, flush shouldn't do anything
        writer.flush();
        Assert.assertEquals(sends.size(), 0);

        // Cause the buffer to expand a few times
        for (int i = 0; i < 100; i++) {
            writer.writeInt(i);
        }
        writer.flush();
        Assert.assertEquals(sends.size(), 1);
        send = (ByteBufferSend) sends.remove();
        Assert.assertEquals(send.size(), 400);
        Assert.assertEquals(send.remaining(), 400);

        writer.writeByte((byte) 5);
        writer.flush();
        Assert.assertEquals(sends.size(), 1);
        send = (ByteBufferSend) sends.remove();
        Assert.assertEquals(send.size(), 1);
        Assert.assertEquals(send.remaining(), 1);
    }
}
