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
package org.apache.kafka.common.network;

import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NetworkReceiveTest {

    @Test
    public void testBytesRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");
        assertEquals(0, receive.bytesRead());

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(128);
            return 4;
        }).thenReturn(0);

        assertEquals(4, receive.readFrom(channel));
        assertEquals(4, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(68, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(132, receive.bytesRead());
        assertTrue(receive.complete());
    }

}
