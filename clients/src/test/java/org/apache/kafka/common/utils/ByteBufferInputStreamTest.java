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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteBufferInputStreamTest {

    @Test
    public void testReadUnsignedIntFromInputStream() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put((byte) 10);
        buffer.put((byte) 20);
        buffer.put((byte) 30);
        buffer.flip(); // prepare for reading

        byte[] b = new byte[6];

        ByteBufferInputStream inputStream = new ByteBufferInputStream(buffer);
        assertEquals(3, inputStream.available());
        // read two bytes
        assertEquals(10, inputStream.read());
        assertEquals(20, inputStream.read());

        // try to read 3 bytes but only able to read one
        assertEquals(1, inputStream.read(b, 3, b.length - 3));
        // all bytes have been read, no more data to read, return -1
        assertEquals(-1, inputStream.read());

        // rewind input and prepare for read again
        buffer.rewind();

        // read 3 bytes
        assertEquals(3, inputStream.read(b, 0, b.length));
        // all bytes have been read, no more data to read, return -1
        assertEquals(-1, inputStream.read(b, 0, b.length));

        // read 0 bytes
        assertEquals(0, inputStream.read(b, 0, 0));
        assertEquals(-1, inputStream.read());
    }
}
