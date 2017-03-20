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

public class Crc32Test {

    @Test
    public void testUpdateByteBuffer() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5};

        Crc32 byteArrayCrc = new Crc32();
        byteArrayCrc.update(bytes, 0, bytes.length);
        long byteArrayCrcValue = byteArrayCrc.getValue();

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        Crc32 bufferCrc = new Crc32();
        bufferCrc.update(buffer, buffer.remaining());
        long bufferCrcValue = bufferCrc.getValue();

        ByteBuffer directBuffer = ByteBuffer.allocateDirect(bytes.length);
        directBuffer.put(bytes);
        directBuffer.flip();

        Crc32 directBufferCrc = new Crc32();
        directBufferCrc.update(directBuffer, directBuffer.remaining());
        long directBufferCrcValue = directBufferCrc.getValue();

        assertEquals(byteArrayCrcValue, bufferCrcValue);
        assertEquals(byteArrayCrcValue, directBufferCrcValue);
    }

}