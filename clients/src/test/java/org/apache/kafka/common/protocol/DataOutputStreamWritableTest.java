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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

public class DataOutputStreamWritableTest {
    @Test
    public void testWritingSlicedByteBuffer() {
        ByteBuffer expectedBuffer = ByteBuffer.wrap(new byte[]{2, 3});
        ByteBuffer originalBuffer = ByteBuffer.allocate(4);
        for (int i = 0; i < originalBuffer.limit(); i++) {
            originalBuffer.put(i, (byte) i);
        }
        // Move position forward to ensure slice is not whole buffer
        originalBuffer.position(2);
        ByteBuffer slicedBuffer = originalBuffer.slice();

        ByteBuffer actualBuf = ByteBuffer.allocate(2);
        Writable writable = new DataOutputStreamWritable(new DataOutputStream(new ByteBufferOutputStream(actualBuf)));

        writable.writeByteBuffer(slicedBuffer);

        assertEquals(2, actualBuf.position(), "Writing to the buffer moves the position forward");
        assertEquals(expectedBuffer, actualBuf.position(0),
                "Actual buffer should have expected elements");
    }
}