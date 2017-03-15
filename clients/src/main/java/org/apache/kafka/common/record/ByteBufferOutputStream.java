/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A byte buffer backed output outputStream
 */
public class ByteBufferOutputStream extends DataOutputStream {

    private static final float REALLOCATION_FACTOR = 1.1f;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        super(new UnderlyingOutputStream(buffer));
    }

    public ByteBuffer buffer() {
        return ((UnderlyingOutputStream) out).buffer;
    }

    public static class UnderlyingOutputStream extends OutputStream {
        private ByteBuffer buffer;

        public UnderlyingOutputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public void write(int b) {
            if (buffer.remaining() < 1)
                expandBuffer(buffer.capacity() + 1);
            buffer.put((byte) b);
        }

        public void write(byte[] bytes, int off, int len) {
            if (buffer.remaining() < len)
                expandBuffer(buffer.capacity() + len);
            buffer.put(bytes, off, len);
        }

        public ByteBuffer buffer() {
            return buffer;
        }

        private void expandBuffer(int size) {
            int expandSize = Math.max((int) (buffer.capacity() * REALLOCATION_FACTOR), size);
            ByteBuffer temp = ByteBuffer.allocate(expandSize);
            temp.put(buffer.array(), buffer.arrayOffset(), buffer.position());
            buffer = temp;
        }
    }

}
