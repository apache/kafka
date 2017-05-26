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

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A ByteBuffer-backed OutputStream
 */
public class ByteBufferOutputStream extends OutputStream {

    private static final float REALLOCATION_FACTOR = 1.1f;

    private ByteBuffer buffer;
    private int initialPosition;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        this.initialPosition = buffer.position();
    }

    public void write(int b) {
        if (buffer.remaining() < 1)
            expandBuffer(buffer.capacity() + 1);
        buffer.put((byte) b);
    }

    public void write(byte[] bytes, int off, int len) {
        if (buffer.remaining() < len)
            expandBuffer(buffer.position() + len);
        buffer.put(bytes, off, len);
    }

    public void write(ByteBuffer buffer) {
        if (buffer.hasArray())
            write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        else {
            int pos = buffer.position();
            for (int i = pos, limit = buffer.remaining() + pos; i < limit; i++)
                write(buffer.get(i));
        }
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public int position() {
        return buffer.position();
    }

    public int capacity() {
        return buffer.capacity();
    }

    public int limit() {
        return buffer.limit();
    }

    public void position(int position) {
        if (position > buffer.limit())
            expandBuffer(position);
        buffer.position(position);
    }

    private void expandBuffer(int size) {
        int expandSize = Math.max((int) (buffer.capacity() * REALLOCATION_FACTOR), size);
        ByteBuffer temp = ByteBuffer.allocate(expandSize);
        temp.put(buffer.array(), buffer.arrayOffset(), buffer.position());

        // reset the old buffer's position so that
        buffer.position(initialPosition);
        buffer = temp;
    }

}
