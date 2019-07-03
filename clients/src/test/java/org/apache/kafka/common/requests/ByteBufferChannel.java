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
package org.apache.kafka.common.requests;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class ByteBufferChannel implements GatheringByteChannel {
    private final ByteBuffer buf;
    private boolean closed = false;

    public ByteBufferChannel(long size) {
        if (size > Integer.MAX_VALUE)
            throw new IllegalArgumentException("size should be not be greater than Integer.MAX_VALUE");
        this.buf = ByteBuffer.allocate((int) size);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        int position = buf.position();
        for (int i = 0; i < length; i++) {
            ByteBuffer src = srcs[i].duplicate();
            if (i == 0)
                src.position(offset);
            buf.put(src);
        }
        return buf.position() - position;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int position = buf.position();
        buf.put(src);
        return buf.position() - position;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        buf.flip();
        closed = true;
    }

    public ByteBuffer buffer() {
        return buf;
    }
}
