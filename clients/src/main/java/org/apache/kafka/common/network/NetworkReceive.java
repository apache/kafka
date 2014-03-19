/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    private final int source;
    private final ByteBuffer size;
    private ByteBuffer buffer;

    public NetworkReceive(int source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
    }

    public NetworkReceive(int source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
    }

    @Override
    public int source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    @Override
    public ByteBuffer[] reify() {
        return new ByteBuffer[] { this.buffer };
    }

    @Override
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        int read = 0;
        if (size.hasRemaining()) {
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                int requestSize = size.getInt();
                if (requestSize < 0)
                    throw new IllegalStateException("Invalid request (size = " + requestSize + ")");
                this.buffer = ByteBuffer.allocate(requestSize);
            }
        }
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

}
