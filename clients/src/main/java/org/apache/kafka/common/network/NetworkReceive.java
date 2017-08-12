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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private final MemoryPool memoryPool;
    private int requestedBufferSize = -1;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer);
            buffer = null;
        }
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        int read = 0;
        if (size.hasRemaining()) {
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }
        if (buffer == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
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
