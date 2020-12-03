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

import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N
 * followed by N bytes of content.
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final String source;
    private final ByteBuffer sizeBuf;
    private final ByteBuffer minBuf;
    private final int maxSize;
    private final MemoryPool memoryPool;
    private int requestedBufferSize = -1;
    private ByteBuffer payloadBuffer = null;
    private int byteCount = 0;
    private ReadState readState = ReadState.READ_SIZE;

    static enum ReadState {
        READ_SIZE, VALIDATE_SIZE, ALLOCATE_BUFFER, READ_PAYLOAD, COMPLETE;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    public NetworkReceive(String source) {
        this(UNLIMITED, source);
    }

    public NetworkReceive(String source, ByteBuffer buffer) {
        this(source);
        this.payloadBuffer = buffer;
    }

    public NetworkReceive(int maxSize, String source) {
        this(maxSize, source, MemoryPool.NONE);
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;

        this.minBuf =
                (ByteBuffer) ByteBuffer.allocate(SslUtils.SSL_RECORD_HEADER_LENGTH).position(4);
        this.sizeBuf = (ByteBuffer) this.minBuf.duplicate().position(0).limit(4);
    }

    @SuppressWarnings("fallthrough")
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        int read = 0;

        switch (readState) {
            case READ_SIZE:
                read += readRequestedBufferSize(channel);
                if (this.sizeBuf.hasRemaining()) {
                    break;
                }
                this.readState = ReadState.VALIDATE_SIZE;
                /** FALLTHROUGH TO NEXT STATE */
            case VALIDATE_SIZE:
                if (this.requestedBufferSize != 0) {
                    read += validateRequestedBufferSize(channel);
                    if (this.minBuf.hasRemaining()) {
                        break;
                    }
                }
                this.readState = ReadState.ALLOCATE_BUFFER;
                /** FALLTHROUGH TO NEXT STATE */
            case ALLOCATE_BUFFER:
                if (this.requestedBufferSize == 0) {
                    this.payloadBuffer = EMPTY_BUFFER;
                } else {
                    this.payloadBuffer = tryAllocateBuffer(this.requestedBufferSize);
                    if (this.payloadBuffer == null) {
                        break;
                    } else {
                        // Copy any bytes that were already consumed
                        this.minBuf.position(this.sizeBuf.limit());
                        this.payloadBuffer.put(this.minBuf);
                    }
                }
                this.readState = ReadState.READ_PAYLOAD;
                /** FALLTHROUGH TO NEXT STATE */
            case READ_PAYLOAD:
                final int payloadRead = channel.read(payloadBuffer);
                if (payloadRead < 0)
                    throw new EOFException();
                read += payloadRead;
                if (!this.payloadBuffer.hasRemaining()) {
                    this.readState = ReadState.COMPLETE;
                }
                break;
            case COMPLETE:
                break;
        }

        this.byteCount += read;

        return read;
    }

    private int validateRequestedBufferSize(final ScatteringByteChannel channel)
            throws IOException {
        int minRead = channel.read(this.minBuf);
        if (minRead < 0) {
            throw new EOFException();
        }
        if (!this.minBuf.hasRemaining()) {
            final boolean isEncrypted =
                    SslUtils.isEncrypted((ByteBuffer) this.minBuf.duplicate().rewind());
            if (isEncrypted) {
                throw new InvalidReceiveException(
                        "Recieved an unexpected SSL packet from the server. "
                                + "Please ensure the client is properly configured with SSL enabled.");
            }
            if (this.requestedBufferSize < 0)
                throw new InvalidReceiveException(
                        "Invalid receive (size = " + this.requestedBufferSize + ")");
            if (maxSize != UNLIMITED && this.requestedBufferSize > maxSize)
                throw new InvalidReceiveException("Invalid receive (size = "
                        + this.requestedBufferSize + " larger than " + maxSize + ")");
        }

        return minRead;
    }

    private ByteBuffer tryAllocateBuffer(final int bufSize) {
        final ByteBuffer bb = memoryPool.tryAllocate(bufSize);
        if (bb == null) {
            log.trace("Broker low on memory - could not allocate buffer of size {} for source {}",
                    requestedBufferSize, source);
        }
        return bb;
    }

    private int readRequestedBufferSize(final ReadableByteChannel channel) throws IOException {
        final int sizeRead = channel.read(sizeBuf);
        if (sizeRead < 0) {
            throw new EOFException();
        }
        if (sizeBuf.hasRemaining()) {
            return sizeRead;
        }
        sizeBuf.rewind();
        this.requestedBufferSize = sizeBuf.getInt();
        return sizeRead;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return this.readState.ordinal() > ReadState.VALIDATE_SIZE.ordinal();
    }

    @Override
    public boolean memoryAllocated() {
        return this.readState.ordinal() >= ReadState.READ_PAYLOAD.ordinal();
    }

    @Override
    public boolean complete() {
        return this.readState == ReadState.COMPLETE;
    }

    @Override
    public void close() throws IOException {
        if (payloadBuffer != null && payloadBuffer != EMPTY_BUFFER) {
            memoryPool.release(payloadBuffer);
            payloadBuffer = null;
        }
    }

    @Override
    public String source() {
        return source;
    }

    public ByteBuffer payload() {
        return this.payloadBuffer;
    }

    public int bytesRead() {
        return this.byteCount;
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     */
    public int size() {
        return this.payloadBuffer.limit() + sizeBuf.limit();
    }

}
