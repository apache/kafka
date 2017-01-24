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

import org.apache.kafka.common.utils.Utils;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.regex.Pattern;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;

    private boolean ensureThrough = false;
    private ByteBuffer tempOverBuf;
    /**
     * Supporting TLSv1, TLSv1.1 and TLSv1.2
     */
    private final static Pattern SSL_HANDSHAKE_ALERT = Pattern.compile("15030[123]00");


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
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
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
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

            ensureNotHandshakeFailurePacket(channel, size);

            if (!size.hasRemaining()) {
                size.rewind();
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }

        // Packet head is the same as a SSL handshake bytes but actually the packet is correct.
        // Over read data must be inserted into the buffer.
        if (tempOverBuf != null) {
            buffer.put(tempOverBuf);
            read += 1024;
            tempOverBuf = null;
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

    // We determine if a peer connection uses SSL/TLS by seeing from the first few bytes.
    private void ensureNotHandshakeFailurePacket(ReadableByteChannel channel, ByteBuffer size) throws IOException {
        if (ensureThrough)
            return;
        ensureThrough = true;
        String head = Utils.hexToString(size.array());
        if (SSL_HANDSHAKE_ALERT.matcher(head).find()) {
            // Actually, SSL record size is 2 bytes, but head byte is already read.
            ByteBuffer recordSizeBuf = ByteBuffer.allocate(1);
            channel.read(recordSizeBuf);
            recordSizeBuf.rewind();
            // If the data exceeding the SSL record size is read, this packet is correct.
            tempOverBuf = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(tempOverBuf);
            if (bytesRead == recordSizeBuf.get()) {
                throw new InvalidTransportLayerException("Destination connection may be SSL/TLS");
            }
        }
    }
}
