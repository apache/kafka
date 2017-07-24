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

import org.apache.kafka.common.protocol.ApiKeys;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {
    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = Integer.MAX_VALUE;

    /**
     * A human-readable string describing the source of this NetworkReceive.
     */
    private final String source;

    /**
     * A receiver which reads the header for the network data.
     */
    private final HeaderReceiver headerReceiver;

    /**
     * A buffer containing the payload of this NetworkReceive, or null if the payload has not been
     * read yet.
     */
    private ByteBuffer payload;

    public interface HeaderReceiver {
        /**
         * Allocate a new buffer for the receiver payload.
         *
         * @return              The new payload buffer, or null if we have not read the header yet.
         */
        ByteBuffer allocatePayload();

        /**
         * Read header data.
         *
         * @param channel       The channel to read from.
         * @return              The number of bytes which were read.  This will always be non-negative.
         */
        int read(ReadableByteChannel channel) throws IOException;
    }

    /**
     * A header receiver reads a size-delimited buffer.
     */
    public static class SizeDelimitedHeaderReceiver implements HeaderReceiver {
        private final int maxSize;
        private final ByteBuffer sizeBuffer;
        private int dataSize = -1;

        public SizeDelimitedHeaderReceiver(int maxSize) {
            this.maxSize = maxSize;
            this.sizeBuffer = ByteBuffer.allocate(4);
        }

        @Override
        public ByteBuffer allocatePayload() {
            if (dataSize < 0)
                return null;
            ByteBuffer buffer = ByteBuffer.allocate(dataSize);
            return buffer;
        }

        @Override
        public int read(ReadableByteChannel channel) throws IOException {
            if (dataSize > 0)
                return 0;
            int read = channel.read(sizeBuffer);
            if (read < 0) {
                throw new EOFException();
            }
            if (!sizeBuffer.hasRemaining()) {
                dataSize = sizeBuffer.getInt();
                if (dataSize < 0) {
                    throw new InvalidReceiveException("Invalid receive (size = " + dataSize + ")");
                } else if (dataSize > maxSize) {
                    throw new InvalidReceiveException("Invalid receive (size = " + dataSize + " larger than " + maxSize + ")");
                }
            }
            return read;
        }
    }

    /**
     * A header receiver which reads a Kafka Request.
     */
    public static class RequestStartHeaderReceiver implements HeaderReceiver {
        private final static int MIN_REQUEST_SIZE = 10;
        private final int maxSize;
        private final ByteBuffer startBuffer;
        private int dataSize = -1;
        private short apiKey = (short) -1;
        private short apiVersion = (short) -1;

        public RequestStartHeaderReceiver(int maxSize) {
            this.maxSize = maxSize;
            this.startBuffer = ByteBuffer.allocate(8);
        }

        @Override
        public ByteBuffer allocatePayload() {
            if (dataSize < 0)
                return null;
            ByteBuffer buffer = ByteBuffer.allocate(dataSize);
            buffer.putShort(apiKey);
            buffer.putShort(apiVersion);
            return buffer;
        }

        @Override
        public int read(ReadableByteChannel channel) throws IOException {
            if (dataSize > 0)
                return 0;
            int read = channel.read(startBuffer);
            if (read < 0)
                throw new EOFException();
            if (startBuffer.hasRemaining())
                return read;
            int size = startBuffer.getInt();
            if (size < MIN_REQUEST_SIZE) {
                throw new InvalidReceiveException("Invalid request receive (size = " + size + ")");
            } else if (size > maxSize) {
                throw new InvalidReceiveException("Invalid request receive (size = " + size + " larger than " + maxSize + ")");
            }
            apiKey = startBuffer.getShort();
            ApiKeys api;
            apiVersion = startBuffer.getShort();
            try {
                api = ApiKeys.forId(apiKey);
            } catch (IllegalArgumentException e) {
                throw new InvalidReceiveException("Unknown API key " + apiKey);
            }
            if (apiVersion < 0) {
                throw new InvalidReceiveException("Invalid api version " + apiVersion);
            } else if (api.latestVersion() < apiVersion) {
                throw new InvalidReceiveException("Unknown api version " + apiVersion);
            }
            dataSize = size;
            return read;
        }

    }

    public NetworkReceive(String source, HeaderReceiver headerReceiver) {
        this.source = source;
        this.headerReceiver = headerReceiver;
        this.payload = null;
    }

    public NetworkReceive(String source, ByteBuffer payload) {
        this.source = source;
        this.headerReceiver = null;
        this.payload = payload;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.headerReceiver = new SizeDelimitedHeaderReceiver(UNLIMITED);
        this.payload = null;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.headerReceiver = new SizeDelimitedHeaderReceiver(maxSize);
        this.payload = null;
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
        return (payload != null) && (!payload.hasRemaining());
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
        if (payload == null) {
            payload = headerReceiver.allocatePayload();
            if (payload == null) {
                read += headerReceiver.read(channel);
                payload = headerReceiver.allocatePayload();
            }
        }
        if (payload != null) {
            int bytesRead = channel.read(payload);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }
        return read;
    }

    public ByteBuffer payload() {
        return this.payload;
    }
}
