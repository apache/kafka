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

import java.nio.ByteBuffer;

/**
 * Utility functions for working with SSL.
 */
final class SslUtils {

    /**
     * change cipher spec
     */
    static final int SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC = 20;

    /**
     * alert
     */
    static final int SSL_CONTENT_TYPE_ALERT = 21;

    /**
     * handshake
     */
    static final int SSL_CONTENT_TYPE_HANDSHAKE = 22;

    /**
     * application data
     */
    static final int SSL_CONTENT_TYPE_APPLICATION_DATA = 23;

    /**
     * HeartBeat Extension
     */
    static final int SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT = 24;

    /**
     * the length of the ssl record header (in bytes)
     */
    static final int SSL_RECORD_HEADER_LENGTH = 5;

    /**
     * Not enough data in buffer to parse the record length
     */
    static final int NOT_ENOUGH_DATA = -1;

    /**
     * data is not encrypted
     */
    static final int NOT_ENCRYPTED = -2;

    /**
     * Returns {@code true} if the given {@link ByteBuffer} is encrypted. Be aware
     * that this method will not increase the readerIndex of the given
     * {@link ByteBuffer}.
     *
     * @param buffer The {@link ByteBuffer} to read from. Be aware that it must
     *            have at least 5 bytes to read, otherwise it will throw an
     *            {@link IllegalArgumentException}.
     * @return encrypted {@code true} if the {@link ByteBuffer} is encrypted,
     *         {@code false} otherwise.
     * @throws IllegalArgumentException Is thrown if the given {@link ByteBuffer}
     *             has not at least 5 bytes to read.
     */
    static boolean isEncrypted(ByteBuffer buffer) {
        if (buffer.remaining() < SSL_RECORD_HEADER_LENGTH) {
            throw new IllegalArgumentException(
                    "buffer must have at least " + SSL_RECORD_HEADER_LENGTH + " readable bytes");
        }
        return getEncryptedPacketLength(buffer) != SslUtils.NOT_ENCRYPTED;
    }

    /**
     * Return how many bytes can be read out of the encrypted data. Be aware
     * that this method will not increase the readerIndex of the given
     * {@link ByteBuffer}. This method assumes that {@link ByteBuffer} is
     * big-endian byte ordered (the default for {@link ByteBuffer}.
     *
     * @param buffer The {@link ByteBuffer} to read from. Be aware that it must
     *            have at least {@link #SSL_RECORD_HEADER_LENGTH} bytes to read,
     *            otherwise it will throw an {@link IllegalArgumentException}.
     * @return length The length of the encrypted packet that is included in the
     *         buffer or {@link #SslUtils#NOT_ENOUGH_DATA} if not enough data is
     *         present in the {@link ByteBuffer}. This will return
     *         {@link SslUtils#NOT_ENCRYPTED} if the given {@link ByteBuffer} is
     *         not encrypted at all.
     * @throws IllegalArgumentException Is thrown if the given
     *             {@link ByteBuffer} has not at least
     *             {@link #SSL_RECORD_HEADER_LENGTH} bytes to read.
     */
    private static int getEncryptedPacketLength(final ByteBuffer buffer) {
        int packetLength = 0;
        int pos = buffer.position();
        // SSLv3 or TLS - Check ContentType
        boolean tls;
        switch (unsignedByte(buffer.get(pos))) {
            case SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC:
            case SSL_CONTENT_TYPE_ALERT:
            case SSL_CONTENT_TYPE_HANDSHAKE:
            case SSL_CONTENT_TYPE_APPLICATION_DATA:
            case SSL_CONTENT_TYPE_EXTENSION_HEARTBEAT:
                tls = true;
                break;
            default:
                // SSLv2 or bad data
                tls = false;
        }

        if (tls) {
            // SSLv3 or TLS - Check ProtocolVersion
            int majorVersion = unsignedByte(buffer.get(pos + 1));
            if (majorVersion == 3) {
                // SSLv3 or TLS
                packetLength = unsignedShortBE(buffer, pos + 3) + SSL_RECORD_HEADER_LENGTH;
                if (packetLength <= SSL_RECORD_HEADER_LENGTH) {
                    // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                    tls = false;
                }
            } else {
                // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                tls = false;
            }
        }

        if (!tls) {
            // SSLv2 or bad data - Check the version
            int headerLength = (unsignedByte(buffer.get(pos)) & 0x80) != 0 ? 2 : 3;
            int majorVersion = unsignedByte(buffer.get(pos + headerLength + 1));
            if (majorVersion == 2 || majorVersion == 3) {
                // SSLv2
                packetLength = headerLength == 2 ? (buffer.getShort(pos) & 0x7FFF) + 2
                        : (buffer.getShort(pos) & 0x3FFF) + 3;
                if (packetLength <= headerLength) {
                    return NOT_ENOUGH_DATA;
                }
            } else {
                return NOT_ENCRYPTED;
            }
        }
        return packetLength;
    }

    // Reads a big-endian unsigned short integer from the buffer
    private static int unsignedShortBE(ByteBuffer buffer, int offset) {
        return buffer.getShort(offset) & 0xFFFF;
    }

    private static short unsignedByte(byte b) {
        return (short) (b & 0xFF);
    }

    private SslUtils() {
    }
}
