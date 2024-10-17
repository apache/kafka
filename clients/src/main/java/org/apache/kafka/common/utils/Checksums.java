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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * Utility methods for `Checksum` instances.
 *
 * Implementation note: we can add methods to our implementations of CRC32 and CRC32C, but we cannot do the same for
 * the Java implementations (we prefer the Java 9 implementation of CRC32C if available). A utility class is the
 * simplest way to add methods that are useful for all Checksum implementations.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public final class Checksums {
    private static final MethodHandle BYTE_BUFFER_UPDATE;

    static {
        MethodHandle byteBufferUpdate = null;
        try {
            byteBufferUpdate = MethodHandles.publicLookup().findVirtual(Checksum.class, "update",
                    MethodType.methodType(void.class, ByteBuffer.class));
        } catch (Throwable t) {
            handleUpdateThrowable(t);
        }
        BYTE_BUFFER_UPDATE = byteBufferUpdate;
    }

    private Checksums() {
    }

    /**
     * Uses {@link Checksum#update} on {@code buffer}'s content, without modifying its position and limit.<br>
     * This is semantically equivalent to {@link #update(Checksum, ByteBuffer, int, int)} with {@code offset = 0}.
     */
    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    /**
     * Uses {@link Checksum#update} on {@code buffer}'s content, starting from the given {@code offset}
     * by the provided {@code length}, without modifying its position and limit.
     */
    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else if (BYTE_BUFFER_UPDATE != null && buffer.isDirect()) {
            final int oldPosition = buffer.position();
            final int oldLimit = buffer.limit();
            try {
                // save a slice to be used to save an allocation in the hot-path
                final int start = oldPosition + offset;
                buffer.limit(start + length);
                buffer.position(start);
                BYTE_BUFFER_UPDATE.invokeExact(checksum, buffer);
            } catch (Throwable t) {
                handleUpdateThrowable(t);
            } finally {
                // reset buffer's offsets
                buffer.limit(oldLimit);
                buffer.position(oldPosition);
            }
        } else {
            // slow-path
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++) {
                checksum.update(buffer.get(i));
            }
        }
    }

    private static void handleUpdateThrowable(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        throw new IllegalStateException(t);
    }
    
    public static void updateInt(Checksum checksum, int input) {
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }

    public static void updateLong(Checksum checksum, long input) {
        checksum.update((byte) (input >> 56));
        checksum.update((byte) (input >> 48));
        checksum.update((byte) (input >> 40));
        checksum.update((byte) (input >> 32));
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input /* >> 0 */);
    }
}
