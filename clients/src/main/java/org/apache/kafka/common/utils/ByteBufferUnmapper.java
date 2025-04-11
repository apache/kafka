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

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

/**
 * Provides a mechanism to unmap mapped and direct byte buffers.
 *
 * The implementation was inspired by the one in Lucene's MMapDirectory.
 */
public final class ByteBufferUnmapper {

    // null if unmap is not supported
    private static final MethodHandle UNMAP;

    // null if unmap is supported
    private static final RuntimeException UNMAP_NOT_SUPPORTED_EXCEPTION;

    static {
        Object unmap = null;
        RuntimeException exception = null;
        try {
            unmap = lookupUnmapMethodHandle();
        } catch (RuntimeException e) {
            exception = e;
        }
        if (unmap != null) {
            UNMAP = (MethodHandle) unmap;
            UNMAP_NOT_SUPPORTED_EXCEPTION = null;
        } else {
            UNMAP = null;
            UNMAP_NOT_SUPPORTED_EXCEPTION = exception;
        }
    }

    private ByteBufferUnmapper() {}

    /**
     * Unmap the provided mapped or direct byte buffer.
     *
     * This buffer cannot be referenced after this call, so it's highly recommended that any fields referencing it
     * should be set to null.
     *
     * @throws IllegalArgumentException if buffer is not mapped or direct.
     */
    public static void unmap(String resourceDescription, ByteBuffer buffer) throws IOException {
        if (!buffer.isDirect())
            throw new IllegalArgumentException("Unmapping only works with direct buffers");
        if (UNMAP == null)
            throw UNMAP_NOT_SUPPORTED_EXCEPTION;

        try {
            UNMAP.invokeExact(buffer);
        } catch (Throwable throwable) {
            throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, throwable);
        }
    }

    private static MethodHandle lookupUnmapMethodHandle() {
        final MethodHandles.Lookup lookup = lookup();
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            MethodHandle unmapper = lookup.findVirtual(unsafeClass, "invokeCleaner",
                    methodType(void.class, ByteBuffer.class));
            Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Object theUnsafe = f.get(null);
            return unmapper.bindTo(theUnsafe);
        } catch (ReflectiveOperationException | RuntimeException e1) {
            throw new UnsupportedOperationException("Unmapping is not supported on this platform, because internal " +
                "Java APIs are not compatible with this Kafka version", e1);
        }
    }
}
