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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;

/**
 * Utility methods for MappedByteBuffer implementations.
 *
 * The unmap implementation was inspired by the one in Lucene's MMapDirectory.
 */
public final class MappedByteBuffers {

    private static final Logger LOGGER = LoggerFactory.getLogger(MappedByteBuffers.class);

    // null if unmap is not supported
    private static final MethodHandle UNMAP;

    // null if unmap is supported
    private static final String UNMAP_NOT_SUPPORTED_REASON;

    static {
        Object unmap = lookupUnmapMethodHandle();
        if (unmap instanceof MethodHandle) {
            UNMAP = (MethodHandle) unmap;
            UNMAP_NOT_SUPPORTED_REASON = null;
        } else {
            UNMAP = null;
            UNMAP_NOT_SUPPORTED_REASON = (String) unmap;
        }
    }

    private MappedByteBuffers() {}

    public static void unmap(String resourceDescription, MappedByteBuffer buffer) throws IOException {
        if (!buffer.isDirect())
            throw new IllegalArgumentException("Unmapping only works with direct buffers");
        if (UNMAP == null)
            throw new UnsupportedOperationException(UNMAP_NOT_SUPPORTED_REASON);

        try {
            UNMAP.invokeExact((ByteBuffer) buffer);
        } catch (Throwable throwable) {
            throw new IOException("Unable to unmap the mapped buffer: " + resourceDescription, throwable);
        }
    }

    /**
     * Return an unmap MethodHandle or a String with the reason why unmap is not supported.
     */
    private static Object lookupUnmapMethodHandle() {
        final MethodHandles.Lookup lookup = lookup();
        try {
            try {
                return unmapJava9(lookup);
            } catch (SecurityException e) {
                return e.getMessage();
            } catch (ReflectiveOperationException | RuntimeException e) {
                LOGGER.debug("Falling back to unmap implementation for Java 7/Java 8 due to " + e.getMessage());
                return unmapJava7Or8(lookup);
            }
        } catch (SecurityException se) {
            return se.getMessage();
        } catch (ReflectiveOperationException | RuntimeException e) {
            return "Unmapping is not supported on this platform, because internal Java APIs are not compatible with " +
                    "this Kafka version: " + e;
        }
    }

    private static Object unmapJava7Or8(MethodHandles.Lookup lookup) throws ReflectiveOperationException {
        /* "Compile" a MethodHandle that is roughly equivalent to the following lambda:
         *
         * (ByteBuffer buffer) -> {
         *   sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
         *   if (nonNull(cleaner))
         *     cleaner.clean();
         *   else
         *     noop(cleaner); // the noop is needed because MethodHandles#guardWithTest always needs both if and else
         * }
         */
        Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
        Method m = directBufferClass.getMethod("cleaner");
        m.setAccessible(true);
        MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
        Class<?> cleanerClass = directBufferCleanerMethod.type().returnType();
        MethodHandle cleanMethod = lookup.findVirtual(cleanerClass, "clean", methodType(void.class));
        MethodHandle nonNullTest = lookup.findStatic(MappedByteBuffers.class, "nonNull",
                methodType(boolean.class, Object.class)).asType(methodType(boolean.class, cleanerClass));
        MethodHandle noop = dropArguments(constant(Void.class, null).asType(methodType(void.class)), 0, cleanerClass);
        MethodHandle unmapper = filterReturnValue(directBufferCleanerMethod, guardWithTest(nonNullTest, cleanMethod, noop))
                .asType(methodType(void.class, ByteBuffer.class));
        return unmapper;
    }

    private static Object unmapJava9(MethodHandles.Lookup lookup) throws ReflectiveOperationException {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        MethodHandle unmapper = lookup.findVirtual(unsafeClass, "invokeCleaner",
                methodType(void.class, ByteBuffer.class));
        Field f = unsafeClass.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Object theUnsafe = f.get(null);
        return unmapper.bindTo(theUnsafe);
    }

    private static boolean nonNull(Object o) {
        return o != null;
    }
}
