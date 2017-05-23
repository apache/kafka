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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE(0, "none", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            return buffer;
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            return buffer;
        }
    },

    GZIP(1, "gzip", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            try {
                return new GZIPOutputStream(buffer, bufferSize);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            try {
                return new GZIPInputStream(buffer);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }
    },

    SNAPPY(2, "snappy", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            try {
                return (OutputStream) SnappyConstructors.OUTPUT.invoke(buffer, bufferSize);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            try {
                return (InputStream) SnappyConstructors.INPUT.invoke(buffer);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }
    },

    LZ4(3, "lz4", 1.0f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            try {
                return (OutputStream) LZ4Constructors.OUTPUT.invoke(buffer,
                        messageVersion == RecordBatch.MAGIC_VALUE_V0);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            try {
                return (InputStream) LZ4Constructors.INPUT.invoke(buffer,
                        messageVersion == RecordBatch.MAGIC_VALUE_V0);
            } catch (Throwable e) {
                throw new KafkaException(e);
            }
        }
    };

    public final int id;
    public final String name;
    public final float rate;

    CompressionType(int id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    public abstract OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize);

    public abstract InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion);

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }

    // Dynamically load the Snappy and LZ4 classes so that we only have a runtime dependency on compression algorithms
    // that are used. This is important for platforms that are not supported by the underlying libraries.
    // Note that we are using the initialization-on-demand holder idiom, so it's important that the initialisation
    // is done in separate classes (one per compression type).

    private static class LZ4Constructors {
        static final MethodHandle INPUT = findConstructor(
                "org.apache.kafka.common.record.KafkaLZ4BlockInputStream",
                MethodType.methodType(void.class, InputStream.class, Boolean.TYPE));

        static final MethodHandle OUTPUT = findConstructor(
                "org.apache.kafka.common.record.KafkaLZ4BlockOutputStream",
                MethodType.methodType(void.class, OutputStream.class, Boolean.TYPE));

    }

    private static class SnappyConstructors {
        static final MethodHandle INPUT = findConstructor("org.xerial.snappy.SnappyInputStream",
                MethodType.methodType(void.class, InputStream.class));
        static final MethodHandle OUTPUT = findConstructor("org.xerial.snappy.SnappyOutputStream",
                MethodType.methodType(void.class, OutputStream.class, Integer.TYPE));
    }

    private static MethodHandle findConstructor(String className, MethodType methodType) {
        try {
            return MethodHandles.publicLookup().findConstructor(Class.forName(className), methodType);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

}
