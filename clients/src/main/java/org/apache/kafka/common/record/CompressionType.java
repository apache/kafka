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
import java.lang.reflect.Constructor;
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

    GZIP(1, "gzip", 0.5f) {
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

    SNAPPY(2, "snappy", 0.5f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            try {
                return (OutputStream) SNAPPY_OUTPUT_STREAM_SUPPLIER.get().newInstance(buffer, bufferSize);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            try {
                return (InputStream) SNAPPY_INPUT_STREAM_SUPPLIER.get().newInstance(buffer);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }
    },

    LZ4(3, "lz4", 0.5f) {
        @Override
        public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion, int bufferSize) {
            try {
                return (OutputStream) LZ4_OUTPUT_STREAM_SUPPLIER.get().newInstance(buffer,
                        messageVersion == Record.MAGIC_VALUE_V0);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        }

        @Override
        public InputStream wrapForInput(ByteBufferInputStream buffer, byte messageVersion) {
            try {
                return (InputStream) LZ4_INPUT_STREAM_SUPPLIER.get().newInstance(buffer,
                        messageVersion == Record.MAGIC_VALUE_V0);
            } catch (Exception e) {
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

    // dynamically load the snappy and lz4 classes to avoid runtime dependency if we are not using compression
    // caching constructors to avoid invoking of Class.forName method for each batch
    private static final MemoizingConstructorSupplier SNAPPY_OUTPUT_STREAM_SUPPLIER = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyOutputStream")
                    .getConstructor(OutputStream.class, Integer.TYPE);
        }
    });

    private static final MemoizingConstructorSupplier LZ4_OUTPUT_STREAM_SUPPLIER = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockOutputStream")
                    .getConstructor(OutputStream.class, Boolean.TYPE);
        }
    });

    private static final MemoizingConstructorSupplier SNAPPY_INPUT_STREAM_SUPPLIER = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyInputStream")
                    .getConstructor(InputStream.class);
        }
    });

    private static final MemoizingConstructorSupplier LZ4_INPUT_STREAM_SUPPLIER = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockInputStream")
                    .getConstructor(InputStream.class, Boolean.TYPE);
        }
    });

    private interface ConstructorSupplier {
        Constructor get() throws ClassNotFoundException, NoSuchMethodException;
    }

    // this code is based on Guava's @see{com.google.common.base.Suppliers.MemoizingSupplier}
    private static class MemoizingConstructorSupplier {
        final ConstructorSupplier delegate;
        transient volatile boolean initialized;
        transient Constructor value;

        public MemoizingConstructorSupplier(ConstructorSupplier delegate) {
            this.delegate = delegate;
        }

        public Constructor get() throws NoSuchMethodException, ClassNotFoundException {
            if (!initialized) {
                synchronized (this) {
                    if (!initialized) {
                        value = delegate.get();
                        initialized = true;
                    }
                }
            }
            return value;
        }
    }

}
