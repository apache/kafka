/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.lang.reflect.Constructor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compressor {

    static private final float COMPRESSION_RATE_DAMPING_FACTOR = 0.9f;
    static private final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    static private final int COMPRESSION_DEFAULT_BUFFER_SIZE = 1024;

    private static final float[] TYPE_TO_RATE;

    static {
        int maxTypeId = -1;
        for (CompressionType type : CompressionType.values())
            maxTypeId = Math.max(maxTypeId, type.id);
        TYPE_TO_RATE = new float[maxTypeId + 1];
        for (CompressionType type : CompressionType.values()) {
            TYPE_TO_RATE[type.id] = type.rate;
        }
    }

    // dynamically load the snappy and lz4 classes to avoid runtime dependency if we are not using compression
    // caching constructors to avoid invoking of Class.forName method for each batch
    private static MemoizingConstructorSupplier snappyOutputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyOutputStream")
                .getConstructor(OutputStream.class, Integer.TYPE);
        }
    });

    private static MemoizingConstructorSupplier lz4OutputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockOutputStream")
                .getConstructor(OutputStream.class);
        }
    });

    private static MemoizingConstructorSupplier snappyInputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.xerial.snappy.SnappyInputStream")
                .getConstructor(InputStream.class);
        }
    });

    private static MemoizingConstructorSupplier lz4InputStreamSupplier = new MemoizingConstructorSupplier(new ConstructorSupplier() {
        @Override
        public Constructor get() throws ClassNotFoundException, NoSuchMethodException {
            return Class.forName("org.apache.kafka.common.record.KafkaLZ4BlockInputStream")
                .getConstructor(InputStream.class);
        }
    });

    private final CompressionType type;
    private final DataOutputStream appendStream;
    private final ByteBufferOutputStream bufferStream;
    private final int initPos;

    public long writtenUncompressed;
    public long numRecords;
    public float compressionRate;

    public Compressor(ByteBuffer buffer, CompressionType type, int blockSize) {
        this.type = type;
        this.initPos = buffer.position();

        this.numRecords = 0;
        this.writtenUncompressed = 0;
        this.compressionRate = 1;

        if (type != CompressionType.NONE) {
            // for compressed records, leave space for the header and the shallow message metadata
            // and move the starting position to the value payload offset
            buffer.position(initPos + Records.LOG_OVERHEAD + Record.RECORD_OVERHEAD);
        }

        // create the stream
        bufferStream = new ByteBufferOutputStream(buffer);
        appendStream = wrapForOutput(bufferStream, type, blockSize);
    }

    public Compressor(ByteBuffer buffer, CompressionType type) {
        this(buffer, type, COMPRESSION_DEFAULT_BUFFER_SIZE);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public double compressionRate() {
        return compressionRate;
    }

    public void close() {
        try {
            appendStream.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        if (type != CompressionType.NONE) {
            ByteBuffer buffer = bufferStream.buffer();
            int pos = buffer.position();
            // write the header, for the end offset write as number of records - 1
            buffer.position(initPos);
            buffer.putLong(numRecords - 1);
            buffer.putInt(pos - initPos - Records.LOG_OVERHEAD);
            // write the shallow message (the crc and value size are not correct yet)
            Record.write(buffer, null, null, type, 0, -1);
            // compute the fill the value size
            int valueSize = pos - initPos - Records.LOG_OVERHEAD - Record.RECORD_OVERHEAD;
            buffer.putInt(initPos + Records.LOG_OVERHEAD + Record.KEY_OFFSET, valueSize);
            // compute and fill the crc at the beginning of the message
            long crc = Record.computeChecksum(buffer,
                initPos + Records.LOG_OVERHEAD + Record.MAGIC_OFFSET,
                pos - initPos - Records.LOG_OVERHEAD - Record.MAGIC_OFFSET);
            Utils.writeUnsignedInt(buffer, initPos + Records.LOG_OVERHEAD + Record.CRC_OFFSET, crc);
            // reset the position
            buffer.position(pos);

            // update the compression ratio
            this.compressionRate = (float) buffer.position() / this.writtenUncompressed;
            TYPE_TO_RATE[type.id] = TYPE_TO_RATE[type.id] * COMPRESSION_RATE_DAMPING_FACTOR +
                compressionRate * (1 - COMPRESSION_RATE_DAMPING_FACTOR);
        }
    }

    // Note that for all the write operations below, IO exceptions should
    // never be thrown since the underlying ByteBufferOutputStream does not throw IOException;
    // therefore upon encountering this issue we just close the append stream.

    public void putLong(final long value) {
        try {
            appendStream.writeLong(value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void putInt(final int value) {
        try {
            appendStream.writeInt(value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void put(final ByteBuffer buffer) {
        try {
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void putByte(final byte value) {
        try {
            appendStream.write(value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void put(final byte[] bytes, final int offset, final int len) {
        try {
            appendStream.write(bytes, offset, len);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void putRecord(byte[] key, byte[] value, CompressionType type, int valueOffset, int valueSize) {
        // put a record as un-compressed into the underlying stream
        long crc = Record.computeChecksum(key, value, type, valueOffset, valueSize);
        byte attributes = Record.computeAttributes(type);
        putRecord(crc, attributes, key, value, valueOffset, valueSize);
    }

    public void putRecord(byte[] key, byte[] value) {
        putRecord(key, value, CompressionType.NONE, 0, -1);
    }

    private void putRecord(final long crc, final byte attributes, final byte[] key, final byte[] value, final int valueOffset, final int valueSize) {
        Record.write(this, crc, attributes, key, value, valueOffset, valueSize);
    }

    public void recordWritten(int size) {
        numRecords += 1;
        writtenUncompressed += size;
    }

    public long numRecordsWritten() {
        return numRecords;
    }

    public long estimatedBytesWritten() {
        if (type == CompressionType.NONE) {
            return bufferStream.buffer().position();
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return (long) (writtenUncompressed * TYPE_TO_RATE[type.id] * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    // the following two functions also need to be public since they are used in MemoryRecords.iteration

    static public DataOutputStream wrapForOutput(ByteBufferOutputStream buffer, CompressionType type, int bufferSize) {
        try {
            switch (type) {
                case NONE:
                    return new DataOutputStream(buffer);
                case GZIP:
                    return new DataOutputStream(new GZIPOutputStream(buffer, bufferSize));
                case SNAPPY:
                    try {
                        OutputStream stream = (OutputStream) snappyOutputStreamSupplier.get().newInstance(buffer, bufferSize);
                        return new DataOutputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                case LZ4:
                    try {
                        OutputStream stream = (OutputStream) lz4OutputStreamSupplier.get().newInstance(buffer);
                        return new DataOutputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                default:
                    throw new IllegalArgumentException("Unknown compression type: " + type);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    static public DataInputStream wrapForInput(ByteBufferInputStream buffer, CompressionType type) {
        try {
            switch (type) {
                case NONE:
                    return new DataInputStream(buffer);
                case GZIP:
                    return new DataInputStream(new GZIPInputStream(buffer));
                case SNAPPY:
                    try {
                        InputStream stream = (InputStream) snappyInputStreamSupplier.get().newInstance(buffer);
                        return new DataInputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                case LZ4:
                    try {
                        InputStream stream = (InputStream) lz4InputStreamSupplier.get().newInstance(buffer);
                        return new DataInputStream(stream);
                    } catch (Exception e) {
                        throw new KafkaException(e);
                    }
                default:
                    throw new IllegalArgumentException("Unknown compression type: " + type);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

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
                        Constructor constructor = delegate.get();
                        value = constructor;
                        initialized = true;
                        return constructor;
                    }
                }
            }
            return value;
        }
    }
}
