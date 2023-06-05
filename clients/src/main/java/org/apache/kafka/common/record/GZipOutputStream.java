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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * A variant of {@link java.util.zip.GZIPOutputStream}, which uses {@link ThreadLocal} {@link java.util.zip.Deflater} instance.
 */
public class GZipOutputStream extends DeflaterOutputStream {
    /**
     * A {@link ThreadLocal} instance of {@link java.util.zip.Deflater}.
     */
    private static final ThreadLocal<Deflater> DEFLATER = ThreadLocal.withInitial(() -> new Deflater(Deflater.DEFAULT_COMPRESSION, true));

    /**
     * CRC-32 of uncompressed data.
     */
    protected CRC32 crc = new CRC32();

    /*
     * GZIP header magic number.
     */
    private static final int GZIP_MAGIC = 0x8b1f;

    /*
     * Trailer size in bytes.
     *
     */
    private static final int TRAILER_SIZE = 8;

    /**
     * Creates a new output stream with the specified buffer size.
     *
     * <p>The new output stream instance is created as if by invoking
     * the 3-argument constructor GZIPOutputStream(out, size, false).
     *
     * @param out  the output stream
     * @param size the output buffer size
     * @throws IOException              If an I/O error has occurred.
     * @throws IllegalArgumentException if {@code size <= 0}
     */
    public GZipOutputStream(OutputStream out, int size) throws IOException {
        this(out, size, false);
    }

    /**
     * Creates a new output stream with the specified buffer size and
     * flush mode.
     *
     * @param out       the output stream
     * @param size      the output buffer size
     * @param syncFlush if {@code true} invocation of the inherited
     *                  {@link DeflaterOutputStream#flush() flush()} method of
     *                  this instance flushes the compressor with flush mode
     *                  {@link Deflater#SYNC_FLUSH} before flushing the output
     *                  stream, otherwise only flushes the output stream
     * @throws IOException              If an I/O error has occurred.
     * @throws IllegalArgumentException if {@code size <= 0}
     * @since 1.7
     */
    public GZipOutputStream(OutputStream out, int size, boolean syncFlush)
            throws IOException {
        super(out, DEFLATER.get(), size, syncFlush);
        writeHeader();
        crc.reset();
    }

    /**
     * Writes array of bytes to the compressed output stream. This method
     * will block until all the bytes are written.
     *
     * @param buf the data to be written
     * @param off the start offset of the data
     * @param len the length of the data
     * @throws IOException If an I/O error has occurred.
     */
    public synchronized void write(byte[] buf, int off, int len)
            throws IOException {
        super.write(buf, off, len);
        crc.update(buf, off, len);
    }

    /**
     * Finishes writing compressed data to the output stream without closing
     * the underlying stream. Use this method when applying multiple filters
     * in succession to the same output stream.
     *
     * @throws IOException if an I/O error has occurred
     */
    public void finish() throws IOException {
        if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
                int len = def.deflate(buf, 0, buf.length);
                if (def.finished() && len <= buf.length - TRAILER_SIZE) {
                    // last deflater buffer. Fit trailer at the end
                    writeTrailer(buf, len);
                    len = len + TRAILER_SIZE;
                    out.write(buf, 0, len);
                    return;
                }
                if (len > 0)
                    out.write(buf, 0, len);
            }
            // if we can't fit the trailer at the end of the last
            // deflater buffer, we write it separately
            byte[] trailer = new byte[TRAILER_SIZE];
            writeTrailer(trailer, 0);
            out.write(trailer);
        }
    }

    /**
     * Writes remaining compressed data to the output stream and closes the
     * underlying stream.
     * @exception IOException if an I/O error has occurred
     */
    public void close() throws IOException {
        super.close();
        DEFLATER.get().reset();
    }

    /*
     * Writes GZIP member header.
     */
    private void writeHeader() throws IOException {
        out.write(new byte[]{
            (byte) GZIP_MAGIC,          // Magic number (short)
            (byte) (GZIP_MAGIC >> 8),   // Magic number (short)
            Deflater.DEFLATED,          // Compression method (CM)
            0,                          // Flags (FLG)
            0,                          // Modification time MTIME (int)
            0,                          // Modification time MTIME (int)
            0,                          // Modification time MTIME (int)
            0,                          // Modification time MTIME (int)
            0,                          // Extra flags (XFLG)
            0                           // Operating system (OS)
        });
    }

    /*
     * Writes GZIP member trailer to a byte array, starting at a given
     * offset.
     */
    private void writeTrailer(byte[] buf, int offset) throws IOException {
        writeInt((int) crc.getValue(), buf, offset); // CRC-32 of uncompr. data
        writeInt(def.getTotalIn(), buf, offset + 4); // Number of uncompr. bytes
    }

    /*
     * Writes integer in Intel byte order to a byte array, starting at a
     * given offset.
     */
    private void writeInt(int i, byte[] buf, int offset) throws IOException {
        writeShort(i & 0xffff, buf, offset);
        writeShort((i >> 16) & 0xffff, buf, offset + 2);
    }

    /*
     * Writes short integer in Intel byte order to a byte array, starting
     * at a given offset
     */
    private void writeShort(int s, byte[] buf, int offset) {
        buf[offset] = (byte) (s & 0xff);
        buf[offset + 1] = (byte) ((s >> 8) & 0xff);
    }
}
