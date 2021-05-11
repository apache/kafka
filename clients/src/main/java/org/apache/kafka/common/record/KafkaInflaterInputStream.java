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

/*
 * Copyright (c) 1996, 2019, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package org.apache.kafka.common.record;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

/**
 * this is a modified {@link java.util.zip.InflaterInputStream} that gets
 * it's internal buffer from a {@link BufferSupplier} and also returns the
 * buffer when it's {@link #close()}ed
 */
public class KafkaInflaterInputStream extends FilterInputStream {
    /**
     * Decompressor for this stream.
     */
    protected Inflater inf;

    /**
     * Buffer Supplier for the decompression buffer
     */
    protected BufferSupplier bufferSupplier;

    /**
     * Actual Buffer originating from (and to be released back to) {@link #bufferSupplier}
     */
    protected ByteBuffer buffer;
    /**
     * Actual Buffer originating from (and to be released back to) {@link #bufferSupplier}
     */
    protected ByteBuffer skipBuffer;

    /**
     * Input buffer for decompression.
     */
    protected byte[] buf;

    /**
     * scratch (output) buffer for skipping
     */
    private byte[] b;

    /**
     * Length of input buffer.
     */
    protected int len;

    private boolean closed = false;
    // this flag is set to true after EOF has reached
    private boolean reachEOF = false;

    /**
     * Check to make sure that this stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    /**
     * Creates a new input stream with the specified decompressor and
     * buffer size.
     * @param in the input stream
     * @param inf the decompressor ("inflater")
     * @param size the input buffer size
     * @throws    IllegalArgumentException if {@code size <= 0}
     */
    public KafkaInflaterInputStream(InputStream in, Inflater inf, BufferSupplier bufferSupplier, int size) {
        super(in);
        if (in == null || inf == null) {
            throw new NullPointerException();
        } else if (size <= 0) {
            throw new IllegalArgumentException("buffer size <= 0");
        }
        this.inf = inf;
        this.bufferSupplier = bufferSupplier;
        this.buffer = this.bufferSupplier.get(size);
        this.skipBuffer = this.bufferSupplier.get(512);
        this.buf = buffer.array(); //implicitly we do not support off-heap buffers
        this.b = skipBuffer.array(); //implicitly we do not support off-heap buffers
    }

    /**
     * Creates a new input stream with the specified decompressor and a
     * default buffer size.
     * @param in the input stream
     * @param inf the decompressor ("inflater")
     */
    public KafkaInflaterInputStream(InputStream in, Inflater inf, BufferSupplier bufferSupplier) {
        this(in, inf, bufferSupplier, 512);
    }

    boolean usesDefaultInflater = false;

    /**
     * Creates a new input stream with a default decompressor and buffer size.
     * @param in the input stream
     */
    public KafkaInflaterInputStream(InputStream in, BufferSupplier bufferSupplier) {
        this(in, new Inflater(), bufferSupplier);
        usesDefaultInflater = true;
    }

    private final byte[] singleByteBuf = new byte[1];

    /**
     * Reads a byte of uncompressed data. This method will block until
     * enough input is available for decompression.
     * @return the byte read, or -1 if end of compressed input is reached
     * @throws    IOException if an I/O error has occurred
     */
    public int read() throws IOException {
        ensureOpen();
        return read(singleByteBuf, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(singleByteBuf[0]);
    }

    /**
     * Reads uncompressed data into an array of bytes. If {@code len} is not
     * zero, the method will block until some input can be decompressed; otherwise,
     * no bytes are read and {@code 0} is returned.
     * @param b the buffer into which the data is read
     * @param off the start offset in the destination array {@code b}
     * @param len the maximum number of bytes read
     * @return the actual number of bytes read, or -1 if the end of the
     *         compressed input is reached or a preset dictionary is needed
     * @throws     NullPointerException If {@code b} is {@code null}.
     * @throws     IndexOutOfBoundsException If {@code off} is negative,
     * {@code len} is negative, or {@code len} is greater than
     * {@code b.length - off}
     * @throws ZipException if a ZIP format error has occurred
     * @throws    IOException if an I/O error has occurred
     */
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        try {
            int n;
            while ((n = inf.inflate(b, off, len)) == 0) {
                if (inf.finished() || inf.needsDictionary()) {
                    reachEOF = true;
                    return -1;
                }
                if (inf.needsInput()) {
                    fill();
                }
            }
            return n;
        } catch (DataFormatException e) {
            String s = e.getMessage();
            throw new ZipException(s != null ? s : "Invalid ZLIB data format");
        }
    }

    /**
     * Returns 0 after EOF has been reached, otherwise always return 1.
     * <p>
     * Programs should not count on this method to return the actual number
     * of bytes that could be read without blocking.
     *
     * @return     1 before EOF and 0 after EOF.
     * @throws     IOException  if an I/O error occurs.
     *
     */
    public int available() throws IOException {
        ensureOpen();
        if (reachEOF) {
            return 0;
        } else if (inf.finished()) {
            // the end of the compressed data stream has been reached
            reachEOF = true;
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * Skips specified number of bytes of uncompressed data.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped.
     * @throws    IOException if an I/O error has occurred
     * @throws    IllegalArgumentException if {@code n < 0}
     */
    public long skip(long n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException("negative skip length");
        }
        ensureOpen();
        int max = (int) Math.min(n, Integer.MAX_VALUE);
        int total = 0;
        while (total < max) {
            int len = max - total;
            if (len > b.length) {
                len = b.length;
            }
            len = read(b, 0, len);
            if (len == -1) {
                reachEOF = true;
                break;
            }
            total += len;
        }
        return total;
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     * @throws    IOException if an I/O error has occurred
     */
    public void close() throws IOException {
        if (!closed) {
            if (usesDefaultInflater)
                inf.end();
            bufferSupplier.release(buffer);
            bufferSupplier.release(skipBuffer);
            buffer = null;
            skipBuffer = null;
            buf = null;
            b = null;
            in.close();
            closed = true;
        }
    }

    /**
     * Fills input buffer with more data to decompress.
     * @throws    IOException if an I/O error has occurred
     */
    protected void fill() throws IOException {
        ensureOpen();
        len = in.read(buf, 0, buf.length);
        if (len == -1) {
            throw new EOFException("Unexpected end of ZLIB input stream");
        }
        inf.setInput(buf, 0, len);
    }

    /**
     * Tests if this input stream supports the {@code mark} and
     * {@code reset} methods. The {@code markSupported}
     * method of {@code InflaterInputStream} returns
     * {@code false}.
     *
     * @return  a {@code boolean} indicating if this stream type supports
     *          the {@code mark} and {@code reset} methods.
     * @see     InputStream#mark(int)
     * @see     InputStream#reset()
     */
    public boolean markSupported() {
        return false;
    }

    /**
     * Marks the current position in this input stream.
     *
     * <p> The {@code mark} method of {@code InflaterInputStream}
     * does nothing.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position becomes invalid.
     * @see     InputStream#reset()
     */
    public synchronized void mark(int readlimit) {
    }

    /**
     * Repositions this stream to the position at the time the
     * {@code mark} method was last called on this input stream.
     *
     * <p> The method {@code reset} for class
     * {@code InflaterInputStream} does nothing except throw an
     * {@code IOException}.
     *
     * @throws     IOException  if this method is invoked.
     * @see     InputStream#mark(int)
     * @see     IOException
     */
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }
}
