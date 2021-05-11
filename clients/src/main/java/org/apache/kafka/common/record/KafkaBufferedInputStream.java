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
 * Copyright (c) 1994, 2019, Oracle and/or its affiliates. All rights reserved.
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * this is a version of {@link java.io.BufferedInputStream} that has been modified
 * to obtain its' buffer from a {@link org.apache.kafka.common.record.BufferSupplier}
 * (and also return said buffer when it's closed).
 * Also, mark/reset support has been removed since it may involve having to re-allocate the buffer
 */
public class KafkaBufferedInputStream extends FilterInputStream {

    private static final int DEFAULT_BUFFER_SIZE = 8192;

    //original code uses Unsafe here, for reasons that are not relevant to our use case.
    //this achieves the same behaviour in a more compatible way.
    private static final AtomicReferenceFieldUpdater<KafkaBufferedInputStream, byte[]> BUF_FIELD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(KafkaBufferedInputStream.class, byte[].class, "buf");

    /**
     * the supplier providing buffer(s), and to which any buffers are returned
     */
    private final BufferSupplier bufferSupplier;
    /**
     * the actual buffer currently used, as returned by supplier.
     */
    private volatile ByteBuffer buffer;

    /**
     * The internal buffer array where the data is stored.
     * Replacement not currently supported
     */
    /*
     * We null this out with a CAS on close(), which is necessary since
     * closes can be asynchronous. We use nullness of buf[] as primary
     * indicator that this stream is closed. (The "in" field is also
     * nulled out on close.)
     */
    protected volatile byte[] buf;

    /**
     * The index one greater than the index of the last valid byte in
     * the buffer.
     * This value is always
     * in the range {@code 0} through {@code buf.length};
     * elements {@code buf[0]} through {@code buf[count-1]}
     * contain buffered input data obtained
     * from the underlying  input stream.
     */
    protected int count;

    /**
     * The current position in the buffer. This is the index of the next
     * character to be read from the {@code buf} array.
     * <p>
     * This value is always in the range {@code 0}
     * through {@code count}. If it is less
     * than {@code count}, then  {@code buf[pos]}
     * is the next byte to be supplied as input;
     * if it is equal to {@code count}, then
     * the  next {@code read} or {@code skip}
     * operation will require more bytes to be
     * read from the contained  input stream.
     *
     * @see     KafkaBufferedInputStream#buf
     */
    protected int pos;

    /**
     * Check to make sure that underlying input stream has not been
     * nulled out due to close; if not return it;
     */
    private InputStream getInIfOpen() throws IOException {
        InputStream input = in;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = buf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    /**
     * Creates a {@code BufferedInputStream}
     * and saves its  argument, the input stream
     * {@code in}, for later use. An internal
     * buffer array is created and  stored in {@code buf}.
     *
     * @param   in   the underlying input stream.
     */
    public KafkaBufferedInputStream(InputStream in, BufferSupplier bufferSupplier) {
        this(in, bufferSupplier, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a {@code BufferedInputStream}
     * with the specified buffer size,
     * and saves its  argument, the input stream
     * {@code in}, for later use.  An internal
     * buffer array of length  {@code size}
     * is created and stored in {@code buf}.
     *
     * @param   in     the underlying input stream.
     * @param   size   the buffer size.
     * @throws  IllegalArgumentException if {@code size <= 0}.
     */
    public KafkaBufferedInputStream(InputStream in, BufferSupplier bufferSupplier, int size) {
        super(in);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        this.bufferSupplier = bufferSupplier;
        this.buffer = this.bufferSupplier.get(size);
        this.buf = this.buffer.array(); //implicitly means off-heap buffers are not supported.
    }

    /**
     * Fills the buffer with more data, taking into account
     * shuffling and other tricks for dealing with marks.
     * Assumes that it is being called by a synchronized method.
     * This method also assumes that all data has already been read in,
     * hence pos > count.
     */
    private void fill() throws IOException {
        byte[] buffer = getBufIfOpen();
        pos = 0;            /* no mark: throw away the buffer */
        count = pos;
        int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
        if (n > 0)
            count = n + pos;
    }

    /**
     * See
     * the general contract of the {@code read}
     * method of {@code InputStream}.
     *
     * @return     the next byte of data, or {@code -1} if the end of the
     *             stream is reached.
     * @throws     IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     * @see        FilterInputStream#in
     */
    public synchronized int read() throws IOException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                return -1;
        }
        return getBufIfOpen()[pos++] & 0xff;
    }

    /**
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     */
    private int read1(byte[] b, int off, int len) throws IOException {
        int avail = count - pos;
        if (avail <= 0) {
            /* If the requested length is at least as large as the buffer, and
               if there is no mark/reset activity, do not bother to copy the
               bytes into the local buffer.  In this way buffered streams will
               cascade harmlessly. */
            if (len >= getBufIfOpen().length) {
                return getInIfOpen().read(b, off, len);
            }
            fill();
            avail = count - pos;
            if (avail <= 0) return -1;
        }
        int cnt = (avail < len) ? avail : len;
        System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
        pos += cnt;
        return cnt;
    }

    /**
     * Reads bytes from this byte-input stream into the specified byte array,
     * starting at the given offset.
     *
     * <p> This method implements the general contract of the corresponding
     * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
     * the <code>{@link InputStream}</code> class.  As an additional
     * convenience, it attempts to read as many bytes as possible by repeatedly
     * invoking the {@code read} method of the underlying stream.  This
     * iterated {@code read} continues until one of the following
     * conditions becomes true: <ul>
     *
     *   <li> The specified number of bytes have been read,
     *
     *   <li> The {@code read} method of the underlying stream returns
     *   {@code -1}, indicating end-of-file, or
     *
     *   <li> The {@code available} method of the underlying stream
     *   returns zero, indicating that further input requests would block.
     *
     * </ul> If the first {@code read} on the underlying stream returns
     * {@code -1} to indicate end-of-file then this method returns
     * {@code -1}.  Otherwise this method returns the number of bytes
     * actually read.
     *
     * <p> Subclasses of this class are encouraged, but not required, to
     * attempt to read as many bytes as possible in the same fashion.
     *
     * @param      b     destination buffer.
     * @param      off   offset at which to start storing bytes.
     * @param      len   maximum number of bytes to read.
     * @return     the number of bytes read, or {@code -1} if the end of
     *             the stream has been reached.
     * @throws     IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        getBufIfOpen(); // Check for closed stream
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (;;) {
            int nread = read1(b, off + n, len - n);
            if (nread <= 0)
                return (n == 0) ? nread : n;
            n += nread;
            if (n >= len)
                return n;
            // if not closed but no bytes available, return
            InputStream input = in;
            if (input != null && input.available() <= 0)
                return n;
        }
    }

    /**
     * See the general contract of the {@code skip}
     * method of {@code InputStream}.
     *
     * @throws IOException  if this input stream has been closed by
     *                      invoking its {@link #close()} method,
     *                      {@code in.skip(n)} throws an IOException,
     *                      or an I/O error occurs.
     */
    public synchronized long skip(long n) throws IOException {
        getBufIfOpen(); // Check for closed stream
        if (n <= 0) {
            return 0;
        }
        long avail = count - pos;

        if (avail <= 0) {
            // If no mark position set then don't keep in buffer
            return getInIfOpen().skip(n);
        }

        long skipped = (avail < n) ? avail : n;
        pos += skipped;
        return skipped;
    }

    /**
     * Returns an estimate of the number of bytes that can be read (or
     * skipped over) from this input stream without blocking by the next
     * invocation of a method for this input stream. The next invocation might be
     * the same thread or another thread.  A single read or skip of this
     * many bytes will not block, but may read or skip fewer bytes.
     * <p>
     * This method returns the sum of the number of bytes remaining to be read in
     * the buffer (<code>count&nbsp;- pos</code>) and the result of calling the
     * {@link FilterInputStream#in in}.available().
     *
     * @return     an estimate of the number of bytes that can be read (or skipped
     *             over) from this input stream without blocking.
     * @throws     IOException  if this input stream has been closed by
     *                          invoking its {@link #close()} method,
     *                          or an I/O error occurs.
     */
    public synchronized int available() throws IOException {
        int n = count - pos;
        int avail = getInIfOpen().available();
        return n > (Integer.MAX_VALUE - avail)
                    ? Integer.MAX_VALUE
                    : n + avail;
    }

    /**
     * See the general contract of the {@code mark}
     * method of {@code InputStream}.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position becomes invalid.
     * @see     KafkaBufferedInputStream#reset()
     */
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark/reset not supported");
    }

    /**
     * See the general contract of the {@code reset}
     * method of {@code InputStream}.
     * <p>
     * If {@code markpos} is {@code -1}
     * (no mark has been set or the mark has been
     * invalidated), an {@code IOException}
     * is thrown. Otherwise, {@code pos} is
     * set equal to {@code markpos}.
     *
     * @throws     IOException  if this stream has not been marked or,
     *                  if the mark has been invalidated, or the stream
     *                  has been closed by invoking its {@link #close()}
     *                  method, or an I/O error occurs.
     * @see        KafkaBufferedInputStream#mark(int)
     */
    public synchronized void reset() throws IOException {
        getBufIfOpen(); // Cause exception if closed
        throw new UnsupportedOperationException("mark/reset not supported");
    }

    /**
     * Tests if this input stream supports the {@code mark}
     * and {@code reset} methods. we dont
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
     * Closes this input stream and releases any system resources
     * associated with the stream.
     * Once the stream has been closed, further read(), available(), reset(),
     * or skip() invocations will throw an IOException.
     * Closing a previously closed stream has no effect.
     *
     * @throws     IOException  if an I/O error occurs.
     */
    public void close() throws IOException {
        byte[] buffer;
        while ((buffer = buf) != null) {
            if (BUF_FIELD_UPDATER.compareAndSet(this, buffer, null)) {
                this.bufferSupplier.release(this.buffer);
                this.buffer = null; //this.buf has been nulled above
                InputStream input = in;
                in = null;
                if (input != null)
                    input.close();
                return;
            }
            // Else retry in case a new buf was CASed in fill()
        }
    }
}
