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

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * ChunkedBytesStream is a copy of {@link BufferedInputStream} with the following differences:
 * - Unlike {@link java.io.BufferedInputStream#skip(long)} this class could be configured to not push skip() to
 * input stream. We may want to avoid pushing this to input stream because it's implementation maybe inefficient,
 * e.g. the case of ZstdInputStream which allocates a new buffer from buffer pool, per skip call.
 * - Unlike {@link java.io.BufferedInputStream}, which allocates an intermediate buffer, this uses a buffer supplier to
 * create the intermediate buffer.
 * - Unlike {@link java.io.BufferedInputStream}, this implementation does not support {@link InputStream#mark(int)} and
 * {@link InputStream#markSupported()} will return false.
 * <p>
 * Note that:
 * - this class is not thread safe and shouldn't be used in scenarios where multiple threads access this.
 * - the implementation of this class is performance sensitive. Minor changes such as usage of ByteBuffer instead of byte[]
 * can significantly impact performance, hence, proceed with caution.
 */
public class ChunkedBytesStream extends FilterInputStream {
    /**
     * Supplies the ByteBuffer which is used as intermediate buffer to store the chunk of output data.
     */
    private final BufferSupplier bufferSupplier;
    /**
     * Intermediate buffer to store the chunk of output data. The ChunkedBytesStream is considered closed if
     * this buffer is null.
     */
    private byte[] intermediateBuf;
    /**
     * The index one greater than the index of the last valid byte in
     * the buffer.
     * This value is always in the range <code>0</code> through <code>intermediateBuf.length</code>;
     * elements <code>intermediateBuf[0]</code>  through <code>intermediateBuf[count-1]
     * </code>contain buffered input data obtained
     * from the underlying  input stream.
     */
    protected int count = 0;
    /**
     * The current position in the buffer. This is the index of the next
     * character to be read from the <code>buf</code> array.
     * <p>
     * This value is always in the range <code>0</code>
     * through <code>count</code>. If it is less
     * than <code>count</code>, then  <code>intermediateBuf[pos]</code>
     * is the next byte to be supplied as input;
     * if it is equal to <code>count</code>, then
     * the  next <code>read</code> or <code>skip</code>
     * operation will require more bytes to be
     * read from the contained  input stream.
     */
    protected int pos = 0;
    /**
     * Reference for the intermediate buffer. This reference is only kept for releasing the buffer from the
     * buffer supplier.
     */
    private final ByteBuffer intermediateBufRef;
    /**
     * If this flag is true, we will delegate the responsibility of skipping to the
     * sourceStream. This is an alternative to reading the data from source stream, storing in an intermediate buffer and
     * skipping the values using this implementation.
     */
    private final boolean delegateSkipToSourceStream;

    public ChunkedBytesStream(InputStream in, BufferSupplier bufferSupplier, int intermediateBufSize, boolean delegateSkipToSourceStream) {
        super(in);
        this.bufferSupplier = bufferSupplier;
        intermediateBufRef = bufferSupplier.get(intermediateBufSize);
        if (!intermediateBufRef.hasArray() || (intermediateBufRef.arrayOffset() != 0)) {
            throw new IllegalArgumentException("provided ByteBuffer lacks array or has non-zero arrayOffset");
        }
        intermediateBuf = intermediateBufRef.array();
        this.delegateSkipToSourceStream = delegateSkipToSourceStream;
    }

    /**
     * Check to make sure that buffer has not been nulled out due to
     * close; if not return it;
     */
    private byte[] getBufIfOpen() throws IOException {
        byte[] buffer = intermediateBuf;
        if (buffer == null)
            throw new IOException("Stream closed");
        return buffer;
    }

    /**
     * See the general contract of the <code>read</code>
     * method of <code>InputStream</code>.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     * stream is reached.
     * @throws IOException if this input stream has been closed by
     *                     invoking its {@link #close()} method,
     *                     or an I/O error occurs.
     * @see BufferedInputStream#read()
     */
    @Override
    public int read() throws IOException {
        if (pos >= count) {
            fill();
            if (pos >= count)
                return -1;
        }

        return getBufIfOpen()[pos++] & 0xff;
    }

    /**
     * Check to make sure that underlying input stream has not been
     * nulled out due to close; if not return it;
     */
    InputStream getInIfOpen() throws IOException {
        InputStream input = in;
        if (input == null)
            throw new IOException("Stream closed");
        return input;
    }

    /**
     * Fills the intermediate buffer with more data. The amount of new data read is equal to the remaining empty space
     * in the buffer. For optimal performance, read as much data as possible in this call.
     * This method also assumes that all data has already been read in, hence pos > count.
     */
    int fill() throws IOException {
        byte[] buffer = getBufIfOpen();
        pos = 0;
        count = pos;
        int n = getInIfOpen().read(buffer, pos, buffer.length - pos);
        if (n > 0)
            count = n + pos;
        return n;
    }

    @Override
    public void close() throws IOException {
        byte[] mybuf = intermediateBuf;
        intermediateBuf = null;

        InputStream input = in;
        in = null;

        if (mybuf != null)
            bufferSupplier.release(intermediateBufRef);
        if (input != null)
            input.close();
    }

    /**
     * Reads bytes from this byte-input stream into the specified byte array,
     * starting at the given offset.
     *
     * <p> This method implements the general contract of the corresponding
     * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
     * the <code>{@link InputStream}</code> class.  As an additional
     * convenience, it attempts to read as many bytes as possible by repeatedly
     * invoking the <code>read</code> method of the underlying stream.  This
     * iterated <code>read</code> continues until one of the following
     * conditions becomes true: <ul>
     *
     * <li> The specified number of bytes have been read,
     *
     * <li> The <code>read</code> method of the underlying stream returns
     * <code>-1</code>, indicating end-of-file, or
     *
     * <li> The <code>available</code> method of the underlying stream
     * returns zero, indicating that further input requests would block.
     *
     * </ul> If the first <code>read</code> on the underlying stream returns
     * <code>-1</code> to indicate end-of-file then this method returns
     * <code>-1</code>.  Otherwise this method returns the number of bytes
     * actually read.
     *
     * <p> Subclasses of this class are encouraged, but not required, to
     * attempt to read as many bytes as possible in the same fashion.
     *
     * @param b   destination buffer.
     * @param off offset at which to start storing bytes.
     * @param len maximum number of bytes to read.
     * @return the number of bytes read, or <code>-1</code> if the end of
     * the stream has been reached.
     * @throws IOException if this input stream has been closed by
     *                     invoking its {@link #close()} method,
     *                     or an I/O error occurs.
     * @see BufferedInputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        getBufIfOpen(); // Check for closed stream
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int n = 0;
        for (; ; ) {
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
     * Read characters into a portion of an array, reading from the underlying
     * stream at most once if necessary.
     * <p>
     * Note - Implementation copied from {@link BufferedInputStream}. Slight modification done to remove
     * the mark position.
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
     * Skips over and discards exactly {@code n} bytes of data from this input stream.
     * If {@code n} is zero, then no bytes are skipped.
     * If {@code n} is negative, then no bytes are skipped.
     * <p>
     * This method blocks until the requested number of bytes has been skipped, end of file is reached, or an
     * exception is thrown.
     * <p> If end of stream is reached before the stream is at the desired position, then the bytes skipped till than pointed
     * are returned.
     * <p> If an I/O error occurs, then the input stream may be in an inconsistent state. It is strongly recommended that the
     * stream be promptly closed if an I/O error occurs.
     * <p>
     * This method first skips and discards bytes in the intermediate buffer.
     * After that, depending on the value of {@link #delegateSkipToSourceStream}, it either delegates the skipping of
     * bytes to the sourceStream or it reads the data from input stream in chunks, copies the data into intermediate
     * buffer and skips it.
     * <p>
     * Starting JDK 12, a new method was introduced in InputStream, skipNBytes which has a similar behaviour as
     * this method.
     *
     * @param toSkip the number of bytes to be skipped.
     * @return the actual number of bytes skipped which might be zero.
     * @throws IOException if this input stream has been closed by invoking its {@link #close()} method,
     *                     {@code in.skip(n)} throws an IOException, or an I/O error occurs.
     */
    @Override
    public long skip(long toSkip) throws IOException {
        getBufIfOpen(); // Check for closed stream
        if (toSkip <= 0) {
            return 0;
        }

        long remaining = toSkip;

        // Skip bytes stored in intermediate buffer first
        int avail = count - pos;
        int bytesSkipped = (int) Math.min(avail, remaining);
        pos += bytesSkipped;
        remaining -= bytesSkipped;

        while (remaining > 0) {
            if (delegateSkipToSourceStream) {
                // Use sourceStream's skip() to skip the rest.
                // conversion to int is acceptable because toSkip and remaining are int.
                long delegateBytesSkipped = getInIfOpen().skip(remaining);
                if (delegateBytesSkipped == 0) {
                    // read one byte to check for EOS
                    if (read() == -1) {
                        break;
                    }
                    // one byte read so decrement number to skip
                    remaining--;
                } else if (delegateBytesSkipped > remaining || delegateBytesSkipped < 0) { // skipped negative or too many bytes
                    throw new IOException("Unable to skip exactly");

                }
                remaining -= delegateBytesSkipped;
            } else {
                // skip from intermediate buffer, filling it first (if required)
                if (pos >= count) {
                    fill();
                    // if we don't have data in intermediate buffer after fill, then stop skipping
                    if (pos >= count)
                        break;
                }
                avail = count - pos;
                bytesSkipped = (int) Math.min(avail, remaining);
                pos += bytesSkipped;
                remaining -= bytesSkipped;
            }
        }
        return toSkip - remaining;
    }

    // visible for testing
    public InputStream sourceStream() {
        return in;
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
     * {@link java.io.FilterInputStream#in in}.available().
     *
     * @return an estimate of the number of bytes that can be read (or skipped
     * over) from this input stream without blocking.
     * @throws IOException if this input stream has been closed by
     *                     invoking its {@link #close()} method,
     *                     or an I/O error occurs.
     * @see BufferedInputStream#available()
     */
    @Override
    public synchronized int available() throws IOException {
        int n = count - pos;
        int avail = getInIfOpen().available();
        return n > (Integer.MAX_VALUE - avail)
            ? Integer.MAX_VALUE
            : n + avail;
    }
}
