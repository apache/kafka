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

package org.apache.kafka.common.network;

import java.io.IOException;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.CancelledKeyException;

import java.security.Principal;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Transport layer for SSL communication
 */

public class SSLTransportLayer implements TransportLayer {
    private static final Logger log = LoggerFactory.getLogger(SSLTransportLayer.class);
    protected SSLEngine sslEngine;
    private SelectionKey key;
    private SocketChannel socketChannel;
    private HandshakeStatus handshakeStatus;
    private SSLEngineResult handshakeResult;
    private boolean handshakeComplete = false;
    private boolean closed = false;
    private boolean closing = false;
    private ByteBuffer netReadBuffer;
    private ByteBuffer netWriteBuffer;
    private ByteBuffer appReadBuffer;
    private ByteBuffer emptyBuf = ByteBuffer.allocate(0);
    private int interestOps;
    private int socketSendBufferSize;

    public SSLTransportLayer(SelectionKey key, SSLEngine sslEngine) throws IOException {
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
        this.sslEngine = sslEngine;
        this.netReadBuffer = ByteBuffer.allocateDirect(packetBufferSize());
        this.netWriteBuffer = ByteBuffer.allocateDirect(packetBufferSize());
        this.appReadBuffer = ByteBuffer.allocateDirect(applicationBufferSize());
        this.socketSendBufferSize = this.socketChannel.socket().getSendBufferSize();
        startHandshake();
    }

    /**
     * starts sslEngine handshake process and sets the selectionKey interestOps based
     * sslEngine handshakeStatus.
     */
    private void startHandshake() throws IOException {
        netWriteBuffer.position(0);
        netWriteBuffer.limit(0);
        netReadBuffer.position(0);
        netReadBuffer.limit(0);
        handshakeComplete = false;
        closed = false;
        closing = false;
        addInterestOps(SelectionKey.OP_READ);
        //initiate handshake
        sslEngine.beginHandshake();
        handshakeStatus = sslEngine.getHandshakeStatus();
        if (handshakeStatus == HandshakeStatus.NEED_WRAP)
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }


    public boolean ready() {
        return handshakeComplete;
    }

    /**
     * does socketChannel.finishConnect()
     */
    public void finishConnect() throws IOException {
        socketChannel.finishConnect();
        removeInterestOps(SelectionKey.OP_CONNECT);
        key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
    }

    /**
     * disconnects selectionKey.
     */
    public void disconnect() {
        key.cancel();
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    /**
    * Sends a SSL close message and closes socketChannel.
    * @throws IOException if an I/O error occurs
    * @throws IOException if there is data on the outgoing network buffer and we are unable to flush it
    */
    public void close() throws IOException {
        if (closing) return;
        closing = true;
        sslEngine.closeOutbound();

        if (!flush(netWriteBuffer)) {
            throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
        }
        //prep the buffer for the close message
        netWriteBuffer.clear();
        //perform the close, since we called sslEngine.closeOutbound
        SSLEngineResult handshake = sslEngine.wrap(emptyBuf, netWriteBuffer);
        //we should be in a close state
        if (handshake.getStatus() != SSLEngineResult.Status.CLOSED) {
            throw new IOException("Invalid close state, will not send network data.");
        }
        netWriteBuffer.flip();
        flush(netWriteBuffer);
        socketChannel.socket().close();
        socketChannel.close();
        closed = !netWriteBuffer.hasRemaining() && (handshake.getHandshakeStatus() != HandshakeStatus.NEED_WRAP);
    }

    /**
     * returns true if there are any pending contents in netWriteBuffer
     */
    public boolean pending() {
        return netWriteBuffer.hasRemaining();
    }

    /**
    * Flushes the buffer to the network, non blocking
    * @param buf ByteBuffer
    * @return boolean true if the buffer has been emptied out, false otherwise
    * @throws IOException
    */
    private boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = socketChannel.write(buf);
            return written >= remaining;
        }
        return true;
    }

    /**
    * Performs SSL handshake, non blocking.
    * @throws IOException
    */
    public void handshake() throws IOException {
        boolean read = key.isReadable();
        boolean write = key.isWritable();
        handshakeComplete = false;
        handshakeStatus = sslEngine.getHandshakeStatus();
        if (!flush(netWriteBuffer)) {
            key.interestOps(SelectionKey.OP_WRITE);
            return;
        }
        try {
            switch (handshakeStatus) {
                case NEED_TASK:
                    handshakeStatus = runDelegatedTasks();
                    break;
                case NEED_WRAP:
                    handshakeResult = handshakeWrap(write);
                    if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                        int currentPacketBufferSize = packetBufferSize();
                        netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, currentPacketBufferSize);
                        if (netWriteBuffer.position() >= currentPacketBufferSize) {
                            throw new IllegalStateException("Buffer overflow when available data (" + netWriteBuffer.position() +
                                                            ") >= network buffer size (" + currentPacketBufferSize + ")");
                        }
                    } else if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                        throw new IllegalStateException("Should not have received BUFFER_UNDERFLOW during handshake WRAP.");
                    } else if (handshakeResult.getStatus() == Status.CLOSED) {
                        throw new EOFException();
                    }
                    //fall down to NEED_UNWRAP on the same call, will result in a
                    //BUFFER_UNDERFLOW if it needs data
                    if (handshakeStatus != HandshakeStatus.NEED_UNWRAP ||  (!write && !flush(netWriteBuffer))) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        break;
                    }
                case NEED_UNWRAP:
                    handshakeResult = handshakeUnwrap(read);
                    if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                        int currentPacketBufferSize = packetBufferSize();
                        netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentPacketBufferSize);
                        if (netReadBuffer.position() >= currentPacketBufferSize) {
                            throw new IllegalStateException("Buffer underflow when there is available data");
                        }
                    } else if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                        int currentAppBufferSize = applicationBufferSize();
                        netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentAppBufferSize);
                        if (netReadBuffer.position() > currentAppBufferSize) {
                            throw new IllegalStateException("Buffer underflow when available data (" + netReadBuffer.position() +
                                                            ") > packet buffer size (" + currentAppBufferSize + ")");
                        }
                    } else if (handshakeResult.getStatus() == Status.CLOSED) {
                        throw new EOFException("SSL handshake status CLOSED during handshake UNWRAP");
                    }
                    //if handshakeStatus completed than fall-through to finished status.
                    //after handshake is finished there is no data left to read/write in socketChannel.
                    //so the selector won't invoke this channel if we don't go through the handshakeFinished here.
                    if (handshakeStatus != HandshakeStatus.FINISHED) {
                        if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        } else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                        }
                        break;
                    }
                case FINISHED:
                    handshakeFinished();
                    break;
                case NOT_HANDSHAKING:
                    handshakeFinished();
                    break;
                default:
                    throw new IllegalStateException(String.format("Unexpected status [%s]", handshakeStatus));
            }
        } catch (SSLException e) {
            handshakeFailure();
            throw e;
        }
    }


    /**
     * Executes the SSLEngine tasks needed.
     * @return HandshakeStatus
     */
    private HandshakeStatus runDelegatedTasks() {
        for (;;) {
            Runnable task = delegatedTask();
            if (task == null) {
                break;
            }
            task.run();
        }
        return sslEngine.getHandshakeStatus();
    }

    /**
     * Checks if the handshake status is finished
     * Sets the interestOps for the selectionKey.
     */
    private void handshakeFinished() throws IOException {
        // SSLEnginge.getHandshakeStatus is transient and it doesn't record FINISHED status properly.
        // It can move from FINISHED status to NOT_HANDSHAKING after the handshake is completed.
        // Hence we also need to check handshakeResult.getHandshakeStatus() if the handshake finished or not
        if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
            //we are complete if we have delivered the last package
            handshakeComplete = !netWriteBuffer.hasRemaining();
            //set interestOps if we are complete, otherwise we still have data to write
            if (handshakeComplete)
                key.interestOps(interestOps);
            else
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } else {
            throw new IOException("NOT_HANDSHAKING during handshake");
        }
    }

    /**
    * Performs the WRAP function
    * @param doWrite boolean
    * @return SSLEngineResult
    * @throws IOException
    */
    private SSLEngineResult  handshakeWrap(Boolean doWrite) throws IOException {
        if (netWriteBuffer.hasRemaining())
            throw new IllegalStateException("handshakeWrap called with netWriteBuffer not empty");
        //this should never be called with a network buffer that contains data
        //so we can clear it here.
        netWriteBuffer.clear();
        SSLEngineResult result = sslEngine.wrap(emptyBuf, netWriteBuffer);
        //prepare the results to be written
        netWriteBuffer.flip();
        handshakeStatus = result.getHandshakeStatus();
        if (result.getStatus() == SSLEngineResult.Status.OK &&
            result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
            handshakeStatus = runDelegatedTasks();
        }
        //optimization, if we do have a writable channel, write it now
        if (doWrite) flush(netWriteBuffer);
        return result;
    }

    /**
    * Perform handshake unwrap
    * @param doRead boolean
    * @return SSLEngineResult
    * @throws IOException
    */
    private SSLEngineResult handshakeUnwrap(Boolean doRead) throws IOException {
        if (netReadBuffer.position() == netReadBuffer.limit()) {
            //clear the buffer if we have emptied it out on data
            netReadBuffer.clear();
        }

        if (doRead)  {
            int read = socketChannel.read(netReadBuffer);
            if (read == -1) throw new IOException("EOF during handshake.");
        }
        SSLEngineResult result;
        boolean cont = false;
        do {
            //prepare the buffer with the incoming data
            netReadBuffer.flip();
            result = sslEngine.unwrap(netReadBuffer, appReadBuffer);
            netReadBuffer.compact();
            handshakeStatus = result.getHandshakeStatus();
            if (result.getStatus() == SSLEngineResult.Status.OK &&
                 result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
                handshakeStatus = runDelegatedTasks();
            }
            cont = result.getStatus() == SSLEngineResult.Status.OK &&
                handshakeStatus == HandshakeStatus.NEED_UNWRAP;
        } while (cont);
        return result;
    }




    /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    */
    public int read(ByteBuffer dst) throws IOException {
        if (closing || closed) return -1;
        int read = 0;
        if (!handshakeComplete) return read;

        //if we have unread decrypted data in appReadBuffer read that into dst buffer.
        if (appReadBuffer.position() > 0) {
            read = readFromAppBuffer(dst);
        }

        if (dst.remaining() > 0) {
            netReadBuffer = Utils.ensureCapacity(netReadBuffer, packetBufferSize());
            if (netReadBuffer.remaining() > 0) {
                int netread = socketChannel.read(netReadBuffer);
                if (netread == 0) return netread;
            }
            do {
                netReadBuffer.flip();
                SSLEngineResult unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);
                netReadBuffer.compact();
                // handle ssl renegotiation.
                if (unwrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
                    handshake();
                    break;
                }

                if (unwrapResult.getStatus() == Status.OK) {
                    read += readFromAppBuffer(dst);
                } else if (unwrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    int currentApplicationBufferSize = applicationBufferSize();
                    appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentApplicationBufferSize);
                    if (appReadBuffer.position() >= currentApplicationBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data (" + appReadBuffer.position() +
                                                        ") >= application buffer size (" + currentApplicationBufferSize + ")");
                    }
                    if (dst.hasRemaining())
                        read += readFromAppBuffer(dst);
                    else
                        break;
                } else if (unwrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    int currentPacketBufferSize = packetBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentPacketBufferSize);
                    if (netReadBuffer.position() >= currentPacketBufferSize) {
                        throw new IllegalStateException("Buffer underflow when available data (" + netReadBuffer.position() +
                                                        ") > packet buffer size (" + currentPacketBufferSize + ")");
                    }
                    break;
                } else if (unwrapResult.getStatus() == Status.CLOSED) {
                    throw new EOFException();
                }
            } while (netReadBuffer.position() != 0);
        }
        return read;
    }


    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }


    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @returns The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length))
            throw new IndexOutOfBoundsException();

        int totalRead = 0;
        int i = offset;
        while (i < length) {
            if (dsts[i].hasRemaining()) {
                int read = read(dsts[i]);
                if (read > 0) {
                    totalRead += read;
                }
            }
            if (!dsts[i].hasRemaining()) {
                i++;
            }
        }
        return totalRead;
    }


    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @returns The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    public int write(ByteBuffer src) throws IOException {
        int written = 0;
        if (closing || closed) throw new IOException("Channel is in closing state");
        if (!handshakeComplete) return written;

        if (!flush(netWriteBuffer))
            return written;

        netWriteBuffer.clear();
        SSLEngineResult wrap = sslEngine.wrap(src, netWriteBuffer);
        netWriteBuffer.flip();

        //handle ssl renegotiation
        if (wrap.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
            handshake();
            return written;
        }

        if (wrap.getStatus() == Status.OK) {
            written = wrap.bytesConsumed();
            flush(netWriteBuffer);
        } else if (wrap.getStatus() == Status.BUFFER_OVERFLOW) {
            int currentPacketBufferSize = packetBufferSize();
            netWriteBuffer = Utils.ensureCapacity(netReadBuffer, packetBufferSize());
            if (netWriteBuffer.position() >= currentPacketBufferSize)
                throw new IllegalStateException("SSL BUFFER_OVERFLOW when available data size (" + netWriteBuffer.position() + ") >= network buffer size (" + currentPacketBufferSize + ")");
        } else if (wrap.getStatus() == Status.BUFFER_UNDERFLOW) {
            throw new IllegalStateException("SSL BUFFER_UNDERFLOW during write");
        } else if (wrap.getStatus() == Status.CLOSED) {
            throw new EOFException();
        }
        return written;
    }

    /**
    * Writes a sequence of bytes to this channel from the subsequence of the given buffers.
    *
    * @param srcs The buffers from which bytes are to be retrieved
    * @param offset The offset within the buffer array of the first buffer from which bytes are to be retrieved; must be non-negative and no larger than srcs.length.
    * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than srcs.length - offset.
    * @return returns no.of bytes written , possibly zero.
    * @throws IOException If some other I/O error occurs
    */
    public long write(ByteBuffer[] srcs, int offset, int length)  throws IOException {
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();

        int totalWritten = 0;
        int i = offset;
        while (i < length) {
            if (srcs[i].hasRemaining() || pending()) {
                int written = write(srcs[i]);
                if (written > 0) {
                    totalWritten += written;
                }
            }
            if (!srcs[i].hasRemaining()) {
                i++;
            } else {
                // if we are unable to write the current buffer to socketChannel we should break,
                // as we might have reached max socket send buffer size.
                break;
            }
        }
        return totalWritten;
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffers.
    *
    * @param srcs The buffers from which bytes are to be retrieved
    * @return returns no.of bytes consumed by SSLEngine.wrap , possibly zero.
    * @throws IOException If some other I/O error occurs
    */

    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }


    /**
     * SSLSession's peerPrincipal for the remote host.
     * @return Principal
     */
    public Principal peerPrincipal() throws IOException {
        try {
            return sslEngine.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException se) {
            throw new IOException(String.format("Unable to retrieve getPeerPrincipal due to %s", se));
        }
    }

    /**
     * returns a SSL Session after the handshake is established
     * throws IlleagalStateException if the handshake is not established
     */
    public SSLSession sslSession() throws IllegalStateException {
        return sslEngine.getSession();
    }

    /**
     * Adds interestOps to SelecitonKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    public void addInterestOps(int ops) {
        interestOps |= ops;
        // if handshake is not complete and key is cancelled.
        // we should check for key.isValid.
        if (handshakeComplete)
            key.interestOps(interestOps);
        else if (!key.isValid())
            throw new CancelledKeyException();
    }

    /**
     * removes interestOps to SelecitonKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    public void removeInterestOps(int ops) {
        interestOps &= ~ops;
        // if handshake is not complete and key is cancelled.
        // we should check for key.isValid.
        if (handshakeComplete)
            key.interestOps(interestOps);
        else if (!key.isValid())
            throw new CancelledKeyException();
    }


    /**
     * returns delegatedTask for the SSLEngine.
     */
    protected Runnable delegatedTask() {
        return sslEngine.getDelegatedTask();
    }

    /**
     * transfers appReadBuffer contents (decrypted data) into dst bytebuffer
     * @param dst ByteBuffer
     */
    private int readFromAppBuffer(ByteBuffer dst) {
        appReadBuffer.flip();
        int remaining = Math.min(appReadBuffer.remaining(), dst.remaining());
        if (remaining > 0) {
            int limit = appReadBuffer.limit();
            appReadBuffer.limit(appReadBuffer.position() + remaining);
            dst.put(appReadBuffer);
            appReadBuffer.limit(limit);
        }
        appReadBuffer.compact();
        return remaining;
    }

    private int packetBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    private int applicationBufferSize() {
        return sslEngine.getSession().getApplicationBufferSize();
    }

    private void handshakeFailure() {
        //Release all resources such as internal buffers that SSLEngine is managing
        sslEngine.closeOutbound();
        try {
            sslEngine.closeInbound();
        } catch (SSLException e) {
            log.debug("SSLEngine.closeInBound() raised an exception.",  e);
        }
    }
}
