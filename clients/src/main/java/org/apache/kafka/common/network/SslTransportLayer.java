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
package org.apache.kafka.common.network;

import java.io.IOException;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.CancelledKeyException;

import java.security.Principal;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ByteBufferUnmapper;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/*
 * Transport layer for SSL communication
 *
 *
 * TLS v1.3 notes:
 *   https://tools.ietf.org/html/rfc8446#section-4.6 : Post-Handshake Messages
 *   "TLS also allows other messages to be sent after the main handshake.
 *   These messages use a handshake content type and are encrypted under
 *   the appropriate application traffic key."
 */
public class SslTransportLayer implements TransportLayer {
    private enum State {
        // Initial state
        NOT_INITIALIZED,
        // SSLEngine is in handshake mode
        HANDSHAKE,
        // SSL handshake failed, connection will be terminated
        HANDSHAKE_FAILED,
        // SSLEngine has completed handshake, post-handshake messages may be pending for TLSv1.3
        POST_HANDSHAKE,
        // SSLEngine has completed handshake, any post-handshake messages have been processed for TLSv1.3
        // For TLSv1.3, we move the channel to READY state when incoming data is processed after handshake
        READY,
        // Channel is being closed
        CLOSING
    }

    private static final String TLS13 = "TLSv1.3";

    private final String channelId;
    private final SSLEngine sslEngine;
    private final SelectionKey key;
    private final SocketChannel socketChannel;
    private final ChannelMetadataRegistry metadataRegistry;
    private final Logger log;

    private HandshakeStatus handshakeStatus;
    private SSLEngineResult handshakeResult;
    private State state;
    private SslAuthenticationException handshakeException;
    private ByteBuffer netReadBuffer;
    private ByteBuffer netWriteBuffer;
    private ByteBuffer appReadBuffer;
    private ByteBuffer fileChannelBuffer;
    private boolean hasBytesBuffered;

    public static SslTransportLayer create(String channelId, SelectionKey key, SSLEngine sslEngine,
                                           ChannelMetadataRegistry metadataRegistry) throws IOException {
        return new SslTransportLayer(channelId, key, sslEngine, metadataRegistry);
    }

    // Prefer `create`, only use this in tests
    SslTransportLayer(String channelId, SelectionKey key, SSLEngine sslEngine,
                      ChannelMetadataRegistry metadataRegistry) {
        this.channelId = channelId;
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
        this.sslEngine = sslEngine;
        this.state = State.NOT_INITIALIZED;
        this.metadataRegistry = metadataRegistry;

        final LogContext logContext = new LogContext(String.format("[SslTransportLayer channelId=%s key=%s] ", channelId, key));
        this.log = logContext.logger(getClass());
    }

    // Visible for testing
    protected void startHandshake() throws IOException {
        if (state != State.NOT_INITIALIZED)
            throw new IllegalStateException("startHandshake() can only be called once, state " + state);

        this.netReadBuffer = ByteBuffer.allocate(netReadBufferSize());
        this.netWriteBuffer = ByteBuffer.allocate(netWriteBufferSize());
        this.appReadBuffer = ByteBuffer.allocate(applicationBufferSize());
        netWriteBuffer.limit(0);
        netReadBuffer.limit(0);

        state = State.HANDSHAKE;
        //initiate handshake
        sslEngine.beginHandshake();
        handshakeStatus = sslEngine.getHandshakeStatus();
    }

    @Override
    public boolean ready() {
        return state == State.POST_HANDSHAKE || state == State.READY;
    }

    /**
     * does socketChannel.finishConnect()
     */
    @Override
    public boolean finishConnect() throws IOException {
        boolean connected = socketChannel.finishConnect();
        if (connected)
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        return connected;
    }

    /**
     * disconnects selectionKey.
     */
    @Override
    public void disconnect() {
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    /**
    * Sends an SSL close message and closes socketChannel.
    */
    @Override
    public void close() throws IOException {
        State prevState = state;
        if (state == State.CLOSING) return;
        state = State.CLOSING;
        sslEngine.closeOutbound();
        try {
            if (prevState != State.NOT_INITIALIZED && isConnected()) {
                if (!flush(netWriteBuffer)) {
                    throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
                }
                //prep the buffer for the close message
                netWriteBuffer.clear();
                //perform the close, since we called sslEngine.closeOutbound
                SSLEngineResult wrapResult = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
                //we should be in a close state
                if (wrapResult.getStatus() != SSLEngineResult.Status.CLOSED) {
                    throw new IOException("Unexpected status returned by SSLEngine.wrap, expected CLOSED, received " +
                            wrapResult.getStatus() + ". Will not send close message to peer.");
                }
                netWriteBuffer.flip();
                flush(netWriteBuffer);
            }
        } catch (IOException ie) {
            log.debug("Failed to send SSL Close message", ie);
        } finally {
            socketChannel.socket().close();
            socketChannel.close();
            netReadBuffer = null;
            netWriteBuffer = null;
            appReadBuffer = null;
            if (fileChannelBuffer != null) {
                ByteBufferUnmapper.unmap("fileChannelBuffer", fileChannelBuffer);
                fileChannelBuffer = null;
            }
        }
    }

    /**
     * returns true if there are any pending contents in netWriteBuffer
     */
    @Override
    public boolean hasPendingWrites() {
        return netWriteBuffer.hasRemaining();
    }

    /**
     * Reads available bytes from socket channel to `netReadBuffer`.
     * Visible for testing.
     * @return  number of bytes read
     */
    protected int readFromSocketChannel() throws IOException {
        return socketChannel.read(netReadBuffer);
    }

    /**
    * Flushes the buffer to the network, non blocking.
    * Visible for testing.
    * @param buf ByteBuffer
    * @return boolean true if the buffer has been emptied out, false otherwise
    * @throws IOException
    */
    protected boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = socketChannel.write(buf);
            return written >= remaining;
        }
        return true;
    }

    /**
    * Performs SSL handshake, non blocking.
    * Before application data (kafka protocols) can be sent client & kafka broker must
    * perform ssl handshake.
    * During the handshake SSLEngine generates encrypted data that will be transported over socketChannel.
    * Each SSLEngine operation generates SSLEngineResult , of which SSLEngineResult.handshakeStatus field is used to
    * determine what operation needs to occur to move handshake along.
    * A typical handshake might look like this.
    * +-------------+----------------------------------+-------------+
    * |  client     |  SSL/TLS message                 | HSStatus    |
    * +-------------+----------------------------------+-------------+
    * | wrap()      | ClientHello                      | NEED_UNWRAP |
    * | unwrap()    | ServerHello/Cert/ServerHelloDone | NEED_WRAP   |
    * | wrap()      | ClientKeyExchange                | NEED_WRAP   |
    * | wrap()      | ChangeCipherSpec                 | NEED_WRAP   |
    * | wrap()      | Finished                         | NEED_UNWRAP |
    * | unwrap()    | ChangeCipherSpec                 | NEED_UNWRAP |
    * | unwrap()    | Finished                         | FINISHED    |
    * +-------------+----------------------------------+-------------+
    *
    * @throws IOException if read/write fails
    * @throws SslAuthenticationException if handshake fails with an {@link SSLException}
    */
    @Override
    public void handshake() throws IOException {
        if (state == State.NOT_INITIALIZED) {
            try {
                startHandshake();
            } catch (SSLException e) {
                maybeProcessHandshakeFailure(e, false, null);
            }
        }
        if (ready())
            throw renegotiationException();
        if (state == State.CLOSING)
            throw closingException();

        int read = 0;
        boolean readable = key.isReadable();
        try {
            // Read any available bytes before attempting any writes to ensure that handshake failures
            // reported by the peer are processed even if writes fail (since peer closes connection
            // if handshake fails)
            if (readable)
                read = readFromSocketChannel();

            doHandshake();
            if (ready())
                updateBytesBuffered(true);
        } catch (SSLException e) {
            maybeProcessHandshakeFailure(e, true, null);
        } catch (IOException e) {
            maybeThrowSslAuthenticationException();

            // This exception could be due to a write. If there is data available to unwrap in the buffer, or data available
            // in the socket channel to read and unwrap, process the data so that any SSL handshake exceptions are reported.
            try {
                do {
                    log.trace("Process any available bytes from peer, netReadBuffer {} netWriterBuffer {} handshakeStatus {} readable? {}",
                        netReadBuffer, netWriteBuffer, handshakeStatus, readable);
                    handshakeWrapAfterFailure(false);
                    handshakeUnwrap(false, true);
                } while (readable && readFromSocketChannel() > 0);
            } catch (SSLException e1) {
                maybeProcessHandshakeFailure(e1, false, e);
            }

            // If we get here, this is not a handshake failure, throw the original IOException
            throw e;
        }

        // Read from socket failed, so throw any pending handshake exception or EOF exception.
        if (read == -1) {
            maybeThrowSslAuthenticationException();
            throw new EOFException("EOF during handshake, handshake status is " + handshakeStatus);
        }
    }

    @SuppressWarnings("fallthrough")
    private void doHandshake() throws IOException {
        boolean read = key.isReadable();
        boolean write = key.isWritable();
        handshakeStatus = sslEngine.getHandshakeStatus();
        if (!flush(netWriteBuffer)) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            return;
        }
        // Throw any pending handshake exception since `netWriteBuffer` has been flushed
        maybeThrowSslAuthenticationException();

        switch (handshakeStatus) {
            case NEED_TASK:
                log.trace("SSLHandshake NEED_TASK channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                handshakeStatus = runDelegatedTasks();
                break;
            case NEED_WRAP:
                log.trace("SSLHandshake NEED_WRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                handshakeResult = handshakeWrap(write);
                if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    int currentNetWriteBufferSize = netWriteBufferSize();
                    netWriteBuffer.compact();
                    netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, currentNetWriteBufferSize);
                    netWriteBuffer.flip();
                    if (netWriteBuffer.limit() >= currentNetWriteBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data size (" + netWriteBuffer.limit() +
                                                        ") >= network buffer size (" + currentNetWriteBufferSize + ")");
                    }
                } else if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    throw new IllegalStateException("Should not have received BUFFER_UNDERFLOW during handshake WRAP.");
                } else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException();
                }
                log.trace("SSLHandshake NEED_WRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                       channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                //if handshake status is not NEED_UNWRAP or unable to flush netWriteBuffer contents
                //we will break here otherwise we can do need_unwrap in the same call.
                if (handshakeStatus != HandshakeStatus.NEED_UNWRAP || !flush(netWriteBuffer)) {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    break;
                }
            case NEED_UNWRAP:
                log.trace("SSLHandshake NEED_UNWRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
                do {
                    handshakeResult = handshakeUnwrap(read, false);
                    if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
                        int currentAppBufferSize = applicationBufferSize();
                        appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentAppBufferSize);
                        if (appReadBuffer.position() > currentAppBufferSize) {
                            throw new IllegalStateException("Buffer underflow when available data size (" + appReadBuffer.position() +
                                                           ") > packet buffer size (" + currentAppBufferSize + ")");
                        }
                    }
                } while (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW);
                if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("Buffer underflow when there is available data");
                    }
                } else if (handshakeResult.getStatus() == Status.CLOSED) {
                    throw new EOFException("SSL handshake status CLOSED during handshake UNWRAP");
                }
                log.trace("SSLHandshake NEED_UNWRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
                          channelId, handshakeResult, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());

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
    }

    private SSLHandshakeException renegotiationException() {
        return new SSLHandshakeException("Renegotiation is not supported");
    }

    private IllegalStateException closingException() {
        throw new IllegalStateException("Channel is in closing state");
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
        // SSLEngine.getHandshakeStatus is transient and it doesn't record FINISHED status properly.
        // It can move from FINISHED status to NOT_HANDSHAKING after the handshake is completed.
        // Hence we also need to check handshakeResult.getHandshakeStatus() if the handshake finished or not
        if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
            //we are complete if we have delivered the last packet
            //remove OP_WRITE if we are complete, otherwise we still have data to write
            if (netWriteBuffer.hasRemaining())
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            else {
                SSLSession session = sslEngine.getSession();
                state = session.getProtocol().equals(TLS13) ? State.POST_HANDSHAKE : State.READY;
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                log.debug("SSL handshake completed successfully with peerHost '{}' peerPort {} peerPrincipal '{}' protocol '{}' cipherSuite '{}'",
                        session.getPeerHost(), session.getPeerPort(), peerPrincipal(), session.getProtocol(), session.getCipherSuite());
                metadataRegistry.registerCipherInformation(
                    new CipherInformation(session.getCipherSuite(),  session.getProtocol()));
            }

            log.trace("SSLHandshake FINISHED channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} ",
                      channelId, appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
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
    private SSLEngineResult handshakeWrap(boolean doWrite) throws IOException {
        log.trace("SSLHandshake handshakeWrap {}", channelId);
        if (netWriteBuffer.hasRemaining())
            throw new IllegalStateException("handshakeWrap called with netWriteBuffer not empty");
        //this should never be called with a network buffer that contains data
        //so we can clear it here.
        netWriteBuffer.clear();
        SSLEngineResult result;
        try {
            result = sslEngine.wrap(ByteUtils.EMPTY_BUF, netWriteBuffer);
        } finally {
            //prepare the results to be written
            netWriteBuffer.flip();
        }
        handshakeStatus = result.getHandshakeStatus();
        if (result.getStatus() == SSLEngineResult.Status.OK &&
            result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
            handshakeStatus = runDelegatedTasks();
        }

        if (doWrite) flush(netWriteBuffer);
        return result;
    }

    /**
     * Perform handshake unwrap
     * @param doRead boolean If true, read more from the socket channel
     * @param ignoreHandshakeStatus If true, continue to unwrap if data available regardless of handshake status
     * @return SSLEngineResult
     * @throws IOException
     */
    private SSLEngineResult handshakeUnwrap(boolean doRead, boolean ignoreHandshakeStatus) throws IOException {
        log.trace("SSLHandshake handshakeUnwrap {}", channelId);
        SSLEngineResult result;
        int read = 0;
        if (doRead)
            read = readFromSocketChannel();
        boolean cont;
        do {
            //prepare the buffer with the incoming data
            int position = netReadBuffer.position();
            netReadBuffer.flip();
            result = sslEngine.unwrap(netReadBuffer, appReadBuffer);
            netReadBuffer.compact();
            handshakeStatus = result.getHandshakeStatus();
            if (result.getStatus() == SSLEngineResult.Status.OK &&
                result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
                handshakeStatus = runDelegatedTasks();
            }
            cont = (result.getStatus() == SSLEngineResult.Status.OK &&
                    handshakeStatus == HandshakeStatus.NEED_UNWRAP) ||
                    (ignoreHandshakeStatus && netReadBuffer.position() != position);
            log.trace("SSLHandshake handshakeUnwrap: handshakeStatus {} status {}", handshakeStatus, result.getStatus());
        } while (netReadBuffer.position() != 0 && cont);

        // Throw EOF exception for failed read after processing already received data
        // so that handshake failures are reported correctly
        if (read == -1)
            throw new EOFException("EOF during handshake, handshake status is " + handshakeStatus);

        return result;
    }


    /**
    * Reads a sequence of bytes from this channel into the given buffer. Reads as much as possible
    * until either the dst buffer is full or there is no more data in the socket.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    *         and no more data is available
    * @throws IOException if some other I/O error occurs
    */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (state == State.CLOSING) return -1;
        else if (!ready()) return 0;

        //if we have unread decrypted data in appReadBuffer read that into dst buffer.
        int read = 0;
        if (appReadBuffer.position() > 0) {
            read = readFromAppBuffer(dst);
        }

        boolean readFromNetwork = false;
        boolean isClosed = false;
        // Each loop reads at most once from the socket.
        while (dst.remaining() > 0) {
            int netread = 0;
            netReadBuffer = Utils.ensureCapacity(netReadBuffer, netReadBufferSize());
            if (netReadBuffer.remaining() > 0) {
                netread = readFromSocketChannel();
                if (netread > 0)
                    readFromNetwork = true;
            }

            while (netReadBuffer.position() > 0) {
                netReadBuffer.flip();
                SSLEngineResult unwrapResult;
                try {
                    unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);
                    if (state == State.POST_HANDSHAKE && appReadBuffer.position() != 0) {
                        // For TLSv1.3, we have finished processing post-handshake messages since we are now processing data
                        state = State.READY;
                    }
                } catch (SSLException e) {
                    // For TLSv1.3, handle SSL exceptions while processing post-handshake messages as authentication exceptions
                    if (state == State.POST_HANDSHAKE) {
                        state = State.HANDSHAKE_FAILED;
                        throw new SslAuthenticationException("Failed to process post-handshake messages", e);
                    } else
                        throw e;
                }
                netReadBuffer.compact();
                // reject renegotiation if TLS < 1.3, key updates for TLS 1.3 are allowed
                if (unwrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                        unwrapResult.getHandshakeStatus() != HandshakeStatus.FINISHED &&
                        unwrapResult.getStatus() == Status.OK &&
                        !sslEngine.getSession().getProtocol().equals(TLS13)) {
                    log.error("Renegotiation requested, but it is not supported, channelId {}, " +
                        "appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} handshakeStatus {}", channelId,
                        appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position(), unwrapResult.getHandshakeStatus());
                    throw renegotiationException();
                }

                if (unwrapResult.getStatus() == Status.OK) {
                    read += readFromAppBuffer(dst);
                } else if (unwrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                    int currentApplicationBufferSize = applicationBufferSize();
                    appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentApplicationBufferSize);
                    if (appReadBuffer.position() >= currentApplicationBufferSize) {
                        throw new IllegalStateException("Buffer overflow when available data size (" + appReadBuffer.position() +
                                                        ") >= application buffer size (" + currentApplicationBufferSize + ")");
                    }

                    // appReadBuffer will extended upto currentApplicationBufferSize
                    // we need to read the existing content into dst before we can do unwrap again. If there are no space in dst
                    // we can break here.
                    if (dst.hasRemaining())
                        read += readFromAppBuffer(dst);
                    else
                        break;
                } else if (unwrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                    int currentNetReadBufferSize = netReadBufferSize();
                    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
                    if (netReadBuffer.position() >= currentNetReadBufferSize) {
                        throw new IllegalStateException("Buffer underflow when available data size (" + netReadBuffer.position() +
                                                        ") > packet buffer size (" + currentNetReadBufferSize + ")");
                    }
                    break;
                } else if (unwrapResult.getStatus() == Status.CLOSED) {
                    // If data has been read and unwrapped, return the data. Close will be handled on the next poll.
                    if (appReadBuffer.position() == 0 && read == 0)
                        throw new EOFException();
                    else {
                        isClosed = true;
                        break;
                    }
                }
            }
            if (read == 0 && netread < 0)
                throw new EOFException("EOF during read");
            if (netread <= 0 || isClosed)
                break;
        }
        updateBytesBuffered(readFromNetwork || read > 0);
        // If data has been read and unwrapped, return the data even if end-of-stream, channel will be closed
        // on a subsequent poll.
        return read;
    }


    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }


    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length))
            throw new IndexOutOfBoundsException();

        int totalRead = 0;
        int i = offset;
        while (i < length) {
            if (dsts[i].hasRemaining()) {
                int read = read(dsts[i]);
                if (read > 0)
                    totalRead += read;
                else
                    break;
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
    * @return The number of bytes read from src, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public int write(ByteBuffer src) throws IOException {
        if (state == State.CLOSING)
            throw closingException();
        if (!ready())
            return 0;

        int written = 0;
        while (flush(netWriteBuffer) && src.hasRemaining()) {
            netWriteBuffer.clear();
            SSLEngineResult wrapResult = sslEngine.wrap(src, netWriteBuffer);
            netWriteBuffer.flip();

            // reject renegotiation if TLS < 1.3, key updates for TLS 1.3 are allowed
            if (wrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING &&
                    wrapResult.getStatus() == Status.OK &&
                    !sslEngine.getSession().getProtocol().equals(TLS13)) {
                throw renegotiationException();
            }

            if (wrapResult.getStatus() == Status.OK) {
                written += wrapResult.bytesConsumed();
            } else if (wrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
                // BUFFER_OVERFLOW means that the last `wrap` call had no effect, so we expand the buffer and try again
                netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, netWriteBufferSize());
                netWriteBuffer.position(netWriteBuffer.limit());
            } else if (wrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                throw new IllegalStateException("SSL BUFFER_UNDERFLOW during write");
            } else if (wrapResult.getStatus() == Status.CLOSED) {
                throw new EOFException();
            }
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
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();
        int totalWritten = 0;
        int i = offset;
        while (i < length) {
            if (srcs[i].hasRemaining() || hasPendingWrites()) {
                int written = write(srcs[i]);
                if (written > 0) {
                    totalWritten += written;
                }
            }
            if (!srcs[i].hasRemaining() && !hasPendingWrites()) {
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
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }


    /**
     * SSLSession's peerPrincipal for the remote host.
     * @return Principal
     */
    public Principal peerPrincipal() {
        try {
            return sslEngine.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException se) {
            log.debug("SSL peer is not authenticated, returning ANONYMOUS instead");
            return KafkaPrincipal.ANONYMOUS;
        }
    }

    /**
     * returns an SSL Session after the handshake is established
     * throws IllegalStateException if the handshake is not established
     */
    public SSLSession sslSession() throws IllegalStateException {
        return sslEngine.getSession();
    }

    /**
     * Adds interestOps to SelectionKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    @Override
    public void addInterestOps(int ops) {
        if (!key.isValid())
            throw new CancelledKeyException();
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        key.interestOps(key.interestOps() | ops);
    }

    /**
     * removes interestOps to SelectionKey of the TransportLayer
     * @param ops SelectionKey interestOps
     */
    @Override
    public void removeInterestOps(int ops) {
        if (!key.isValid())
            throw new CancelledKeyException();
        else if (!ready())
            throw new IllegalStateException("handshake is not completed");

        key.interestOps(key.interestOps() & ~ops);
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

    protected int netReadBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    protected int netWriteBufferSize() {
        return sslEngine.getSession().getPacketBufferSize();
    }

    protected int applicationBufferSize() {
        return sslEngine.getSession().getApplicationBufferSize();
    }

    protected ByteBuffer netReadBuffer() {
        return netReadBuffer;
    }

    // Visibility for testing
    protected ByteBuffer appReadBuffer() {
        return appReadBuffer;
    }

    /**
     * SSL exceptions are propagated as authentication failures so that clients can avoid
     * retries and report the failure. If `flush` is true, exceptions are propagated after
     * any pending outgoing bytes are flushed to ensure that the peer is notified of the failure.
     */
    private void handshakeFailure(SSLException sslException, boolean flush) throws IOException {
        //Release all resources such as internal buffers that SSLEngine is managing
        log.debug("SSL Handshake failed", sslException);
        sslEngine.closeOutbound();
        try {
            sslEngine.closeInbound();
        } catch (SSLException e) {
            log.debug("SSLEngine.closeInBound() raised an exception.", e);
        }

        state = State.HANDSHAKE_FAILED;
        handshakeException = new SslAuthenticationException("SSL handshake failed", sslException);

        // Attempt to flush any outgoing bytes. If flush doesn't complete, delay exception handling until outgoing bytes
        // are flushed. If write fails because remote end has closed the channel, log the I/O exception and  continue to
        // handle the handshake failure as an authentication exception.
        if (!flush || handshakeWrapAfterFailure(flush))
            throw handshakeException;
        else
            log.debug("Delay propagation of handshake exception till {} bytes remaining are flushed", netWriteBuffer.remaining());
    }

    // SSL handshake failures are typically thrown as SSLHandshakeException, SSLProtocolException,
    // SSLPeerUnverifiedException or SSLKeyException if the cause is known. These exceptions indicate
    // authentication failures (e.g. configuration errors) which should not be retried. But the SSL engine
    // may also throw exceptions using the base class SSLException in a few cases:
    //   a) If there are no matching ciphers or TLS version or the private key is invalid, client will be
    //      unable to process the server message and an SSLException is thrown:
    //      javax.net.ssl.SSLException: Unrecognized SSL message, plaintext connection?
    //   b) If server closes the connection gracefully during handshake, client may receive close_notify
    //      and and an SSLException is thrown:
    //      javax.net.ssl.SSLException: Received close_notify during handshake
    // We want to handle a) as a non-retriable SslAuthenticationException and b) as a retriable IOException.
    // To do this we need to rely on the exception string. Since it is safer to throw a retriable exception
    // when we are not sure, we will treat only the first exception string as a handshake exception.
    private void maybeProcessHandshakeFailure(SSLException sslException, boolean flush, IOException ioException) throws IOException {
        if (sslException instanceof SSLHandshakeException || sslException instanceof SSLProtocolException ||
                sslException instanceof SSLPeerUnverifiedException || sslException instanceof SSLKeyException ||
                sslException.getMessage().contains("Unrecognized SSL message") ||
                sslException.getMessage().contains("Received fatal alert: "))
            handshakeFailure(sslException, flush);
        else if (ioException == null)
            throw sslException;
        else {
            log.debug("SSLException while unwrapping data after IOException, original IOException will be propagated", sslException);
            throw ioException;
        }
    }

    // If handshake has already failed, throw the authentication exception.
    private void maybeThrowSslAuthenticationException() {
        if (handshakeException != null)
            throw handshakeException;
    }

    /**
     * Perform handshake wrap after an SSLException or any IOException.
     *
     * If `doWrite=false`, we are processing IOException after peer has disconnected, so we
     * cannot send any more data. We perform any pending wraps so that we can unwrap any
     * peer data that is already available.
     *
     * If `doWrite=true`, we are processing SSLException, we perform wrap and flush
     * any data to notify the peer of the handshake failure.
     *
     * Returns true if no more wrap is required and any data is flushed or discarded.
     */
    private boolean handshakeWrapAfterFailure(boolean doWrite) {
        try {
            log.trace("handshakeWrapAfterFailure status {} doWrite {}", handshakeStatus, doWrite);
            while (handshakeStatus == HandshakeStatus.NEED_WRAP && (!doWrite || flush(netWriteBuffer))) {
                if (!doWrite)
                    clearWriteBuffer();
                handshakeWrap(doWrite);
            }
        } catch (Exception e) {
            log.debug("Failed to wrap and flush all bytes before closing channel", e);
            clearWriteBuffer();
        }
        if (!doWrite)
            clearWriteBuffer();
        return !netWriteBuffer.hasRemaining();
    }

    private void clearWriteBuffer() {
        if (netWriteBuffer.hasRemaining())
            log.debug("Discarding write buffer {} since peer has disconnected", netWriteBuffer);
        netWriteBuffer.position(0);
        netWriteBuffer.limit(0);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return hasBytesBuffered;
    }

    // Update `hasBytesBuffered` status. If any bytes were read from the network or
    // if data was returned from read, `hasBytesBuffered` is set to true if any buffered
    // data is still remaining. If not, `hasBytesBuffered` is set to false since no progress
    // can be made until more data is available to read from the network.
    private void updateBytesBuffered(boolean madeProgress) {
        if (madeProgress)
            hasBytesBuffered = netReadBuffer.position() != 0 || appReadBuffer.position() != 0;
        else
            hasBytesBuffered = false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        if (state == State.CLOSING)
            throw closingException();
        if (state != State.READY)
            return 0;

        if (!flush(netWriteBuffer))
            return 0;

        long channelSize = fileChannel.size();
        if (position > channelSize)
            return 0;
        int totalBytesToWrite = (int) Math.min(Math.min(count, channelSize - position), Integer.MAX_VALUE);

        if (fileChannelBuffer == null) {
            // Pick a size that allows for reasonably efficient disk reads, keeps the memory overhead per connection
            // manageable and can typically be drained in a single `write` call. The `netWriteBuffer` is typically 16k
            // and the socket send buffer is 100k by default, so 32k is a good number given the mentioned trade-offs.
            int transferSize = 32768;
            // Allocate a direct buffer to avoid one heap to heap buffer copy. SSLEngine copies the source
            // buffer (fileChannelBuffer) to the destination buffer (netWriteBuffer) and then encrypts in-place.
            // FileChannel.read() to a heap buffer requires a copy from a direct buffer to a heap buffer, which is not
            // useful here.
            fileChannelBuffer = ByteBuffer.allocateDirect(transferSize);
            // The loop below drains any remaining bytes from the buffer before reading from disk, so we ensure there
            // are no remaining bytes in the empty buffer
            fileChannelBuffer.position(fileChannelBuffer.limit());
        }

        int totalBytesWritten = 0;
        long pos = position;
        try {
            while (totalBytesWritten < totalBytesToWrite) {
                if (!fileChannelBuffer.hasRemaining()) {
                    fileChannelBuffer.clear();
                    int bytesRemaining = totalBytesToWrite - totalBytesWritten;
                    if (bytesRemaining < fileChannelBuffer.limit())
                        fileChannelBuffer.limit(bytesRemaining);
                    int bytesRead = fileChannel.read(fileChannelBuffer, pos);
                    if (bytesRead <= 0)
                        break;
                    fileChannelBuffer.flip();
                }
                int networkBytesWritten = write(fileChannelBuffer);
                totalBytesWritten += networkBytesWritten;
                // In the case of a partial write we only return the written bytes to the caller. As a result, the
                // `position` passed in the next `transferFrom` call won't include the bytes remaining in
                // `fileChannelBuffer`. By draining `fileChannelBuffer` first, we ensure we update `pos` before
                // we invoke `fileChannel.read`.
                if (fileChannelBuffer.hasRemaining())
                    break;
                pos += networkBytesWritten;
            }
            return totalBytesWritten;
        } catch (IOException e) {
            if (totalBytesWritten > 0)
                return totalBytesWritten;
            throw e;
        }
    }
}
