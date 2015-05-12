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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLPeerUnverifiedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Transport layer for SSL communication
 */

public class SSLTransportLayer implements TransportLayer {
    private static final Logger log = LoggerFactory.getLogger(SSLTransportLayer.class);
    protected SSLEngine sslEngine;

    private SocketChannel socketChannel;
    private HandshakeStatus handshakeStatus;
    private SSLEngineResult handshakeResult;
    private boolean handshakeComplete = false;
    private boolean closed = false;
    private boolean closing = false;
    private ByteBuffer netInBuffer;
    private ByteBuffer netOutBuffer;
    private ByteBuffer appReadBuffer;
    private ByteBuffer appWriteBuffer;
    private ByteBuffer emptyBuf = ByteBuffer.allocate(0);
    private DataInputStream inStream;
    private DataOutputStream outStream;
    private ExecutorService executorService;

    public SSLTransportLayer(SocketChannel socketChannel, SSLEngine sslEngine, ExecutorService executorService) throws IOException {
        this.socketChannel = socketChannel;
        this.sslEngine = sslEngine;
        this.executorService = executorService;
        this.netInBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
        this.netOutBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize());
        this.appWriteBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getApplicationBufferSize());
        this.appReadBuffer = ByteBuffer.allocateDirect(sslEngine.getSession().getApplicationBufferSize());
        startHandshake();
    }

    public void startHandshake() throws IOException {
        netOutBuffer.position(0);
        netOutBuffer.limit(0);
        netInBuffer.position(0);
        netInBuffer.limit(0);
        handshakeComplete = false;
        closed = false;
        closing = false;
        //initiate handshake
        sslEngine.beginHandshake();
        handshakeStatus = sslEngine.getHandshakeStatus();
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    public boolean finishConnect() throws IOException {
        return socketChannel.finishConnect();
    }

    /**
    * Flushes the buffer to the network, non blocking
    * @param buf ByteBuffer
    * @return boolean true if the buffer has been emptied out, false otherwise
    * @throws IOException
    */
    public boolean flush(ByteBuffer buf) throws IOException {
        int remaining = buf.remaining();
        if (remaining > 0) {
            int written = socketChannel.write(buf);
            return written >= remaining;
        }
        return true;
    }

    /**
    * Performs SSL handshake, non blocking.
    * The return for this operation is 0 if the handshake is complete and a positive value if it is not complete.
    * In the event of a positive value coming back, re-register the selection key for the return values interestOps.
    * @param read boolean - true if the underlying channel is readable
    * @param write boolean - true if the underlying channel is writable
    * @return int - 0 if hand shake is complete, otherwise it returns a SelectionKey interestOps value
    * @throws IOException
    */
    public int handshake(boolean read, boolean write) throws IOException {
        if (handshakeComplete) return 0; //we have done our initial handshake

        if (!flush(netOutBuffer)) return SelectionKey.OP_WRITE;
        try {
            switch(handshakeStatus) {
                case NOT_HANDSHAKING:
                    // SSLEnginge.getHandshakeStatus is transient and it doesn't record FINISHED status properly
                    if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
                        handshakeComplete = !netOutBuffer.hasRemaining();
                        if (handshakeComplete)
                            return 0;
                        else
                            return SelectionKey.OP_WRITE;
                    } else {
                        throw new IOException("NOT_HANDSHAKING during handshake");
                    }
                case FINISHED:
                    //we are complete if we have delivered the last package
                    handshakeComplete = !netOutBuffer.hasRemaining();
                    //return 0 if we are complete, otherwise we still have data to write
                    if (handshakeComplete) return 0;
                    else return SelectionKey.OP_WRITE;
                case NEED_WRAP:
                    handshakeResult = handshakeWrap(write);
                    if (handshakeResult.getStatus() == Status.OK) {
                        if (handshakeStatus == HandshakeStatus.NEED_TASK)
                            handshakeStatus = tasks();
                    } else {
                        //wrap should always work with our buffers
                        throw new IOException("Unexpected status [" + handshakeResult.getStatus() + "] during handshake WRAP.");
                    }
                    if (handshakeStatus != HandshakeStatus.NEED_UNWRAP || (!flush(netOutBuffer)))
                        return SelectionKey.OP_WRITE;
                    //fall down to NEED_UNWRAP on the same call, will result in a
                    //BUFFER_UNDERFLOW if it needs data
                case NEED_UNWRAP:
                    handshakeResult = handshakeUnwrap(read);
                    if (handshakeResult.getStatus() == Status.OK) {
                        if (handshakeStatus == HandshakeStatus.NEED_TASK)
                            handshakeStatus = tasks();
                    } else if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
                        return SelectionKey.OP_READ;
                    } else {
                        throw new IOException(String.format("Unexpected status [%s] during handshake UNWRAP", handshakeStatus));
                    }
                    break;
                case NEED_TASK:
                    handshakeStatus = tasks();
                    break;
                default:
                    throw new IllegalStateException(String.format("Unexpected status [%s]", handshakeStatus));
            }
        } catch (SSLException e) {
            handshakeFailure();
            throw e;
        }

        //return 0 if we are complete, otherwise re-register for any activity that
        //would cause this method to be called again.
        if (handshakeComplete) return 0;
        else return SelectionKey.OP_WRITE | SelectionKey.OP_READ;
    }

    /**
     * Executes all the tasks needed on the executorservice thread.
     * @return HandshakeStatus
     */
    private HandshakeStatus tasks() {
        for (;;) {
            final Runnable task = sslEngine.getDelegatedTask();
            if (task == null)
                break;

            executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        task.run();
                    }
                });
        }
        return sslEngine.getHandshakeStatus();
    }

    /**
    * Performs the WRAP function
    * @param doWrite boolean
    * @return SSLEngineResult
    * @throws IOException
    */
    private SSLEngineResult  handshakeWrap(Boolean doWrite) throws IOException {
        //this should never be called with a network buffer that contains data
        //so we can clear it here.
        netOutBuffer.clear();
        SSLEngineResult result = sslEngine.wrap(appWriteBuffer, netOutBuffer);
        //prepare the results to be written
        netOutBuffer.flip();
        handshakeStatus = result.getHandshakeStatus();
        //optimization, if we do have a writable channel, write it now
        if (doWrite) flush(netOutBuffer);
        return result;
    }

    /**
    * Perform handshake unwrap
    * @param doRead boolean
    * @return SSLEngineResult
    * @throws IOException
    */
    private SSLEngineResult handshakeUnwrap(Boolean doRead) throws IOException {
        if (netInBuffer.position() == netInBuffer.limit()) {
            //clear the buffer if we have emptied it out on data
            netInBuffer.clear();
        }

        if (doRead)  {
            int read = socketChannel.read(netInBuffer);
            if (read == -1) throw new IOException("EOF during handshake.");
        }

        SSLEngineResult result;
        boolean cont = false;
        do {
            //prepare the buffer with the incoming data
            netInBuffer.flip();
            result = sslEngine.unwrap(netInBuffer, appWriteBuffer);
            netInBuffer.compact();
            handshakeStatus = result.getHandshakeStatus();
            if (result.getStatus() == SSLEngineResult.Status.OK &&
                 result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
                handshakeStatus = tasks();
            }
            cont = result.getStatus() == SSLEngineResult.Status.OK &&
                handshakeStatus == HandshakeStatus.NEED_UNWRAP;
        } while(cont);
        return result;
    }


    /**
    * Sends a SSL close message, will not physically close the connection here.<br>
    * @throws IOException if an I/O error occurs
    * @throws IOException if there is data on the outgoing network buffer and we are unable to flush it
    */
    public void close() throws IOException {
        if (closing) return;
        closing = true;
        sslEngine.closeOutbound();

        if (!flush(netOutBuffer)) {
            throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
        }
        //prep the buffer for the close message
        netOutBuffer.clear();
        //perform the close, since we called sslEngine.closeOutbound
        SSLEngineResult handshake = sslEngine.wrap(emptyBuf, netOutBuffer);
        //we should be in a close state
        if (handshake.getStatus() != SSLEngineResult.Status.CLOSED) {
            throw new IOException("Invalid close state, will not send network data.");
        }
        netOutBuffer.flip();
        flush(netOutBuffer);
        socketChannel.socket().close();
        socketChannel.close();
        closed = !netOutBuffer.hasRemaining() && (handshake.getHandshakeStatus() != HandshakeStatus.NEED_WRAP);
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    public boolean isReady() {
        return handshakeComplete;
    }

    /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    * @throws IllegalStateException if handshake is not complete.
    */
    public int read(ByteBuffer dst) throws IOException {
        if (closing || closed) return -1;
        if (!handshakeComplete) throw new IllegalStateException("Handshake incomplete.");
        netInBuffer = Utils.ensureCapacity(netInBuffer, packetBufferSize());
        int netread = socketChannel.read(netInBuffer);
        if (netread == -1) return -1;
        int read = 0;
        SSLEngineResult unwrap = null;

        do {
            netInBuffer.flip();
            unwrap = sslEngine.unwrap(netInBuffer, appReadBuffer);
            //compact the buffer
            netInBuffer.compact();
            if (unwrap.getStatus() == Status.OK || unwrap.getStatus() == Status.BUFFER_UNDERFLOW) {
                read += unwrap.bytesProduced();
                // perform any task if needed
                if (unwrap.getHandshakeStatus() == HandshakeStatus.NEED_TASK) tasks();
                //if we need more network data, than return for now.
                if (unwrap.getStatus() == Status.BUFFER_UNDERFLOW) return readFromAppBuffer(dst);
            } else if (unwrap.getStatus() == Status.BUFFER_OVERFLOW) {
                appReadBuffer = Utils.ensureCapacity(appReadBuffer, applicationBufferSize());
                //empty out the dst buffer before we do another read
                return readFromAppBuffer(dst);
            }
        } while(netInBuffer.position() != 0);
        return readFromAppBuffer(dst);
    }

    public long read(ByteBuffer[] dsts) throws IOException {
        return read(dsts, 0, dsts.length);
    }

    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        int totalRead = 0;
        for (int i = offset; i < length; i++) {
            int read = read(dsts[i]);
            if (read > 0) {
                totalRead += read;
            }
        }
        return totalRead;
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @return The number of bytes written, possibly zero
    * @throws IOException If some other I/O error occurs
    */

    public int write(ByteBuffer src) throws IOException {
        int written = 0;
        if (closing || closed) throw new IOException("Channel is in closing state");
        if (!flush(netOutBuffer))
            return written;
        netOutBuffer.clear();
        SSLEngineResult result = sslEngine.wrap(src, netOutBuffer);
        written = result.bytesConsumed();
        netOutBuffer.flip();
        if (result.getStatus() == Status.OK) {
            if (result.getHandshakeStatus() == HandshakeStatus.NEED_TASK)
                tasks();
        } else {
            throw new IOException(String.format("Unable to wrap data, invalid status %s", result.getStatus()));
        }
        flush(netOutBuffer);
        return written;
    }

    public long write(ByteBuffer[] srcs, int offset, int length)  throws IOException {
        int totalWritten = 0;
        for (int i = offset; i < length; i++) {
            if (srcs[i].hasRemaining()) {
                int written = write(srcs[i]);
                if (written > 0) {
                    totalWritten += written;
                }
            }
        }
        return totalWritten;
    }

    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    public DataInputStream inStream() throws IOException {
        if (inStream == null)
            this.inStream = new DataInputStream(socketChannel.socket().getInputStream());
        return inStream;
    }

    public DataOutputStream outStream() throws IOException {
        if (outStream == null)
            this.outStream = new DataOutputStream(socketChannel.socket().getOutputStream());
        return outStream;
    }

    public Principal peerPrincipal() throws IOException {
        try {
            return sslEngine.getSession().getPeerPrincipal();
        } catch (SSLPeerUnverifiedException se) {
            throw new IOException(String.format("Unable to retrieve getPeerPrincipal due to %s", se));
        }
    }

    public SSLSession sslSession() throws IllegalStateException, UnsupportedOperationException {
        if (!handshakeComplete)
            throw new IllegalStateException("Handshake incomplete.");
        return sslEngine.getSession();
    }

    private int readFromAppBuffer(ByteBuffer dst) {
        appReadBuffer.flip();
        try {
            int remaining = appReadBuffer.remaining();
            if (remaining > 0) {
                if (remaining > dst.remaining())
                    remaining = dst.remaining();
                int i = 0;
                while (i < remaining) {
                    dst.put(appReadBuffer.get());
                    i++;
                }
            }
            return remaining;
        } finally {
            appReadBuffer.compact();
        }
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
