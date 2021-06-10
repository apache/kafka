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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * 这是一个基类，可以用于客户端（生产者/消费者），也可以用于broker端（服务端）。
 * <p>
 * 每个实例都包含以下内容：
 * <p>
 * ① 全局唯一ID，即 node id。在客户端和broker端有不同的含义。客户端表示所连接的节点ID。而服务端表示接收连接的节点ID。
 * <p>
 * ② 对传输层 {@link TransportLayer} 的引用，它的实现类持有 {@link SocketChannel} 的引用。负责对 {@link SocketChannel}的读/写操作。
 * <p>
 * ③ 一个 {@link Authenticator} 认证器。负责和对端的认证工作。这个类持有 {@link TransportLayer} 引用，可以直接从传输层读/写数据。
 * <p>
 * ④ 一个 {@link MemoryPool} 内存对象池（客户端通常使用堆内内存 JVM heap）。
 * <p>
 * ⑤ 一个 {@link NetworkReceive} 对象，代表当前正在「读取」的未完成/进行中的请求（从broker角度看）或响应（从客户端角度看）。比如对于客户端，
 *   这个对象就是接收broker发过来的二进制数据。
 * <p>
 * ⑥ 一个 {@link Send} 对象。代表当前正在「写入」的未完成/进行中的响应（从broker角度看）或请求（从客户端角度看）。
 * <p>
 * ⑦ 同一时刻，每个 {@link KafkaChannel} 只能发送或接收一个请求或响应，这些请求和响应的接收具有顺序性。
 *
 * 总结：KafkaChannel 本质是对 {@link SocketChannel} 的包装，但是这中间还隔了一层 {@link TransportLayer}，它才是和 {@link SocketChannel} 打交道的类。
 * KafkaChannel通过组合的方式达到解耦的目的。同时 KafkaChannel 有两个重要的引用类，分别是 {@link NetworkReceive} 和 {@link Send}，
 * 分别存放从 {@link SocketChannel} 接收到的数据和客户端待发送的数据。
 * KafkaChannel同时拥有通道的状态 {@link ChannelState}。
 */
public class KafkaChannel implements AutoCloseable {
    private static final long MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS = 1000 * 1000 * 1000;

    /**
     * 由于内存资源紧张或其他原因，可能会让 {@link KafkaChannel} 处于"静默"状态，
     * 也就是说不能发送数据，也不能接收数据。等待合适的条件后才能放开。Kafka关于静默状态细分了以下几类
     */
    public enum ChannelMuteState {
        /**
         * 通道默认状态，即不静默
         */
        NOT_MUTED,

        /**
         * 通道被静默
         */
        MUTED,

        /**
         * 仅限于 SocketServer。
         * 通道被默认且broker端仍未发送一个响应给客户端（acks!=0）
         * 或者目前正在等待从 API 层（acks==0）接收响应
         */
        MUTED_AND_RESPONSE_PENDING,

        /**
         * 仅限于 SocketServer。
         * 通道被静默且通道被限流
         */
        MUTED_AND_THROTTLED,

        /**
         * 仅限于 SocketServer。
         * 通道被静默且被限流而且有一个响应处于待发送状态
         */
        MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
    }


    /**
     * 定义触发通道静默的相关事件源
     * 事件转换顺序：
     * ① REQUEST_RECEIVED: MUTED => MUTED_AND_RESPONSE_PENDING
     * ② RESPONSE_SENT:    MUTED_AND_RESPONSE_PENDING => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED
     * ③ THROTTLE_STARTED: MUTED_AND_RESPONSE_PENDING => MUTED_AND_THROTTLED_AND_RESPONSE_PENDING
     * ④ THROTTLE_ENDED:   MUTED_AND_THROTTLED => MUTED, MUTED_AND_THROTTLED_AND_RESPONSE_PENDING => MUTED_AND_RESPONSE_PENDING
     */
    public enum ChannelMuteEvent {
        /**
         * 收到来自客户端的请求
         */
        REQUEST_RECEIVED,

        /**
         * 已经向客户端发送响应（ack!=0）
         * 或SocketServer收到API层的反馈（acks=0）
         */
        RESPONSE_SENT,

        /**
         * 由于超出分配的额度而被限流
         */
        THROTTLE_STARTED,

        /**
         * 限流结束
         */
        THROTTLE_ENDED
    }

    // 节点ID，客户端和服务端语义不同
    private final String id;

    // 传输层
    private final TransportLayer transportLayer;

    // 认证相关的类
    private final Supplier<Authenticator> authenticatorCreator;
    private Authenticator authenticator;

    // 追踪累加网络线程时间，这被Sender线程更新。
    // 该值在每次发送响应后被读取和重置
    private long networkThreadTimeNanos;

    // 接收数据最大值
    private final int maxReceiveSize;

    // 内存对象池
    private final MemoryPool memoryPool;

    // 通道元数据收集器
    private final ChannelMetadataRegistry metadataRegistry;

    // 对于客户端来说，receive表示接收从broker端发出的响应，send表示客户端发往broker端的二进制数据
    private NetworkReceive receive;
    private NetworkSend send;

    // 追踪通道的连接和静默状态，以便在通道断开后能够处理通道上的未处理的请求
    private boolean disconnected;

    // 当前通道的静默状态
    private ChannelMuteState muteState;

    // 通道状态
    private ChannelState state;

    // 对端地址
    private SocketAddress remoteAddress;
    private int successfulAuthentications;
    private boolean midWrite;

    // 最近一次开始认证时间戳
    private long lastReauthenticationStartNanos;

    public KafkaChannel(String id, TransportLayer transportLayer, Supplier<Authenticator> authenticatorCreator,
                        int maxReceiveSize, MemoryPool memoryPool, ChannelMetadataRegistry metadataRegistry) {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticatorCreator = authenticatorCreator;
        this.authenticator = authenticatorCreator.get();
        this.networkThreadTimeNanos = 0L;
        this.maxReceiveSize = maxReceiveSize;
        this.memoryPool = memoryPool;
        this.metadataRegistry = metadataRegistry;
        this.disconnected = false;
        this.muteState = ChannelMuteState.NOT_MUTED;
        this.state = ChannelState.NOT_CONNECTED;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator, receive, metadataRegistry);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public KafkaPrincipal principal() {
        return authenticator.principal();
    }

    public Optional<KafkaPrincipalSerde> principalSerde() {
        return authenticator.principalSerde();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator.
     * For SSL with client authentication enabled, {@link TransportLayer#handshake()} performs
     * authentication. For SASL, authentication is performed by {@link Authenticator#authenticate()}.
     */
    public void prepare() throws AuthenticationException, IOException {
        boolean authenticating = false;
        try {
            if (!transportLayer.ready())
                transportLayer.handshake();
            if (transportLayer.ready() && !authenticator.complete()) {
                authenticating = true;
                authenticator.authenticate();
            }
        } catch (AuthenticationException e) {
            // Clients are notified of authentication exceptions to enable operations to be terminated
            // without retries. Other errors are handled as network exceptions in Selector.
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            if (authenticating) {
                delayCloseOnAuthenticationFailure();
                throw new DelayedResponseAuthenticationException(e);
            }
            throw e;
        }
        if (ready()) {
            ++successfulAuthentications;
            state = ChannelState.READY;
        }
    }

    public void disconnect() {
        disconnected = true;
        if (state == ChannelState.NOT_CONNECTED && remoteAddress != null) {
            //if we captured the remote address we can provide more information
            state = new ChannelState(ChannelState.State.NOT_CONNECTED, remoteAddress.toString());
        }
        transportLayer.disconnect();
    }

    public void state(ChannelState state) {
        this.state = state;
    }

    public ChannelState state() {
        return this.state;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public boolean finishConnect() throws IOException {
        //we need to grab remoteAddr before finishConnect() is called otherwise
        //it becomes inaccessible if the connection was refused.

        // #1 获取JDK底层的SocketChannel
        SocketChannel socketChannel = transportLayer.socketChannel();
        if (socketChannel != null) {
            // #2 获取远端的地址
            remoteAddress = socketChannel.getRemoteAddress();
        }

        // #3 判断通道是否已经成功连接
        boolean connected = transportLayer.finishConnect();
        if (connected) {
            // #4 通道成功连接，设置「KafkaChannel」通道状态
            if (ready()) {
                state = ChannelState.READY;
            } else if (remoteAddress != null) {
                state = new ChannelState(ChannelState.State.AUTHENTICATE, remoteAddress.toString());
            } else {
                state = ChannelState.AUTHENTICATE;
            }
        }
        // #4 boolean值表示连接是否已成功建立
        return connected;
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public SelectionKey selectionKey() {
        return transportLayer.selectionKey();
    }

    /**
     * externally muting a channel should be done via selector to ensure proper state handling
     */
    void mute() {
        if (muteState == ChannelMuteState.NOT_MUTED) {
            if (!disconnected) transportLayer.removeInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.MUTED;
        }
    }

    /**
     * Unmute the channel. The channel can be unmuted only if it is in the MUTED state. For other muted states
     * (MUTED_AND_*), this is a no-op.
     *
     * @return Whether or not the channel is in the NOT_MUTED state after the call
     */
    boolean maybeUnmute() {
        if (muteState == ChannelMuteState.MUTED) {
            if (!disconnected) transportLayer.addInterestOps(SelectionKey.OP_READ);
            muteState = ChannelMuteState.NOT_MUTED;
        }
        return muteState == ChannelMuteState.NOT_MUTED;
    }

    // Handle the specified channel mute-related event and transition the mute state according to the state machine.

    /**
     * 通道静默事件状态机：根据 mute-related 事件
     *
     * @param event
     */
    public void handleChannelMuteEvent(ChannelMuteEvent event) {
        boolean stateChanged = false;
        switch (event) {
            case REQUEST_RECEIVED:
                // #1 收到来自客户端的请求
                if (muteState == ChannelMuteState.MUTED) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case RESPONSE_SENT:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED;
                    stateChanged = true;
                }
                break;
            case THROTTLE_STARTED:
                if (muteState == ChannelMuteState.MUTED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
                break;
            case THROTTLE_ENDED:
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED) {
                    muteState = ChannelMuteState.MUTED;
                    stateChanged = true;
                }
                if (muteState == ChannelMuteState.MUTED_AND_THROTTLED_AND_RESPONSE_PENDING) {
                    muteState = ChannelMuteState.MUTED_AND_RESPONSE_PENDING;
                    stateChanged = true;
                }
        }
        if (!stateChanged) {
            throw new IllegalStateException("Cannot transition from " + muteState.name() + " for " + event.name());
        }
    }

    public ChannelMuteState muteState() {
        return muteState;
    }

    /**
     * Delay channel close on authentication failure. This will remove all read/write operations from the channel until
     * {@link #completeCloseOnAuthenticationFailure()} is called to finish up the channel close.
     */
    private void delayCloseOnAuthenticationFailure() {
        transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * Finish up any processing on {@link #prepare()} failure.
     * @throws IOException
     */
    void completeCloseOnAuthenticationFailure() throws IOException {
        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        // Invoke the underlying handler to finish up any processing on authentication failure
        authenticator.handleAuthenticationFailure();
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMuted() {
        return muteState != ChannelMuteState.NOT_MUTED;
    }

    public boolean isInMutableState() {
        //some requests do not require memory, so if we do not know what the current (or future) request is
        //(receive == null) we dont mute. we also dont mute if whatever memory required has already been
        //successfully allocated (if none is required for the currently-being-read request
        //receive.memoryAllocated() is expected to return true)
        if (receive == null || receive.memoryAllocated())
            return false;
        //also cannot mute if underlying transport is not in the ready state
        return transportLayer.ready();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 将待发送的数据加入缓冲区，这一步是委托 {@link TransportLayer} 来完成。
     * 这里说一下，{@link TransportLayer} 是对底层{@link SocketChannel} 封装的类，
     * 至于为什么这么设计是因为在TCP协议之上还有SSL协议，所以就再封装了一层。通常我们是使用 {@link PlaintextTransportLayer} 传输层。
     *
     * @param send  待缓存的数据对象
     */
    public void setSend(NetworkSend send) {
        // #1 如果通道中的send不为null，会抛出状态异常。因为按正常逻辑讲，只有当send==null，才会将下一个send添加进来，如果不为null是不会被添加的。
        if (this.send != null) throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress, connection id is " + id);

        // #2 加入缓存
        this.send = send;

        // #3 注册OP_WRITE事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 判断数据是否完成发送。发送完成条件（针对 {@link ByteBufferSend} 实现类而言）： 剩余字节数据为0，没有暂留的数据
     *
     * @return 如果完成发送，则返回 {@link #send} 对象，未完成发送则返回null
     */
    public NetworkSend maybeCompleteSend() {
        if (send != null && send.completed()) {
            midWrite = false;
            // 移除OP_WRITE事件
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            NetworkSend result = send;
            send = null;
            return result;
        }
        return null;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public long read() throws IOException {
        // #1 创建响应接收类
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id, memoryPool);
        }
        // 从通道中读取数据并写入「receive」对象中的ByteBuffer里
        long bytesReceived = receive(this.receive);

        if (this.receive.requiredMemoryAmountKnown() && !this.receive.memoryAllocated() && isInMutableState()) {
            // pool must be out of memory, mute ourselves.
            mute();
        }
        return bytesReceived;
    }

    public NetworkReceive currentReceive() {
        return receive;
    }

    /**
     * 如果数据已读取完毕，则返回 {@link #receive} 对象，否则返回null
     * @return
     */
    public NetworkReceive maybeCompleteReceive() {
        if (receive != null && receive.complete()) {
            receive.payload().rewind();
            NetworkReceive result = receive;
            receive = null;
            return result;
        }
        return null;
    }

    public long write() throws IOException {
        if (send == null)
            return 0;

        midWrite = true;
        return send.writeTo(transportLayer);
    }

    /**
     * Accumulates network thread time for this channel.
     */
    public void addNetworkThreadTimeNanos(long nanos) {
        networkThreadTimeNanos += nanos;
    }

    /**
     * Returns accumulated network thread time for this channel and resets
     * the value to zero.
     */
    public long getAndResetNetworkThreadTimeNanos() {
        long current = networkThreadTimeNanos;
        networkThreadTimeNanos = 0;
        return current;
    }

    /**
     * 从 {@link SocketChannel} 通道中接收数据并写入 {@param receive} 对象
     * @param receive
     * @return
     * @throws IOException
     */
    private long receive(NetworkReceive receive) throws IOException {
        try {
            return receive.readFrom(transportLayer);
        } catch (SslAuthenticationException e) {
            // With TLSv1.3, post-handshake messages may throw SSLExceptions, which are
            // handled as authentication failures
            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
            throw e;
        }
    }

    /**
     * @return true if underlying transport has bytes remaining to be read from any underlying intermediate buffers.
     */
    public boolean hasBytesBuffered() {
        return transportLayer.hasBytesBuffered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaChannel that = (KafkaChannel) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return super.toString() + " id=" + id;
    }
    
    /**
     * Return the number of times this instance has successfully authenticated. This
     * value can only exceed 1 when re-authentication is enabled and it has
     * succeeded at least once.
     * 
     * @return the number of times this instance has successfully authenticated
     */
    public int successfulAuthentications() {
        return successfulAuthentications;
    }

    /**
     * If this is a server-side connection that has an expiration time and at least
     * 1 second has passed since the prior re-authentication (if any) started then
     * begin the process of re-authenticating the connection and return true,
     * otherwise return false
     * 
     * @param saslHandshakeNetworkReceive
     *            the mandatory {@link NetworkReceive} containing the
     *            {@code SaslHandshakeRequest} that has been received on the server
     *            and that initiates re-authentication.
     * @param nowNanosSupplier
     *            {@code Supplier} of the current time. The value must be in
     *            nanoseconds as per {@code System.nanoTime()} and is therefore only
     *            useful when compared to such a value -- it's absolute value is
     *            meaningless.
     * 
     * @return true if this is a server-side connection that has an expiration time
     *         and at least 1 second has passed since the prior re-authentication
     *         (if any) started to indicate that the re-authentication process has
     *         begun, otherwise false
     * @throws AuthenticationException
     *             if re-authentication fails due to invalid credentials or other
     *             security configuration errors
     * @throws IOException
     *             if read/write fails due to an I/O error
     * @throws IllegalStateException
     *             if this channel is not "ready"
     */
    public boolean maybeBeginServerReauthentication(NetworkReceive saslHandshakeNetworkReceive,
            Supplier<Long> nowNanosSupplier) throws AuthenticationException, IOException {
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should be \"ready\" when processing SASL Handshake for potential re-authentication");
        /*
         * Re-authentication is disabled if there is no session expiration time, in
         * which case the SASL handshake network receive will be processed normally,
         * which results in a failure result being sent to the client. Also, no need to
         * check if we are muted since since we are processing a received packet when we
         * invoke this.
         */
        if (authenticator.serverSessionExpirationTimeNanos() == null)
            return false;
        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        long nowNanos = nowNanosSupplier.get();
        /*
         * Cannot re-authenticate more than once every second; an attempt to do so will
         * result in the SASL handshake network receive being processed normally, which
         * results in a failure result being sent to the client.
         */
        if (lastReauthenticationStartNanos != 0
                && nowNanos - lastReauthenticationStartNanos < MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS)
            return false;
        lastReauthenticationStartNanos = nowNanos;
        swapAuthenticatorsAndBeginReauthentication(
                new ReauthenticationContext(authenticator, saslHandshakeNetworkReceive, nowNanos));
        return true;
    }

    /**
     * If this is a client-side connection that is not muted, there is no
     * in-progress write, and there is a session expiration time defined that has
     * past then begin the process of re-authenticating the connection and return
     * true, otherwise return false
     * 
     * @param nowNanosSupplier
     *            {@code Supplier} of the current time. The value must be in
     *            nanoseconds as per {@code System.nanoTime()} and is therefore only
     *            useful when compared to such a value -- it's absolute value is
     *            meaningless.
     * 
     * @return true if this is a client-side connection that is not muted, there is
     *         no in-progress write, and there is a session expiration time defined
     *         that has past to indicate that the re-authentication process has
     *         begun, otherwise false
     * @throws AuthenticationException
     *             if re-authentication fails due to invalid credentials or other
     *             security configuration errors
     * @throws IOException
     *             if read/write fails due to an I/O error
     * @throws IllegalStateException
     *             if this channel is not "ready"
     */
    public boolean maybeBeginClientReauthentication(Supplier<Long> nowNanosSupplier)
            throws AuthenticationException, IOException {
        if (!ready())
            throw new IllegalStateException(
                    "KafkaChannel should always be \"ready\" when it is checked for possible re-authentication");
        if (muteState != ChannelMuteState.NOT_MUTED || midWrite
                || authenticator.clientSessionReauthenticationTimeNanos() == null)
            return false;
        /*
         * We've delayed getting the time as long as possible in case we don't need it,
         * but at this point we need it -- so get it now.
         */
        long nowNanos = nowNanosSupplier.get();
        if (nowNanos < authenticator.clientSessionReauthenticationTimeNanos())
            return false;
        swapAuthenticatorsAndBeginReauthentication(new ReauthenticationContext(authenticator, receive, nowNanos));
        receive = null;
        return true;
    }
    
    /**
     * Return the number of milliseconds that elapsed while re-authenticating this
     * session from the perspective of this instance, if applicable, otherwise null.
     * The server-side perspective will yield a lower value than the client-side
     * perspective of the same re-authentication because the client-side observes an
     * additional network round-trip.
     * 
     * @return the number of milliseconds that elapsed while re-authenticating this
     *         session from the perspective of this instance, if applicable,
     *         otherwise null
     */
    public Long reauthenticationLatencyMs() {
        return authenticator.reauthenticationLatencyMs();
    }

    /**
     * Return true if this is a server-side channel and the given time is past the
     * session expiration time, if any, otherwise false
     * 
     * @param nowNanos
     *            the current time in nanoseconds as per {@code System.nanoTime()}
     * @return true if this is a server-side channel and the given time is past the
     *         session expiration time, if any, otherwise false
     */
    public boolean serverAuthenticationSessionExpired(long nowNanos) {
        Long serverSessionExpirationTimeNanos = authenticator.serverSessionExpirationTimeNanos();
        return serverSessionExpirationTimeNanos != null && nowNanos - serverSessionExpirationTimeNanos > 0;
    }
    
    /**
     * Return the (always non-null but possibly empty) client-side
     * {@link NetworkReceive} response that arrived during re-authentication but
     * is unrelated to re-authentication. This corresponds to a request sent
     * prior to the beginning of re-authentication; the request was made when the
     * channel was successfully authenticated, and the response arrived during the
     * re-authentication process.
     * 
     * @return client-side {@link NetworkReceive} response that arrived during
     *         re-authentication that is unrelated to re-authentication. This may
     *         be empty.
     */
    public Optional<NetworkReceive> pollResponseReceivedDuringReauthentication() {
        return authenticator.pollResponseReceivedDuringReauthentication();
    }
    
    /**
     * Return true if this is a server-side channel and the connected client has
     * indicated that it supports re-authentication, otherwise false
     * 
     * @return true if this is a server-side channel and the connected client has
     *         indicated that it supports re-authentication, otherwise false
     */
    boolean connectedClientSupportsReauthentication() {
        return authenticator.connectedClientSupportsReauthentication();
    }

    private void swapAuthenticatorsAndBeginReauthentication(ReauthenticationContext reauthenticationContext)
            throws IOException {
        // it is up to the new authenticator to close the old one
        // replace with a new one and begin the process of re-authenticating
        authenticator = authenticatorCreator.get();
        authenticator.reauthenticate(reauthenticationContext);
    }

    public ChannelMetadataRegistry channelMetadataRegistry() {
        return metadataRegistry;
    }
}
