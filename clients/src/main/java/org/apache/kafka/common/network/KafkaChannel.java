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

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

/**
 * Kafka通道
 */
public class KafkaChannel {
    //id
    private final String id;
    //传输层
    private final TransportLayer transportLayer;
    //权限器
    private final Authenticator authenticator;
    //最大接受数量
    private final int maxReceiveSize;
    //RCV_BUFFER
    private NetworkReceive receive;
    //SEND请求
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        //传输层和权限器
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        //得到一个权限信息
        return authenticator.principal();
    }

    /**
     *
     * 准备阶段
     * 使用配置认证传送层的不握手和认证
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        //如果传输层为权限认证和我是
        if (!transportLayer.ready())

            transportLayer.handshake();
        //如果就绪连但是权限为完成认证，则认证
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
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

    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 读取数据防止RVC_BUF中
     * @return
     * @throws IOException
     */
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        //如果revice为空，则生成，传入最大接受大小和通道id
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }
        //接受RVC_BUF
        receive(receive);
        //如果接受完成，就将buffer倒置
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        //返回
        return result;
    }

    /**
     * 写入数据
     * @return
     * @throws IOException
     */
    public Send write() throws IOException {
        Send result = null;
        //如果发送完毕
        if (send != null && send(send)) {
            result = send;
            //help gc
            send = null;
        }
        //返回send结果
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        //读取数据
        return receive.readFrom(transportLayer);
    }

    /**
     * 是否可以发送
     * @param send
     * @return
     * @throws IOException
     */
    private boolean send(Send send) throws IOException {
        //send数据写入传输层
        send.writeTo(transportLayer);
        //如果发送完毕
        if (send.completed())
            //移除OP_WRITE操作
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        //返回是否发送完毕
        return send.completed();
    }

}
