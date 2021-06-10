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
package org.apache.kafka.clients;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.requests.AbstractRequest;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 定义数据发送、数据接收、相关判断等接口。主要实现是 {@link NetworkClient}
 * ① 判断节点状态（是否已经准备就绪）
 * ② 判断连接等待时间。根据规则计算连接等待时间。
 * ③ 判断节点是否已经断开连接了。
 * ④ 将 {@link ClientRequest} 对象放入缓存
 * ⑤ 真正执行I/O操作，将缓存中的数据发往节点或读取节点返回的数据，并放入相应的对象中。
 *
 * 这个是一个高级的API接口，定义了数据发送、触发I/O操作、判断等一系列方法。
 */
public interface KafkaClient extends Closeable {

    /**
     * 判断节点node是否已经准备好数据接收
     * 数据接收不仅仅是指TCP或SSL完成相关握手，
     * 这里还包含节点相关元数据是否缺失、连接没有被限流、连接是否可以允许发送请求等
     *
     * @param node  待判断节点详情
     * @param now   时间戳
     * @return
     */
    boolean isReady(Node node, long now);

    /**
     * 判断给定的节点是否成功连接。只有当调用{@link #poll} 方法时节点才会进入就绪状态。
     *
     * @param node 待连接的节点详情
     * @param now  当前的时间戳
     * @return     true：节点已就绪，可以发送TCP请求。否则为false
     */

    /**
     * 判断+尝试完成连接的建立。
     * {@link #isReady(Node, long)} 只能判断，不会触发相关操作使得连接变成已准备数据发送状态。
     * 而 {@link #ready(Node, long)} 判断还未准备好，会尝试初始化通道。
     *
     * @param node  待判断节点详情
     * @param now   时间戳
     * @return
     */
    boolean ready(Node node, long now);

    /**
     * 根据连接状态，在尝试发送数据之前，返回要等待的毫秒数。
     * 当连接断开时，需要遵从重连退避时间。
     * 当处于正在连接或已连接状态时，这将处理慢/停顿连接。
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    long connectionDelay(Node node, long now);

    /**
     * 根据连接状态和限流时间，返回在尝试发送数据之前所需等待的毫秒数。
     * ① 如果 TCP 已经建立。通道没有被限流，返回0。通道被限流，返回限流等待时长。
     * ② 如果仍未建立 TCP 连接，则返回 {@link Long#MAX_VALUE}。在完成 TCP 建立后，会唤醒发送线程。
     *
     * @param node 待检查的连接节点
     * @param now  当前时间戳
     */
    long pollDelayMs(Node node, long now);

    /**
     * 根据连接状态，检查节点的连接是否已经失败。
     * 这种连接失败通常是短暂的（transient），
     * 可以在下一个 {@link #ready(org.apache.kafka.common.Node, long)} } 调用中恢复，
     * 但在有些情况下，需要捕捉和重新处理暂时性的失败。
     *
     * @param node 待检查的连接节点
     * @return
     */
    boolean connectionFailed(Node node);

    /**
     * 根据连接状态，检查对该节点的认证是否失败。
     * 认证失败会被传播，没有任何重试。
     *
     * @param node 待检查的连接节点
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    AuthenticationException authenticationException(Node node);

    /**
     * 这个方法很重要，是客户端发送消息的入口。
     * 我们之前说过，Sender线程会按规则从 {@link org.apache.kafka.clients.producer.internals.RecordAccumulator}
     * 抽取数据，并按<node id, List<ProducerBatch>>形式组装数据，
     * 然后由 {@link org.apache.kafka.clients.producer.internals.Sender#sendProduceRequest(long, int, short, int, List)} 方法创建 {@link ClientRequest}，
     * 最后调用这个API向SocketChannel写入数据（当然，这个写入不是由 {@link KafkaClient} 实现类完成，而是 {@link org.apache.kafka.common.network.TransportLayer} 完成） 。
     *
     * 这个方法主要目的是将待发送的 {@param request} 封装为 {@link org.apache.kafka.common.network.Send} 对象，
     * 然后再把该对象写入 {@link org.apache.kafka.common.network.KafkaChannel#send} 中。
     *
     * @param request 待发送请求对象
     * @param now     当前时间戳
     */
    void send(ClientRequest request, long now);

    /**
     * {@link #send(ClientRequest, long)} 方法将数据已经放入指定位置（{@link org.apache.kafka.common.network.KafkaChannel#send}），
     * 这个方法本质就是调用JDK底层的 {@link java.nio.channels.SocketChannel#write(ByteBuffer)}将 {@link KafkaChannel#send}的数据写入通道中。
     * 当然，这个方法并非只做这么一件事情，还要接收从节点返回过来的Response消息，在收到响应后做收尾工作，比如触发相关回调函数。
     *
     * @param timeout   超时时间
     * @param now       当前时间戳
     * @return
     */
    List<ClientResponse> poll(long timeout, long now);

    /**
     * 和指定的节点ID断开连接
     * 如果连接还有未发送的请求（ClientRequests），这些请求都会收到断开连接的消息
     *
     * @param nodeId    节点ID
     */
    void disconnect(String nodeId);

    /**
     * 和指定的节点ID断开连接并关闭。
     * 所有的未发送请求被直接清除，也不会回调 ClientRequest 的回调方法。
     *
     * @param nodeId
     */
    void close(String nodeId);

    /**
     * 选择网络最好的节点
     * 怎么评判网络最好呢?可以通过对每个KafkaChannel的未发送的请求计算，数值越小，说明网络越通畅。
     *
     * @param now 当前时间戳
     * @return
     */
    Node leastLoadedNode(long now);

    /**
     * in-flight：在飞行中，表示将要发送或已经发送但未收到ACK的请求（ClientRequest）
     * 这个方法就是获取全局（整个生产者）的「in-flight」的数量，包含所有连接到不同节点的通道中的「in-flight」数量。
     * 每个通道中也有「in-flight」概念，不过这个数量仅限于单个通道而言，而非全局。
     *
     * @return
     */
    int inFlightRequestCount();

    /**
     * 如果至少有一个处于「in-Flight」的 CLientRequest，则返回true
     *
     * @return
     */
    boolean hasInFlightRequests();

    /**
     * 获取指定节点的「in-flight」数量
     *
     * @param nodeId 待获取的节点ID
     * @return
     */
    int inFlightRequestCount(String nodeId);

    /**
     * 如果某个节点至少有一个处于「in-flight」的 ClientRequest，则返回true
     * @param nodeId
     * @return
     */
    boolean hasInFlightRequests(String nodeId);

    /**
     * 至少有一个节点处于「READY」状态且未被限流，则返回true
     *
     * @param now   当前时间戳
     * @return
     */
    boolean hasReadyNodes(long now);

    /**
     * 如果Sender线程被阻塞在I/O上，则唤醒该线程
     */
    void wakeup();

    /**
     * 创建一个 {@link ClientRequest} 对象，这个对象是由若干个待发送到节点nodeId的批次构成。
     *
     * @param nodeId            目标节点ID
     * @param requestBuilder    构造器，用来创建{@link ClientRequest} 对象
     * @param createdTimeMs     创建时间
     * @param expectResponse    是否希望收到节点的响应。具体根据请求类型做出判断
     * @return
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs, boolean expectResponse);

    /**
     * 创建一个 {@link ClientRequest} 对象，这个对象是由若干个待发送到节点nodeId的批次构成。
     * 这里多了一个请求超时时间和回调函数
     *
     * @param nodeId            待发送的节点ID
     * @param requestBuilder    构造器，用来创建{@link ClientRequest} 对象
     * @param createdTimeMs     创建时间
     * @param expectResponse    是否希望收到节点的响应。具体根据请求类型做出判断。
     * @param requestTimeoutMs  响应超时时间
     * @param callback          回调函数，当收到请求后会回调此方法
     * @return
     */
    ClientRequest newClientRequest(String nodeId, AbstractRequest.Builder<?> requestBuilder, long createdTimeMs,
                                   boolean expectResponse, int requestTimeoutMs, RequestCompletionHandler callback);

    /**
     * 启动该客户机的关闭。
     * 这个方法可以在这个客户端被轮询的时候从另一个线程调用。
     * 不得再使用该客户端发送任何请求。
     * 当前的poll()将使用wakeup()终止。
     * 在轮询返回后，应使用close()显示关闭客户端。
     * 请注意，在轮询时不应该同时调用close()。
     */
    void initiateClose();

    /**
     * 判断当前客户端是否处于活跃状态，如果 {@link #initiateClose()} 或 {@link #close()} 则会返回 false。
     *
     * @return
     */
    boolean active();

}