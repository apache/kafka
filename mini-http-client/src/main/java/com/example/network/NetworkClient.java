package com.example.network;

import com.example.future.RequestFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 🔥 核心类：网络客户端（Layer 1）
 *
 * 学习点：
 * 1. 使用 Java NIO（Selector + SocketChannel）实现非阻塞网络 I/O
 * 2. 单线程处理所有网络事件
 * 3. 使用队列解耦请求发送和响应处理
 *
 * 类似 Kafka 的 ConsumerNetworkClient
 */
public class NetworkClient {

    // NIO Selector
    private final Selector selector;

    // 待发送的请求队列
    private final Queue<PendingRequest> unsent = new ConcurrentLinkedQueue<>();

    // 正在进行的请求（key: host:port, value: PendingRequest）
    private final Map<String, PendingRequest> inFlight = new ConcurrentHashMap<>();

    // 已完成但未触发回调的请求队列
    private final Queue<CompletedRequest> pendingCompletion = new ConcurrentLinkedQueue<>();

    public NetworkClient() throws IOException {
        this.selector = Selector.open();
    }

    /**
     * 发送请求（异步，非阻塞）
     *
     * 学习点：只是将请求加入队列，真正发送在 poll() 中
     */
    public RequestFuture<Response> send(Request request) {
        // 创建 Future 和完成处理器
        RequestFutureCompletionHandler handler = new RequestFutureCompletionHandler();
        PendingRequest pending = new PendingRequest(request, handler);

        // 加入待发送队列
        unsent.add(pending);

        System.out.println("[NetworkClient] Request queued: " + request);

        // 返回 Future
        return handler.future;
    }

    /**
     * 🔥 核心方法：poll - 执行网络 I/O
     *
     * 学习点：
     * 1. 先处理已完成的请求（触发回调）
     * 2. 发送待发送的请求
     * 3. 使用 Selector 检查网络事件
     * 4. 读取响应
     * 5. 再次处理已完成的请求
     */
    public void poll(long timeoutMs) throws IOException {
        System.out.println("[NetworkClient] Poll started (timeout=" + timeoutMs + "ms)");

        // 阶段 1: 触发已完成的回调
        firePendingCompletions();

        // 阶段 2: 发送待发送的请求
        trySend();

        // 阶段 3: 等待网络事件
        int readyCount = selector.select(timeoutMs);
        System.out.println("[NetworkClient] Selector returned " + readyCount + " ready channels");

        // 阶段 4: 处理可读事件
        if (readyCount > 0) {
            handleReadableChannels();
        }

        // 阶段 5: 再次触发已完成的回调
        firePendingCompletions();

        System.out.println("[NetworkClient] Poll completed");
    }

    /**
     * 发送待发送的请求
     */
    private void trySend() throws IOException {
        while (!unsent.isEmpty()) {
            PendingRequest pending = unsent.poll();
            if (pending == null) break;

            String key = pending.request.getHost() + ":" + pending.request.getPort();

            // 打开连接
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(pending.request.getHost(), pending.request.getPort()));

            // 注册 OP_CONNECT 和 OP_READ 事件
            SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
            selectionKey.attach(pending);

            // 加入 inFlight
            inFlight.put(key, pending);

            System.out.println("[NetworkClient] Connecting to " + key);
        }
    }

    /**
     * 处理可读的通道
     */
    private void handleReadableChannels() throws IOException {
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectedKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();

            PendingRequest pending = (PendingRequest) key.attachment();
            SocketChannel channel = (SocketChannel) key.channel();

            try {
                // 处理连接完成
                if (key.isConnectable()) {
                    if (channel.finishConnect()) {
                        System.out.println("[NetworkClient] Connected, sending request: " + pending.request);
                        // 发送 HTTP 请求
                        ByteBuffer buffer = ByteBuffer.wrap(pending.request.toHttpString().getBytes());
                        channel.write(buffer);
                    }
                }

                // 处理可读
                if (key.isReadable()) {
                    System.out.println("[NetworkClient] Reading response for: " + pending.request);
                    ByteBuffer buffer = ByteBuffer.allocate(4096);
                    int bytesRead = channel.read(buffer);

                    if (bytesRead > 0) {
                        buffer.flip();
                        byte[] data = new byte[buffer.remaining()];
                        buffer.get(data);
                        String responseText = new String(data);

                        // 创建响应
                        Response response = new Response(pending.request, responseText, false);

                        // 加入完成队列
                        pendingCompletion.add(new CompletedRequest(pending.handler, response, null));

                        // 清理
                        String mapKey = pending.request.getHost() + ":" + pending.request.getPort();
                        inFlight.remove(mapKey);
                        key.cancel();
                        channel.close();

                        System.out.println("[NetworkClient] Response received: " + response);
                    } else if (bytesRead < 0) {
                        // 连接关闭
                        Response response = new Response(pending.request, "", true);
                        pendingCompletion.add(new CompletedRequest(pending.handler, response, null));

                        String mapKey = pending.request.getHost() + ":" + pending.request.getPort();
                        inFlight.remove(mapKey);
                        key.cancel();
                        channel.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("[NetworkClient] Error handling channel: " + e.getMessage());
                pendingCompletion.add(new CompletedRequest(pending.handler, null, new RuntimeException(e)));

                String mapKey = pending.request.getHost() + ":" + pending.request.getPort();
                inFlight.remove(mapKey);
                key.cancel();
                channel.close();
            }
        }
    }

    /**
     * 🔥 触发已完成的回调
     *
     * 学习点：这是 Kafka 的关键设计 - 将"检测完成"和"触发回调"分离
     */
    private void firePendingCompletions() {
        while (true) {
            CompletedRequest completed = pendingCompletion.poll();
            if (completed == null) {
                break;
            }

            System.out.println("[NetworkClient] Firing completion callback");

            if (completed.exception != null) {
                completed.handler.onFailure(completed.exception);
            } else {
                completed.handler.onComplete(completed.response);
            }
        }
    }

    /**
     * 待发送的请求
     */
    private static class PendingRequest {
        final Request request;
        final RequestCompletionHandler handler;

        PendingRequest(Request request, RequestCompletionHandler handler) {
            this.request = request;
            this.handler = handler;
        }
    }

    /**
     * 已完成的请求
     */
    private static class CompletedRequest {
        final RequestCompletionHandler handler;
        final Response response;
        final RuntimeException exception;

        CompletedRequest(RequestCompletionHandler handler, Response response, RuntimeException exception) {
            this.handler = handler;
            this.response = response;
            this.exception = exception;
        }
    }

    /**
     * 🔥 RequestFutureCompletionHandler - 连接网络层和 Future 层
     *
     * 学习点：这是 Kafka 的核心设计 - 将 RequestCompletionHandler 和 RequestFuture 绑定
     */
    private static class RequestFutureCompletionHandler implements RequestCompletionHandler {
        final RequestFuture<Response> future;

        RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        @Override
        public void onComplete(Response response) {
            if (response.isDisconnected()) {
                future.raise(new RuntimeException("Connection disconnected"));
            } else {
                future.complete(response);
            }
        }

        @Override
        public void onFailure(RuntimeException e) {
            future.raise(e);
        }
    }

    public void close() throws IOException {
        selector.close();
    }
}
