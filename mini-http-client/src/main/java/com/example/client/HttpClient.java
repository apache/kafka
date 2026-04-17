package com.example.client;

import com.example.api.HttpCallback;
import com.example.api.HttpResult;
import com.example.future.RequestFuture;
import com.example.future.RequestFutureListener;
import com.example.network.NetworkClient;
import com.example.network.Request;
import com.example.network.Response;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 🔥 HTTP 客户端（Layer 3）
 *
 * 学习点：
 * 1. 封装网络层，提供业务 API
 * 2. 实现三层 Future 架构
 * 3. 使用队列延迟执行用户回调
 * 4. 类似 Kafka 的 ConsumerCoordinator
 */
public class HttpClient {

    private final NetworkClient networkClient;

    // 已完成但未调用的用户回调队列
    private final ConcurrentLinkedQueue<CallbackCompletion> completedCallbacks = new ConcurrentLinkedQueue<>();

    public HttpClient() throws IOException {
        this.networkClient = new NetworkClient();
    }

    /**
     * 🔥 异步 GET 请求
     *
     * 学习点：这是三层 Future 架构的入口
     */
    public void getAsync(String host, int port, String path, HttpCallback callback) {
        System.out.println("\n========== getAsync START ==========");
        System.out.println("[HttpClient] Request: " + host + ":" + port + path);

        // 创建请求
        Request request = new Request(host, port, path);

        // 🔥 Layer 1: 发送网络请求，返回 RequestFuture<Response>
        RequestFuture<Response> networkFuture = networkClient.send(request);
        System.out.println("[HttpClient] Layer 1 Future created: RequestFuture<Response>");

        // 🔥 Layer 2: 使用 compose 转换类型，返回 RequestFuture<HttpResult>
        RequestFuture<HttpResult> businessFuture = networkFuture.compose(new HttpResponseHandler());
        System.out.println("[HttpClient] Layer 2 Future created: RequestFuture<HttpResult>");

        // 🔥 Layer 3: 添加用户回调监听器
        businessFuture.addListener(new RequestFutureListener<HttpResult>() {
            @Override
            public void onSuccess(HttpResult result) {
                System.out.println("[HttpClient] Layer 3 Future succeeded, queuing callback");
                // 不直接调用 callback，而是加入队列
                completedCallbacks.add(new CallbackCompletion(callback, result, null));
            }

            @Override
            public void onFailure(RuntimeException exception) {
                System.out.println("[HttpClient] Layer 3 Future failed, queuing callback");
                // 不直接调用 callback，而是加入队列
                completedCallbacks.add(new CallbackCompletion(callback, null, exception));
            }
        });

        System.out.println("[HttpClient] Listener added to Layer 3 Future");
        System.out.println("========== getAsync END ==========\n");

        // 触发网络 poll（非阻塞）
        pollNoWait();
    }

    /**
     * 🔥 Poll 方法 - 驱动事件循环
     *
     * 学习点：
     * 1. 先触发网络 I/O
     * 2. 然后调用已完成的用户回调
     */
    public void poll(long timeoutMs) throws IOException {
        System.out.println("\n========== poll START ==========");

        // 阶段 1: 先调用已完成的回调
        invokeCompletedCallbacks();

        // 阶段 2: 执行网络 I/O
        networkClient.poll(timeoutMs);

        // 阶段 3: 再次调用已完成的回调
        invokeCompletedCallbacks();

        System.out.println("========== poll END ==========\n");
    }

    /**
     * 非阻塞 poll
     */
    private void pollNoWait() {
        try {
            poll(0);
        } catch (IOException e) {
            System.err.println("[HttpClient] Poll error: " + e.getMessage());
        }
    }

    /**
     * 🔥 调用已完成的回调
     *
     * 学习点：这是 Kafka 的关键设计 - 在主线程的明确时机调用用户回调
     */
    private void invokeCompletedCallbacks() {
        System.out.println("[HttpClient] Invoking completed callbacks...");

        int count = 0;
        while (true) {
            CallbackCompletion completion = completedCallbacks.poll();
            if (completion == null) {
                break;
            }

            count++;
            System.out.println("[HttpClient] Invoking callback #" + count);

            try {
                completion.callback.onComplete(completion.result, completion.exception);
            } catch (Exception e) {
                System.err.println("[HttpClient] Callback threw exception: " + e.getMessage());
                e.printStackTrace();
            }
        }

        if (count > 0) {
            System.out.println("[HttpClient] Invoked " + count + " callbacks");
        } else {
            System.out.println("[HttpClient] No callbacks to invoke");
        }
    }

    /**
     * 等待所有请求完成
     */
    public void waitForCompletion(long maxWaitMs) throws IOException {
        System.out.println("\n[HttpClient] Waiting for completion (max " + maxWaitMs + "ms)...");

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < maxWaitMs) {
            poll(100);

            // 如果没有待完成的回调，退出
            if (completedCallbacks.isEmpty()) {
                System.out.println("[HttpClient] All requests completed");
                return;
            }
        }

        System.out.println("[HttpClient] Timeout waiting for completion");
    }

    /**
     * 回调完成对象
     */
    private static class CallbackCompletion {
        final HttpCallback callback;
        final HttpResult result;
        final Exception exception;

        CallbackCompletion(HttpCallback callback, HttpResult result, Exception exception) {
            this.callback = callback;
            this.result = result;
            this.exception = exception;
        }
    }

    public void close() throws IOException {
        networkClient.close();
    }
}
