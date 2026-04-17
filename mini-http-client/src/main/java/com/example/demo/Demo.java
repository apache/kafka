package com.example.demo;

import com.example.api.HttpCallback;
import com.example.api.HttpResult;
import com.example.client.HttpClient;

import java.io.IOException;

/**
 * 🔥 演示程序
 *
 * 学习点：
 * 1. 体验异步非阻塞 API
 * 2. 观察三层 Future 的执行流程
 * 3. 理解单线程事件循环
 */
public class Demo {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║  Mini HTTP Client - Kafka 设计模式学习项目                 ║");
        System.out.println("║  展示：单线程 + NIO + 三层 Future + Callback 延迟执行        ║");
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");

        // 创建客户端
        HttpClient client = new HttpClient();

        System.out.println("===============================================");
        System.out.println("示例 1: 单个异步请求");
        System.out.println("===============================================\n");

        // 发起异步请求
        client.getAsync("httpbin.org", 80, "/get", new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                if (exception != null) {
                    System.err.println("❌ Request failed: " + exception.getMessage());
                } else {
                    System.out.println("✅ Request succeeded!");
                    System.out.println("   Status: " + result.getStatusCode());
                    System.out.println("   Latency: " + result.getLatencyMs() + "ms");
                    System.out.println("   Body preview: " +
                        (result.getBody().length() > 100
                            ? result.getBody().substring(0, 100) + "..."
                            : result.getBody()));
                }
            }
        });

        System.out.println("\n⏳ 请求已发送，现在调用 poll() 等待响应...\n");

        // 等待完成（通过循环 poll）
        client.waitForCompletion(10000);

        System.out.println("\n===============================================");
        System.out.println("示例 2: 多个并发异步请求");
        System.out.println("===============================================\n");

        // 发起多个请求
        String[] paths = {"/get", "/headers", "/ip"};
        for (int i = 0; i < paths.length; i++) {
            final int index = i;
            client.getAsync("httpbin.org", 80, paths[i], new HttpCallback() {
                @Override
                public void onComplete(HttpResult result, Exception exception) {
                    if (exception != null) {
                        System.err.println("❌ Request #" + index + " failed: " + exception.getMessage());
                    } else {
                        System.out.println("✅ Request #" + index + " succeeded! Status=" +
                            result.getStatusCode() + ", Latency=" + result.getLatencyMs() + "ms");
                    }
                }
            });
        }

        // 等待所有完成
        client.waitForCompletion(10000);

        System.out.println("\n===============================================");
        System.out.println("示例 3: 手动控制事件循环");
        System.out.println("===============================================\n");

        System.out.println("发起请求，但不立即等待...");
        client.getAsync("httpbin.org", 80, "/delay/1", new HttpCallback() {
            @Override
            public void onComplete(HttpResult result, Exception exception) {
                System.out.println("🎉 延迟请求完成！");
            }
        });

        System.out.println("做一些其他工作...");
        Thread.sleep(500);

        System.out.println("现在手动 poll...");
        for (int i = 0; i < 20; i++) {
            System.out.println("Poll #" + (i + 1));
            client.poll(200);
            Thread.sleep(100);
        }

        // 关闭客户端
        client.close();

        System.out.println("\n╔════════════════════════════════════════════════════════════╗");
        System.out.println("║  演示完成！                                                 ║");
        System.out.println("║                                                            ║");
        System.out.println("║  关键观察点：                                               ║");
        System.out.println("║  1. 所有操作在单线程完成                                    ║");
        System.out.println("║  2. 三层 Future 的转换过程                                  ║");
        System.out.println("║  3. Callback 在 poll() 中延迟执行                           ║");
        System.out.println("║  4. 非阻塞 I/O 的工作方式                                   ║");
        System.out.println("╚════════════════════════════════════════════════════════════╝");
    }
}
