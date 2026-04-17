package com.example.client;

import com.example.api.HttpResult;
import com.example.future.RequestFuture;
import com.example.future.RequestFutureAdapter;
import com.example.network.Response;

/**
 * 🔥 HTTP 响应处理器（Layer 2）
 *
 * 学习点：
 * 1. 继承 RequestFutureAdapter，实现类型转换
 * 2. 将 Response（字节流） -> HttpResult（业务对象）
 * 3. 类似 Kafka 的 OffsetCommitResponseHandler
 */
public class HttpResponseHandler extends RequestFutureAdapter<Response, HttpResult> {

    @Override
    public void onSuccess(Response response, RequestFuture<HttpResult> future) {
        System.out.println("[HttpResponseHandler] Processing response: " + response);

        try {
            // 解析 HTTP 响应
            String rawResponse = response.getRawResponse();

            if (rawResponse.isEmpty()) {
                future.raise(new RuntimeException("Empty response"));
                return;
            }

            // 简单的 HTTP 解析（生产环境需要更复杂的解析）
            String[] lines = rawResponse.split("\r\n");
            if (lines.length < 1) {
                future.raise(new RuntimeException("Invalid HTTP response"));
                return;
            }

            // 解析状态行：HTTP/1.1 200 OK
            String statusLine = lines[0];
            String[] parts = statusLine.split(" ");
            if (parts.length < 2) {
                future.raise(new RuntimeException("Invalid status line: " + statusLine));
                return;
            }

            int statusCode;
            try {
                statusCode = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                future.raise(new RuntimeException("Invalid status code: " + parts[1]));
                return;
            }

            // 查找空行（头部和 body 的分隔）
            int bodyStart = -1;
            for (int i = 0; i < lines.length; i++) {
                if (lines[i].isEmpty()) {
                    bodyStart = i + 1;
                    break;
                }
            }

            // 提取 body
            StringBuilder body = new StringBuilder();
            if (bodyStart > 0 && bodyStart < lines.length) {
                for (int i = bodyStart; i < lines.length; i++) {
                    body.append(lines[i]);
                    if (i < lines.length - 1) {
                        body.append("\r\n");
                    }
                }
            }

            // 创建结果
            HttpResult result = new HttpResult(statusCode, body.toString(), response.getLatencyMs());

            System.out.println("[HttpResponseHandler] Parsed result: " + result);

            // 完成 Future
            future.complete(result);

        } catch (Exception e) {
            System.err.println("[HttpResponseHandler] Error parsing response: " + e.getMessage());
            future.raise(new RuntimeException("Failed to parse response", e));
        }
    }

    @Override
    public void onFailure(RuntimeException e, RequestFuture<HttpResult> future) {
        System.err.println("[HttpResponseHandler] Request failed: " + e.getMessage());
        // 直接传递异常
        future.raise(e);
    }
}
