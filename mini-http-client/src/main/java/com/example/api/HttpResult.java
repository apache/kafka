package com.example.api;

/**
 * HTTP 结果（业务层）
 *
 * 学习点：业务层关心的是状态码、body 等，不关心字节流
 */
public class HttpResult {
    private final int statusCode;
    private final String body;
    private final long latencyMs;

    public HttpResult(int statusCode, String body, long latencyMs) {
        this.statusCode = statusCode;
        this.body = body;
        this.latencyMs = latencyMs;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public boolean isSuccess() {
        return statusCode >= 200 && statusCode < 300;
    }

    @Override
    public String toString() {
        return String.format("HttpResult{status=%d, latency=%dms, bodyLength=%d}",
                statusCode, latencyMs, body != null ? body.length() : 0);
    }
}
