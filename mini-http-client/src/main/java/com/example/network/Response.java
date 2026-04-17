package com.example.network;

/**
 * HTTP 响应（网络层 - 字节级）
 *
 * 学习点：网络层只关心字节，不关心业务语义
 */
public class Response {
    private final Request request;
    private final String rawResponse;  // 原始 HTTP 响应文本
    private final long receivedTime;
    private final boolean disconnected;

    public Response(Request request, String rawResponse, boolean disconnected) {
        this.request = request;
        this.rawResponse = rawResponse;
        this.receivedTime = System.currentTimeMillis();
        this.disconnected = disconnected;
    }

    public Request getRequest() {
        return request;
    }

    public String getRawResponse() {
        return rawResponse;
    }

    public long getLatencyMs() {
        return receivedTime - request.getCreatedTime();
    }

    public boolean isDisconnected() {
        return disconnected;
    }

    @Override
    public String toString() {
        return String.format("Response{request=%s, latency=%dms, disconnected=%s}",
                request, getLatencyMs(), disconnected);
    }
}
