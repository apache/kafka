package com.example.network;

/**
 * HTTP 请求（简化版）
 */
public class Request {
    private final String host;
    private final int port;
    private final String path;
    private final long createdTime;

    public Request(String host, int port, String path) {
        this.host = host;
        this.port = port;
        this.path = path;
        this.createdTime = System.currentTimeMillis();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * 构建 HTTP 请求字节流
     */
    public String toHttpString() {
        return String.format(
            "GET %s HTTP/1.1\r\n" +
            "Host: %s:%d\r\n" +
            "Connection: close\r\n" +
            "\r\n",
            path, host, port
        );
    }

    @Override
    public String toString() {
        return String.format("Request{%s:%d%s}", host, port, path);
    }
}
