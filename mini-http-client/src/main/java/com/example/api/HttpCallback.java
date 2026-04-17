package com.example.api;

/**
 * 用户回调接口
 *
 * 学习点：类似 Kafka 的 OffsetCommitCallback
 */
public interface HttpCallback {
    /**
     * 请求完成时调用
     *
     * @param result 结果（如果成功）
     * @param exception 异常（如果失败）
     */
    void onComplete(HttpResult result, Exception exception);
}
