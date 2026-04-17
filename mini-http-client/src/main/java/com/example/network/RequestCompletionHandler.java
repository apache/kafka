package com.example.network;

/**
 * 网络请求完成回调接口
 *
 * 学习点：网络层的回调接口，不关心业务逻辑
 */
public interface RequestCompletionHandler {
    /**
     * 请求完成时调用（成功或失败都会调用）
     *
     * @param response 响应对象（可能包含错误信息）
     */
    void onComplete(Response response);

    /**
     * 请求失败时调用（网络错误，不是 HTTP 错误）
     *
     * @param e 异常
     */
    void onFailure(RuntimeException e);
}
