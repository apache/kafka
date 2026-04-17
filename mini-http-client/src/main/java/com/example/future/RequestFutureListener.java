package com.example.future;

/**
 * 监听器接口 - 当 Future 完成时回调
 *
 * 学习点：Observer 模式的核心接口
 */
public interface RequestFutureListener<T> {
    /**
     * Future 成功完成时调用
     * @param value Future 的结果值
     */
    void onSuccess(T value);

    /**
     * Future 失败时调用
     * @param exception 失败的异常
     */
    void onFailure(RuntimeException exception);
}
