package com.example.future;

/**
 * Future 适配器 - 用于类型转换
 *
 * 学习点：Adapter 模式，实现 Future 的类型转换
 * 例如：RequestFuture<Response> -> RequestFuture<HttpResult>
 */
public abstract class RequestFutureAdapter<F, T> implements RequestFutureListener<F> {

    /**
     * 成功时的转换逻辑
     * @param value 原始类型的值
     * @param future 目标类型的 Future（用于设置结果）
     */
    public abstract void onSuccess(F value, RequestFuture<T> future);

    /**
     * 失败时的转换逻辑
     * @param e 异常
     * @param future 目标类型的 Future（用于设置失败）
     */
    public abstract void onFailure(RuntimeException e, RequestFuture<T> future);

    // 这两个方法实现 RequestFutureListener 接口
    // 但不应该被直接调用，而是通过 compose() 方法使用
    @Override
    public final void onSuccess(F value) {
        throw new UnsupportedOperationException("Use onSuccess(F, RequestFuture<T>) instead");
    }

    @Override
    public final void onFailure(RuntimeException exception) {
        throw new UnsupportedOperationException("Use onFailure(RuntimeException, RequestFuture<T>) instead");
    }
}
