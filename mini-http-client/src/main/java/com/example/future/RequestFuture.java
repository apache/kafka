package com.example.future;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 核心 Future 实现 - Kafka RequestFuture 的简化版
 *
 * 学习点：
 * 1. 使用 AtomicReference 实现线程安全的状态管理
 * 2. 使用 ConcurrentLinkedQueue 存储监听器
 * 3. 支持 compose 进行类型转换（关键！）
 */
public class RequestFuture<T> {

    // 标记未完成状态的哨兵值
    private static final Object INCOMPLETE = new Object();

    // 存储结果：可能是 T 类型的值，也可能是 RuntimeException
    private final AtomicReference<Object> result = new AtomicReference<>(INCOMPLETE);

    // 监听器队列（线程安全）
    private final ConcurrentLinkedQueue<RequestFutureListener<T>> listeners = new ConcurrentLinkedQueue<>();

    /**
     * 检查 Future 是否已完成
     */
    public boolean isDone() {
        return result.get() != INCOMPLETE;
    }

    /**
     * 检查是否成功完成
     */
    public boolean succeeded() {
        return isDone() && !failed();
    }

    /**
     * 检查是否失败
     */
    public boolean failed() {
        return result.get() instanceof RuntimeException;
    }

    /**
     * 获取成功的值
     */
    @SuppressWarnings("unchecked")
    public T value() {
        if (!succeeded()) {
            throw new IllegalStateException("Future has not succeeded");
        }
        return (T) result.get();
    }

    /**
     * 获取失败的异常
     */
    public RuntimeException exception() {
        if (!failed()) {
            throw new IllegalStateException("Future has not failed");
        }
        return (RuntimeException) result.get();
    }

    /**
     * 完成 Future（成功）
     *
     * 学习点：使用 CAS 保证只能完成一次
     */
    public void complete(T value) {
        if (value instanceof RuntimeException) {
            throw new IllegalArgumentException("Value cannot be an exception");
        }

        if (!result.compareAndSet(INCOMPLETE, value)) {
            throw new IllegalStateException("Future is already complete");
        }

        // 触发所有监听器
        fireSuccess();
    }

    /**
     * 完成 Future（失败）
     */
    public void raise(RuntimeException e) {
        if (e == null) {
            throw new IllegalArgumentException("Exception cannot be null");
        }

        if (!result.compareAndSet(INCOMPLETE, e)) {
            throw new IllegalStateException("Future is already complete");
        }

        // 触发所有监听器
        fireFailure();
    }

    /**
     * 添加监听器
     *
     * 学习点：如果 Future 已完成，立即触发监听器
     */
    public void addListener(RequestFutureListener<T> listener) {
        listeners.add(listener);

        // 如果已经完成，立即触发
        if (failed()) {
            fireFailure();
        } else if (succeeded()) {
            fireSuccess();
        }
    }

    /**
     * 🔥 核心方法：compose - 类型转换
     *
     * 学习点：这是 Kafka 三层 Future 的关键！
     * 将 RequestFuture<F> 转换为 RequestFuture<T>
     *
     * 例如：RequestFuture<Response> -> RequestFuture<HttpResult>
     */
    public <S> RequestFuture<S> compose(final RequestFutureAdapter<T, S> adapter) {
        // 创建新的 Future
        final RequestFuture<S> adapted = new RequestFuture<>();

        // 给当前 Future 添加监听器
        addListener(new RequestFutureListener<T>() {
            @Override
            public void onSuccess(T value) {
                // 调用 adapter 转换
                adapter.onSuccess(value, adapted);
            }

            @Override
            public void onFailure(RuntimeException e) {
                // 调用 adapter 处理失败
                adapter.onFailure(e, adapted);
            }
        });

        // 返回新的 Future
        return adapted;
    }

    /**
     * 触发成功监听器
     */
    private void fireSuccess() {
        T value = value();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null) {
                break;
            }
            listener.onSuccess(value);
        }
    }

    /**
     * 触发失败监听器
     */
    private void fireFailure() {
        RuntimeException exception = exception();
        while (true) {
            RequestFutureListener<T> listener = listeners.poll();
            if (listener == null) {
                break;
            }
            listener.onFailure(exception);
        }
    }

    /**
     * 静态工厂方法：创建已成功的 Future
     */
    public static <T> RequestFuture<T> success(T value) {
        RequestFuture<T> future = new RequestFuture<>();
        future.complete(value);
        return future;
    }

    /**
     * 静态工厂方法：创建已失败的 Future
     */
    public static <T> RequestFuture<T> failure(RuntimeException e) {
        RequestFuture<T> future = new RequestFuture<>();
        future.raise(e);
        return future;
    }
}
