# 快速开始 - 5 分钟上手

## 🚀 运行 Demo

```bash
cd mini-http-client

# 1. 编译
./build.sh

# 2. 运行
./run.sh
```

## 📖 学习路径（推荐顺序）

### Step 1: 运行并观察输出（5 分钟）
```bash
./run.sh
```

观察输出，注意：
- `[NetworkClient]` - 网络层日志
- `[HttpResponseHandler]` - 协议层日志
- `[HttpClient]` - 业务层日志
- `✅` - 用户回调执行

### Step 2: 阅读核心代码（30 分钟）

按顺序阅读：

1. **`RequestFuture.java`** (15 分钟)
   - 理解 `complete()` 和 `raise()`
   - 🔥 **重点**：`compose()` 方法（三层架构的核心）
   - 理解 `addListener()` 的触发时机

2. **`NetworkClient.java`** (15 分钟)
   - 理解 `send()` 和 `poll()` 的配合
   - 观察三个队列：`unsent`、`inFlight`、`pendingCompletion`
   - 🔥 **重点**：`firePendingCompletions()` 方法

### Step 3: 理解三层架构（20 分钟）

打开 `HttpClient.getAsync()`，追踪：

```java
// Layer 1: 网络层
RequestFuture<Response> networkFuture = networkClient.send(request);

// Layer 2: 协议层（compose 转换）
RequestFuture<HttpResult> businessFuture =
    networkFuture.compose(new HttpResponseHandler());

// Layer 3: 业务层（添加用户回调）
businessFuture.addListener(new RequestFutureListener<HttpResult>() {
    void onSuccess(HttpResult result) {
        completedCallbacks.add(new Completion(callback, result));
    }
});
```

**关键问题**：
- Q: 为什么要三层？
- A: 分离关注点 - 网络不关心 HTTP，HTTP 不关心业务

- Q: `compose()` 做了什么？
- A: 给 Layer 1 Future 添加 listener，当它完成时调用 handler 转换类型

### Step 4: 对比 Kafka 源码（30 分钟）

打开 Kafka 源码：`ConsumerCoordinator.java`

找到 `doCommitOffsetsAsync()` 方法（约 955 行），对比：

| Mini HTTP Client | Kafka ConsumerCoordinator |
|-----------------|---------------------------|
| `networkClient.send(request)` | `sendOffsetCommitRequest(offsets)` |
| `.compose(new HttpResponseHandler())` | `.compose(new OffsetCommitResponseHandler())` |
| `completedCallbacks` | `completedOffsetCommits` |
| `invokeCompletedCallbacks()` | `invokeCompletedOffsetCommitCallbacks()` |

**发现**：完全一样的设计！

### Step 5: 调试实践（20 分钟）

#### 实验 1: 观察 Future 链
在 `HttpClient.getAsync()` 中添加断点：
```java
RequestFuture<Response> networkFuture = networkClient.send(request);
// ← 断点：观察 networkFuture 的 listeners 队列

RequestFuture<HttpResult> businessFuture = networkFuture.compose(handler);
// ← 断点：观察 networkFuture.listeners 多了一个 listener

businessFuture.addListener(...);
// ← 断点：观察 businessFuture.listeners 多了一个 listener
```

#### 实验 2: 观察回调延迟执行
在 `RequestFutureListener.onSuccess()` 中添加打印：
```java
businessFuture.addListener(new RequestFutureListener<HttpResult>() {
    void onSuccess(HttpResult result) {
        System.out.println("🔥 我被调用了！但用户回调还没执行");
        completedCallbacks.add(...);
        System.out.println("✅ 用户回调已加入队列");
    }
});
```

在 `invokeCompletedCallbacks()` 中添加打印：
```java
void invokeCompletedCallbacks() {
    while (...) {
        System.out.println("🎯 现在才真正调用用户回调");
        completion.callback.onComplete(...);
    }
}
```

## 🎯 核心知识点总结

### 1. 为什么用单线程？
- 无需线程同步（无锁）
- 无上下文切换开销
- 代码逻辑简单（易调试）

### 2. 为什么用三层 Future？
```
网络层 (Response)      - 只管字节流
   ↓ compose
协议层 (HttpResult)    - 解析 HTTP
   ↓ addListener
业务层 (Callback)      - 用户逻辑
```

### 3. 为什么延迟执行 Callback？
```java
// ❌ 直接调用 - 在 Future 触发的栈中执行
future.addListener(r -> callback.onComplete(r));

// ✅ 延迟调用 - 在 poll() 的栈中执行
future.addListener(r -> queue.add(callback));
poll() {
    invokeCallbacks();  // 明确的调用时机
}
```

**好处**：
1. 控制调用栈深度（避免深层递归）
2. 异常隔离（用户代码抛异常不影响 Future）
3. 顺序保证（通过队列）
4. 上下文清晰（在 `poll()` 中统一调用）

## 🤔 思考题

1. 如果去掉 `compose()`，直接在 Layer 1 Future 中解析 HTTP，会有什么问题？

2. 如果不延迟执行 Callback，直接在 `RequestFutureListener.onSuccess()` 中调用用户回调，会有什么问题？

3. Kafka 为什么选择单线程而不是线程池？

## 📚 下一步

- 阅读完整的 [README.md](README.md)
- 尝试扩展练习（添加 POST、超时、重试）
- 阅读 Kafka 完整源码

---

**开始手搓吧！🔥**
