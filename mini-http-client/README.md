# Mini HTTP Client - Kafka 设计模式学习项目

## 🎯 项目目标

通过实现一个**最小化的异步 HTTP 客户端**，深入学习 Kafka Consumer 中使用的核心设计模式：

- ✅ 单线程 + NIO 异步网络 I/O
- ✅ 三层 Future 架构（网络层 → 协议层 → 业务层）
- ✅ Callback 延迟执行模式
- ✅ Reactor 事件驱动模式
- ✅ 关注点分离（Separation of Concerns）

## 📚 核心概念

### 1. 三层 Future 架构

```
Layer 1: RequestFuture<Response>      (网络层)
            ↓ compose()
Layer 2: RequestFuture<HttpResult>    (协议层)
            ↓ addListener()
Layer 3: 用户 Callback                 (业务层)
```

**为什么需要三层？**
- **Layer 1**: 处理字节流、TCP 连接，不关心 HTTP 协议
- **Layer 2**: 解析 HTTP 响应，提取状态码和 body
- **Layer 3**: 执行用户业务逻辑

### 2. 单线程事件循环

```java
// 所有操作在同一个线程完成
while (running) {
    // 1. 触发已完成的回调
    invokeCompletedCallbacks();

    // 2. 发送待发送的请求
    trySend();

    // 3. 非阻塞检查网络事件
    selector.select(timeout);

    // 4. 处理可读的 Socket
    handleReadable();

    // 5. 再次触发回调
    invokeCompletedCallbacks();
}
```

### 3. Callback 延迟执行

```java
// ❌ 错误做法：直接在 Future 完成时调用
future.addListener(new Listener() {
    void onSuccess(Result r) {
        userCallback.onComplete(r);  // 在 Future 触发栈中调用
    }
});

// ✅ 正确做法：加入队列，稍后调用
future.addListener(new Listener() {
    void onSuccess(Result r) {
        completionQueue.add(new Completion(userCallback, r));
    }
});

// 在明确的时机调用
void invokeCompletedCallbacks() {
    while ((c = completionQueue.poll()) != null) {
        c.callback.onComplete(c.result);
    }
}
```

**为什么延迟执行？**
1. 控制调用栈深度
2. 用户代码抛异常不影响 Future 机制
3. 保证回调执行顺序
4. 上下文清晰（在 `poll()` 中统一调用）

## 🏗️ 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                         用户代码                              │
│  client.getAsync(url, callback)                              │
│          ↓                                                   │
└──────────┼──────────────────────────────────────────────────┘
           ↓
┌─────────────────────────────────────────────────────────────┐
│                    HttpClient (Layer 3)                      │
│  - 封装业务 API                                               │
│  - 管理 completedCallbacks 队列                               │
│  - 在 poll() 中调用用户回调                                   │
└──────────┼──────────────────────────────────────────────────┘
           ↓
    RequestFuture<HttpResult> =
        networkFuture.compose(HttpResponseHandler)
           ↓
┌─────────────────────────────────────────────────────────────┐
│              HttpResponseHandler (Layer 2)                   │
│  - 解析 HTTP 响应                                             │
│  - 提取状态码、body                                           │
│  - 类型转换：Response → HttpResult                            │
└──────────┼──────────────────────────────────────────────────┘
           ↓
    RequestFuture<Response> = networkClient.send(request)
           ↓
┌─────────────────────────────────────────────────────────────┐
│                NetworkClient (Layer 1)                       │
│  - Java NIO (Selector + SocketChannel)                      │
│  - 管理 unsent、inFlight、pendingCompletion 队列              │
│  - 非阻塞发送和接收字节流                                      │
└─────────────────────────────────────────────────────────────┘
```

## 📝 代码结构

### 核心类说明

#### 1. `RequestFuture<T>` - Future 核心实现
```java
// 关键方法：
void complete(T value)              // 成功完成
void raise(RuntimeException e)      // 失败
void addListener(Listener<T>)       // 添加监听器
<S> RequestFuture<S> compose(Adapter<T,S>)  // 🔥 类型转换
```

**学习点**：
- 使用 `AtomicReference` 保证线程安全
- 使用 `ConcurrentLinkedQueue` 存储监听器
- `compose()` 是三层架构的关键

#### 2. `NetworkClient` - 网络层
```java
RequestFuture<Response> send(Request)  // 发送请求
void poll(long timeout)                // 事件循环
```

**学习点**：
- Java NIO Selector 的使用
- 三个队列：unsent、inFlight、pendingCompletion
- `firePendingCompletions()` 触发回调

#### 3. `HttpResponseHandler` - 协议层
```java
extends RequestFutureAdapter<Response, HttpResult>

void onSuccess(Response r, RequestFuture<HttpResult> f) {
    // 解析 HTTP 响应
    HttpResult result = parse(r);
    f.complete(result);
}
```

**学习点**：
- 继承 `RequestFutureAdapter` 实现类型转换
- 处理协议细节（解析 HTTP）

#### 4. `HttpClient` - 业务层
```java
void getAsync(String url, HttpCallback callback) {
    RequestFuture<Response> f1 = network.send(req);
    RequestFuture<HttpResult> f2 = f1.compose(handler);
    f2.addListener(result -> queue.add(callback));
}

void poll(long timeout) {
    invokeCompletedCallbacks();  // 调用用户回调
    network.poll(timeout);       // 网络 I/O
    invokeCompletedCallbacks();  // 再次调用
}
```

**学习点**：
- 封装三层 Future
- 管理用户回调队列
- 提供友好的 API

## 🚀 运行示例

### 编译

```bash
cd mini-http-client
javac -d target/classes src/main/java/com/example/**/*.java
```

### 运行

```bash
java -cp target/classes com.example.demo.Demo
```

### 预期输出

```
╔════════════════════════════════════════════════════════════╗
║  Mini HTTP Client - Kafka 设计模式学习项目                 ║
║  展示：单线程 + NIO + 三层 Future + Callback 延迟执行        ║
╚════════════════════════════════════════════════════════════╝

========== getAsync START ==========
[HttpClient] Request: httpbin.org:80/get
[HttpClient] Layer 1 Future created: RequestFuture<Response>
[HttpClient] Layer 2 Future created: RequestFuture<HttpResult>
[HttpClient] Listener added to Layer 3 Future
========== getAsync END ==========

========== poll START ==========
[NetworkClient] Poll started (timeout=0ms)
[NetworkClient] Connecting to httpbin.org:80
[NetworkClient] Selector returned 1 ready channels
[NetworkClient] Connected, sending request...
[NetworkClient] Reading response...
[HttpResponseHandler] Processing response...
[HttpClient] Layer 3 Future succeeded, queuing callback
[HttpClient] Invoking callback #1
✅ Request succeeded!
   Status: 200
   Latency: 523ms
========== poll END ==========
```

## 🔍 调试技巧

### 1. 观察 Future 链
在 `HttpClient.getAsync()` 中设置断点，观察：
- Layer 1 Future 的创建
- `compose()` 创建 Layer 2 Future
- `addListener()` 注册回调

### 2. 追踪回调触发
在 `NetworkClient.firePendingCompletions()` 设置断点，观察：
- 从 `pendingCompletion` 队列取出 handler
- 调用 `handler.onComplete()`
- 触发 Layer 1 Future

### 3. 观察延迟执行
在 `HttpClient.invokeCompletedCallbacks()` 设置断点，观察：
- 从 `completedCallbacks` 队列取出 completion
- 调用用户的 `callback.onComplete()`

## 📖 学习路径

### 第 1 步：理解 RequestFuture
1. 阅读 `RequestFuture.java`
2. 运行单元测试（TODO：添加测试）
3. 理解 `complete()`、`raise()`、`addListener()` 的实现

### 第 2 步：理解 compose()
1. 阅读 `RequestFuture.compose()`
2. 理解如何将 `RequestFuture<T>` 转换为 `RequestFuture<S>`
3. 在 Demo 中观察 Layer 1 → Layer 2 的转换

### 第 3 步：理解网络层
1. 阅读 `NetworkClient.java`
2. 理解 Java NIO Selector 的使用
3. 观察三个队列的作用

### 第 4 步：理解延迟执行
1. 对比直接调用 vs 队列延迟
2. 理解 `invokeCompletedCallbacks()` 的作用
3. 思考：为什么不直接在 Future 完成时调用？

### 第 5 步：对比 Kafka 源码
1. 打开 Kafka 的 `ConsumerCoordinator.java`
2. 找到 `commitOffsetsAsync()` 方法
3. 对比：
   - `sendOffsetCommitRequest()` ≈ `network.send()`
   - `.compose(handler)` ≈ `.compose(HttpResponseHandler)`
   - `completedOffsetCommits` ≈ `completedCallbacks`
   - `invokeCompletedOffsetCommitCallbacks()` ≈ `invokeCompletedCallbacks()`

## 💡 扩展练习

### 初级
1. 添加 POST 请求支持
2. 添加请求超时处理
3. 添加重试机制

### 中级
1. 支持多个并发连接（连接池）
2. 添加请求取消功能
3. 实现 sync 版本的 API（阻塞等待）

### 高级
1. 添加 HTTPS 支持
2. 实现 HTTP/2
3. 添加请求优先级队列

## 🤔 思考题

1. **为什么 Kafka 不使用独立的网络线程？**
   - 提示：线程安全、锁、上下文切换

2. **compose() 相比直接转换有什么优势？**
   - 提示：关注点分离、可复用、可测试

3. **如果在用户 callback 中再次调用 getAsync()，会发生什么？**
   - 提示：递归、队列、调用栈

4. **如何保证多个请求的回调顺序？**
   - 提示：队列的 FIFO 特性

## 📚 相关资料

- [Kafka 源码：ConsumerCoordinator](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java)
- [Java NIO Tutorial](https://docs.oracle.com/javase/tutorial/essential/io/nio.html)
- [Reactor Pattern](https://en.wikipedia.org/wiki/Reactor_pattern)
- [Future Pattern](https://en.wikipedia.org/wiki/Futures_and_promises)

## 📄 License

本项目仅用于学习目的。

---

**Happy Learning! 🎉**

如有问题，请在 Issues 中讨论。
