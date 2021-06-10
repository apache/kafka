/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.zookeeper

import java.util.Locale
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent._
import java.util.{List => JList}

import com.yammer.metrics.core.MetricName
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.{KafkaScheduler, Logging}
import kafka.zookeeper.ZooKeeperClient._
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.AsyncCallback.{Children2Callback, DataCallback, StatCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper._
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.collection.mutable.Set

object ZooKeeperClient {
  val RetryBackoffMs = 1000
}

/**
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString       comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs    session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking.
 * @param name                name of the client instance
 * @param zkClientConfig      ZooKeeper client configuration, for TLS configs if desired
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int,
                      time: Time,
                      metricGroup: String,
                      metricType: String,
                      name: Option[String],
                      zkClientConfig: Option[ZKClientConfig]) extends Logging with KafkaMetricsGroup {

  def this(connectString: String,
           sessionTimeoutMs: Int,
           connectionTimeoutMs: Int,
           maxInFlightRequests: Int,
           time: Time,
           metricGroup: String,
           metricType: String) = {
    this(connectString, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests, time, metricGroup, metricType, None,
      None)
  }

  this.logIdent = name match {
    case Some(n) => s"[ZooKeeperClient $n] "
    case _ => "[ZooKeeperClient] "
  }
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()

  /**
   * 节点发生变化时触发的handler集合
   * key：Zookeeper对应的节点路径，value：路径变更后调用的处理方法
   */
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala

  /**
   * 子节点发生变化时触发的handler集合
   */
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala
  private val inFlightRequests = new Semaphore(maxInFlightRequests)

  // 状态处理器，主要用于Zookeeper session 过期
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala

  //
  private[zookeeper] val reinitializeScheduler = new KafkaScheduler(threads = 1, s"zk-client-${threadPrefix}reinit-")
  private var isFirstConnectionEstablished = false

  private val metricNames = Set[String]()

  // The state map has to be created before creating ZooKeeper since it's needed in the ZooKeeper callback.
  private val stateToMeterMap = {
    import KeeperState._
    val stateToEventTypeMap = Map(
      Disconnected -> "Disconnects",
      SyncConnected -> "SyncConnects",
      AuthFailed -> "AuthFailures",
      ConnectedReadOnly -> "ReadOnlyConnects",
      SaslAuthenticated -> "SaslAuthentications",
      Expired -> "Expires"
    )
    stateToEventTypeMap.map { case (state, eventType) =>
      val name = s"ZooKeeper${eventType}PerSec"
      metricNames += name
      state -> newMeter(name, eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
    }
  }

  private val clientConfig = zkClientConfig getOrElse new ZKClientConfig()

  info(s"Initializing a new session to $connectString.")
  // Fail-fast if there's an error during construction (so don't call initialize, which retries forever)
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher,
    clientConfig)

  private[zookeeper] def getClientConfig = clientConfig

  newGauge("SessionState", () => connectionState.toString)

  metricNames += "SessionState"

  reinitializeScheduler.startup()
  try waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)
  catch {
    case e: Throwable =>
      close()
      throw e
  }

  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName(metricGroup, metricType, name, metricTags)
  }

  /**
   * Return the state of the ZooKeeper connection.
   */
  def connectionState: States = zooKeeper.getState

  /**
   * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single request to send and wait on.
   * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  /**
   * Send a pipelined sequence of requests and wait for all of their responses.
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the request.
   *
   * @param requests a sequence of requests to send and wait on.
   * @return the responses for the requests. If all requests have the same type, the responses will have the respective
   *         response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   *         will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    if (requests.isEmpty)
      Seq.empty
    else {
      val countDownLatch = new CountDownLatch(requests.size)
      val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)

      requests.foreach { request =>
        inFlightRequests.acquire()
        try {
          inReadLock(initializationLock) {
            send(request) { response =>
              responseQueue.add(response)
              inFlightRequests.release()
              countDownLatch.countDown()
            }
          }
        } catch {
          case e: Throwable =>
            inFlightRequests.release()
            throw e
        }
      }
      countDownLatch.await()
      responseQueue.asScala.toBuffer
    }
  }

  /**
   * 发送请求
   *
   * @param request
   * @param processResponse
   * @tparam Req
   */
  private[zookeeper] def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    // Safe to cast as we always create a response of the right type
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    def responseMetadata(sendTimeMs: Long) = new ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    val sendTimeMs = time.hiResClockMs()

    // Cast to AsyncRequest to workaround a scalac bug that results in an false exhaustiveness warning
    // with -Xlint:strict-unsealed-patmat
    (request: AsyncRequest) match {
      // 这个请求可以监听一个尚未存在的节点，从无到有、节点数据的变化，从有到无都可以完成
      case ExistsRequest(path, ctx) =>
        zooKeeper.exists(path, shouldWatch(request), new StatCallback {
          def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(ExistsResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetDataRequest(path, ctx) =>
        zooKeeper.getData(path, shouldWatch(request), new DataCallback {
          def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
            callback(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetChildrenRequest(path, _, ctx) =>
        zooKeeper.getChildren(path, shouldWatch(request), new Children2Callback {
          def processResult(rc: Int, path: String, ctx: Any, children: JList[String], stat: Stat): Unit =
            callback(GetChildrenResponse(Code.get(rc), path, Option(ctx), Option(children).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case CreateRequest(path, data, acl, createMode, ctx) =>
        zooKeeper.create(path, data, acl.asJava, createMode,
          (rc, path, ctx, name) =>
            callback(CreateResponse(Code.get(rc), path, Option(ctx), name, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case SetDataRequest(path, data, version, ctx) =>
        zooKeeper.setData(path, data, version,
          (rc, path, ctx, stat) =>
            callback(SetDataResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case DeleteRequest(path, version, ctx) =>
        zooKeeper.delete(path, version,
          (rc, path, ctx) => callback(DeleteResponse(Code.get(rc), path, Option(ctx), responseMetadata(sendTimeMs))),
          ctx.orNull)
      case GetAclRequest(path, ctx) =>
        zooKeeper.getACL(path, null,
          (rc, path, ctx, acl, stat) =>
            callback(GetAclResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case SetAclRequest(path, acl, version, ctx) =>
        zooKeeper.setACL(path, acl.asJava, version,
          (rc, path, ctx, stat) =>
            callback(SetAclResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs))),
          ctx.orNull)
      case MultiRequest(zkOps, ctx) =>
        def toZkOpResult(opResults: JList[OpResult]): Seq[ZkOpResult] =
          Option(opResults).map(results => zkOps.zip(results.asScala).map { case (zkOp, result) =>
            ZkOpResult(zkOp, result)
          }).orNull

        zooKeeper.multi(zkOps.map(_.toZookeeperOp).asJava,
          (rc, path, ctx, opResults) =>
            callback(MultiResponse(Code.get(rc), path, Option(ctx), toZkOpResult(opResults), responseMetadata(sendTimeMs))),
          ctx.orNull)
    }
  }

  /**
   * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
   *
   * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
   * @throws ZooKeeperClientExpiredException    if the session expired either before or while waiting for connection.
   */
  def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    waitUntilConnected(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  private def waitUntilConnected(timeout: Long, timeUnit: TimeUnit): Unit = {
    info("Waiting until connected.")
    var nanos = timeUnit.toNanos(timeout)
    inLock(isConnectedOrExpiredLock) {
      var state = connectionState
      while (!state.isConnected && state.isAlive) {
        if (nanos <= 0) {
          throw new ZooKeeperClientTimeoutException(s"Timed out waiting for connection while in state: $state")
        }
        nanos = isConnectedOrExpiredCondition.awaitNanos(nanos)
        state = connectionState
      }
      if (state == States.AUTH_FAILED) {
        throw new ZooKeeperClientAuthFailedException("Auth failed either before or while waiting for connection")
      } else if (state == States.CLOSED) {
        throw new ZooKeeperClientExpiredException("Session expired either before or while waiting for connection")
      }
      isFirstConnectionEstablished = true
    }
    info("Connected.")
  }

  // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
  // may need to be updated.
  private def shouldWatch(request: AsyncRequest): Boolean = request match {
    case GetChildrenRequest(_, registerWatch, _) => registerWatch && zNodeChildChangeHandlers.contains(request.path)
    case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(request.path)
    case _ => throw new IllegalArgumentException(s"Request $request is not watchable")
  }

  /**
   * 向 ZookeeperClient 注册一个处理器（handler）。仅仅是一个本地方法，不会真正向ZK注册一个监听器（watcher）
   * 只有当用户使用 GetDataRequest 或 ExistsRequest 调用  handle(AsyncRequest) 或 handle(Seq[AsyncRequest])
   * 时，都会注册监听器。
   * 注意：Zookeeper只允许使用 ExistsRequest 注册一个不存在的节点
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   *
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path, zNodeChildChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   *
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  /**
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = inReadLock(initializationLock) {
    if (stateChangeHandler != null)
      stateChangeHandlers.put(stateChangeHandler.name, stateChangeHandler)
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = inReadLock(initializationLock) {
    stateChangeHandlers.remove(name)
  }

  def close(): Unit = {
    info("Closing.")

    // Shutdown scheduler outside of lock to avoid deadlock if scheduler
    // is waiting for lock to process session expiry. Close expiry thread
    // first to ensure that new clients are not created during close().
    reinitializeScheduler.shutdown()

    inWriteLock(initializationLock) {
      zNodeChangeHandlers.clear()
      zNodeChildChangeHandlers.clear()
      stateChangeHandlers.clear()
      zooKeeper.close()
      metricNames.foreach(removeMetric(_))
    }
    info("Closed.")
  }

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
  }

  // Only for testing
  private[kafka] def currentZooKeeper: ZooKeeper = inReadLock(initializationLock) {
    zooKeeper
  }

  /**
   * 会话过期，当前Broker和ZK断连，重新创建新的ZK客户端直接成功建立连接或超时后返回
   */
  private def reinitialize(): Unit = {
    // Initialization callbacks are invoked outside of the lock to avoid deadlock potential since their completion
    // may require additional Zookeeper requests, which will block to acquire the initialization lock
    // #1 遍历状态变更处理器，触发「beforeInitializingSession」回调
    stateChangeHandlers.values.foreach(callBeforeInitializingSession _)

    // #2 加写锁
    inWriteLock(initializationLock) {
      if (!connectionState.isAlive) {
        // #3 如果和ZK断开连接，则调用close()关闭客户端
        zooKeeper.close()
        info(s"Initializing a new session to $connectString.")
        // retry forever until ZooKeeper can be instantiated
        var connected = false

        // #4 创建一个新的ZK客户端
        while (!connected) {
          try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher, clientConfig)
            connected = true
          } catch {
            case e: Exception =>
              info("Error when recreating ZooKeeper, retrying after a short sleep", e)
              Thread.sleep(RetryBackoffMs)
          }
        }
      }
    }

    // #5 触发「afterInitializingSession」回调方法
    stateChangeHandlers.values.foreach(callAfterInitializingSession _)
  }

  /**
   * Close the zookeeper client to force session reinitialization. This is visible for testing only.
   */
  private[zookeeper] def forceReinitialize(): Unit = {
    zooKeeper.close()
    reinitialize()
  }

  /**
   * 触发回调方法，将 Expire 事件注册到事件管理器中
   *
   * @param handler
   */
  private def callBeforeInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.beforeInitializingSession()
    } catch {
      case t: Throwable =>
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }

  /**
   * 触发回调方法，将 RegisterBrokerAndReelect 事件注册到事件管理器中
   *
   * @param handler
   */
  private def callAfterInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.afterInitializingSession()
    } catch {
      case t: Throwable =>
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }

  // Visibility for testing
  private[zookeeper] def scheduleReinitialize(name: String, message: String, delayMs: Long): Unit = {
    reinitializeScheduler.schedule(name, () => {
      info(message)
      reinitialize()
    }, delayMs, period = -1L, unit = TimeUnit.MILLISECONDS)
  }

  private def threadPrefix: String = name.map(n => n.replaceAll("\\s", "") + "-").getOrElse("")

  /**
   * Zookeeper监听器，继承Zookeeper提供的 {@link Watcher} 接口
   * 实现 {@link Watcher.processor} 方法
   */
  private[zookeeper] object ZooKeeperClientWatcher extends Watcher {

    /**
     * 处理Zookeeper的事件，从相应集合中找到合适的Handler处理响应
     *
     * @param event Zookeeper事件
     */
    override def process(event: WatchedEvent): Unit = {
      debug(s"Received event: $event")
      // 核心步骤是根据 路径+EventType匹配Handler
      Option(event.getPath) match {
        case None =>
          // 没有路径，表明是状态改变，比如认证失败，但最常见的是会话过期
          val state = event.getState
          stateToMeterMap.get(state).foreach(_.mark())
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (state == KeeperState.AuthFailed) {
            // 认证失败
            error("Auth failed.")
            stateChangeHandlers.values.foreach(_.onAuthFailure())

            // If this is during initial startup, we fail fast. Otherwise, schedule retry.
            val initialized = inLock(isConnectedOrExpiredLock) {
              isFirstConnectionEstablished
            }
            if (initialized)
              scheduleReinitialize("auth-failed", "Reinitializing due to auth failure.", RetryBackoffMs)
          } else if (state == KeeperState.Expired) {
            // 会话过期
            scheduleReinitialize("session-expired", "Session expired.", delayMs = 0L)
          }
        case Some(path) =>
          // 根据事件类型从缓存中获取对应的handler处理器并回调相关方法
          (event.getType: @unchecked) match {
            // 子节点数据变更事件
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            // 节点被创建事件
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            // 节点被删除事件
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            // 节点数据变更事件
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }
}

/**
 * 状态处理器
 */
trait StateChangeHandler {
  val name: String

  def beforeInitializingSession(): Unit = {}

  def afterInitializingSession(): Unit = {}

  def onAuthFailure(): Unit = {}
}

/**
 * Zookeeper节点变更就会触发相关回调方法
 */
trait ZNodeChangeHandler {
  /**
   * 对应Zookeeper的路径
   */
  val path: String

  /**
   * 处理创建节点事件
   */
  def handleCreation(): Unit = {}

  /**
   * 处理节点被删除事件
   */
  def handleDeletion(): Unit = {}

  /**
   * 处理节点数据改变事件
   */
  def handleDataChange(): Unit = {}
}

/**
 * ZK中子节点变更触发器
 */
trait ZNodeChildChangeHandler {
  val path: String

  /**
   * 处理子节点变更事件
   */
  def handleChildChange(): Unit = {}
}

// Thin wrapper for zookeeper.Op
sealed trait ZkOp {
  def toZookeeperOp: Op
}

case class CreateOp(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode) extends ZkOp {
  override def toZookeeperOp: Op = Op.create(path, data, acl.asJava, createMode)
}

case class DeleteOp(path: String, version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.delete(path, version)
}

case class SetDataOp(path: String, data: Array[Byte], version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.setData(path, data, version)
}

case class CheckOp(path: String, version: Int) extends ZkOp {
  override def toZookeeperOp: Op = Op.check(path, version)
}

case class ZkOpResult(zkOp: ZkOp, rawOpResult: OpResult)

sealed trait AsyncRequest {
  /**
   * This type member allows us to define methods that take requests and return responses with the correct types.
   * See ``ZooKeeperClient.handleRequests`` for example.
   */
  type Response <: AsyncResponse

  def path: String

  def ctx: Option[Any]
}

case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode,
                         ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse
}

case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse
}

case class ExistsRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = ExistsResponse
}

case class GetDataRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetDataResponse
}

case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse
}

case class GetAclRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetAclResponse
}

case class SetAclRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetAclResponse
}

case class GetChildrenRequest(path: String, registerWatch: Boolean, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
}

case class MultiRequest(zkOps: Seq[ZkOp], ctx: Option[Any] = None) extends AsyncRequest {
  type Response = MultiResponse

  override def path: String = null
}


sealed abstract class AsyncResponse {
  def resultCode: Code

  def path: String

  def ctx: Option[Any]

  /** Return None if the result code is OK and KeeperException otherwise. */
  def resultException: Option[KeeperException] =
    if (resultCode == Code.OK) None else Some(KeeperException.create(resultCode, path))

  /**
   * Throw KeeperException if the result code is not OK.
   */
  def maybeThrow(): Unit = {
    if (resultCode != Code.OK)
      throw KeeperException.create(resultCode, path)
  }

  def metadata: ResponseMetadata
}

case class ResponseMetadata(sendTimeMs: Long, receivedTimeMs: Long) {
  def responseTimeMs: Long = receivedTimeMs - sendTimeMs
}

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String,
                          metadata: ResponseMetadata) extends AsyncResponse

case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any],
                          metadata: ResponseMetadata) extends AsyncResponse

case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse

case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse

case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse

case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse

case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse

case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse

case class MultiResponse(resultCode: Code, path: String, ctx: Option[Any], zkOpResults: Seq[ZkOpResult],
                         metadata: ResponseMetadata) extends AsyncResponse

class ZooKeeperClientException(message: String) extends RuntimeException(message)

class ZooKeeperClientExpiredException(message: String) extends ZooKeeperClientException(message)

class ZooKeeperClientAuthFailedException(message: String) extends ZooKeeperClientException(message)

class ZooKeeperClientTimeoutException(message: String) extends ZooKeeperClientException(message)
