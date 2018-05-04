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
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, Semaphore, TimeUnit}

import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.{KafkaScheduler, Logging}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConverters._
import scala.collection.mutable.Set

/**
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server
 * @param sessionTimeoutMs session timeout in milliseconds
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking.
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int,
                      time: Time,
                      metricGroup: String,
                      metricType: String) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[ZooKeeperClient] "
  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala
  private val inFlightRequests = new Semaphore(maxInFlightRequests)
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala
  private[zookeeper] val expiryScheduler = new KafkaScheduler(0, "zk-session-expiry-handler")

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

  info(s"Initializing a new session to $connectString.")
  // Fail-fast if there's an error during construction (so don't call initialize, which retries forever)
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)

  newGauge("SessionState", new Gauge[String] {
    override def value: String = Option(connectionState.toString).getOrElse("DISCONNECTED")
  })

  metricNames += "SessionState"

  expiryScheduler.startup()
  waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)

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
   * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
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

  // Visibility to override for testing
  private[zookeeper] def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    // Safe to cast as we always create a response of the right type
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    def responseMetadata(sendTimeMs: Long) = new ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    val sendTimeMs = time.hiResClockMs()
    request match {
      case ExistsRequest(path, ctx) =>
        zooKeeper.exists(path, shouldWatch(request), new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(ExistsResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetDataRequest(path, ctx) =>
        zooKeeper.getData(path, shouldWatch(request), new DataCallback {
          override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
            callback(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetChildrenRequest(path, ctx) =>
        zooKeeper.getChildren(path, shouldWatch(request), new Children2Callback {
          override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat): Unit =
            callback(GetChildrenResponse(Code.get(rc), path, Option(ctx),
              Option(children).map(_.asScala).getOrElse(Seq.empty), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case CreateRequest(path, data, acl, createMode, ctx) =>
        zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
          override def processResult(rc: Int, path: String, ctx: Any, name: String): Unit =
            callback(CreateResponse(Code.get(rc), path, Option(ctx), name, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case SetDataRequest(path, data, version, ctx) =>
        zooKeeper.setData(path, data, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetDataResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case DeleteRequest(path, version, ctx) =>
        zooKeeper.delete(path, version, new VoidCallback {
          override def processResult(rc: Int, path: String, ctx: Any): Unit =
            callback(DeleteResponse(Code.get(rc), path, Option(ctx), responseMetadata(sendTimeMs)))
        }, ctx.orNull)
      case GetAclRequest(path, ctx) =>
        zooKeeper.getACL(path, null, new ACLCallback {
          override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
            callback(GetAclResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs)))
        }}, ctx.orNull)
      case SetAclRequest(path, acl, version, ctx) =>
        zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetAclResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)
    }
  }

  /**
   * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
   * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
   * @throws ZooKeeperClientExpiredException if the session expired either before or while waiting for connection.
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
    }
    info("Connected.")
  }

  // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
  // may need to be updated.
  private def shouldWatch(request: AsyncRequest): Boolean = request match {
    case _: GetChildrenRequest => zNodeChildChangeHandlers.contains(request.path)
    case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(request.path)
    case _ => throw new IllegalArgumentException(s"Request $request is not watchable")
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
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
    inWriteLock(initializationLock) {
      zNodeChangeHandlers.clear()
      zNodeChildChangeHandlers.clear()
      stateChangeHandlers.clear()
      zooKeeper.close()
      metricNames.foreach(removeMetric(_))
    }
    // Shutdown scheduler outside of lock to avoid deadlock if scheduler
    // is waiting for lock to process session expiry
    expiryScheduler.shutdown()
    info("Closed.")
  }

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
  }

  // Only for testing
  private[kafka] def currentZooKeeper: ZooKeeper = inReadLock(initializationLock) {
    zooKeeper
  }
  
  private def initialize(): Unit = {
    if (!connectionState.isAlive) {
      zooKeeper.close()
      info(s"Initializing a new session to $connectString.")
      // retry forever until ZooKeeper can be instantiated
      var connected = false
      while (!connected) {
        try {
          zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)
          connected = true
        } catch {
          case e: Exception =>
            info("Error when recreating ZooKeeper, retrying after a short sleep", e)
            Thread.sleep(1000)
        }
      }
    }
  }

  /**
   * reinitialize method to use in unit tests
   */
  private[zookeeper] def reinitialize(): Unit = {
    zooKeeper.close()
    initialize()
  }

  // Visibility for testing
  private[zookeeper] def scheduleSessionExpiryHandler(): Unit = {
    expiryScheduler.schedule("zk-session-expired", () => {
      inWriteLock(initializationLock) {
        info("Session expired.")
        stateChangeHandlers.values.foreach(_.beforeInitializingSession())
        initialize()
        stateChangeHandlers.values.foreach(_.afterInitializingSession())
      }
    }, delay = 0L, period = -1L, unit = TimeUnit.MILLISECONDS)
  }

  // package level visibility for testing only
  private[zookeeper] object ZooKeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug(s"Received event: $event")
      Option(event.getPath) match {
        case None =>
          val state = event.getState
          stateToMeterMap.get(state).foreach(_.mark())
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (state == KeeperState.AuthFailed) {
            error("Auth failed.")
            stateChangeHandlers.values.foreach(_.onAuthFailure())
          } else if (state == KeeperState.Expired) {
            scheduleSessionExpiryHandler()
          }
        case Some(path) =>
          (event.getType: @unchecked) match {
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }
}

trait StateChangeHandler {
  val name: String
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}

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

case class GetChildrenRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
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

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String, metadata: ResponseMetadata) extends AsyncResponse
case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any], metadata: ResponseMetadata) extends AsyncResponse
case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse
case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse
case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse
case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse

class ZooKeeperClientException(message: String) extends RuntimeException(message)
class ZooKeeperClientExpiredException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientAuthFailedException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientTimeoutException(message: String) extends ZooKeeperClientException(message)
